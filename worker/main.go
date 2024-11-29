package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/SaadAhmedGit/goMapReduceWorker/internal/proto"
	"github.com/SaadAhmedGit/goMapReduceWorker/internal/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	Host string
	Port string
}

func (cfg *config) load() {
	flag.StringVar(&cfg.Host, "host", "localhost", "host")
	flag.StringVar(&cfg.Port, "port", "8080", "port")
	flag.Parse()
}

func main() {
	var cfg config
	cfg.load()

	grpcServer := grpc.NewServer()

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", cfg.Host, cfg.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	pb.RegisterWorkerServer(grpcServer, &worker{})

	log.Printf("starting server on %s:%s...\n", cfg.Host, cfg.Port)

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

type worker struct {
	pb.UnimplementedWorkerServer

	intermediateFileLock sync.Mutex
	interMediateFile     *os.File
	mapChan              chan *pb.Kv

	reducerConfs []*pb.WorkerInfo // Used by mappers to send intermediate files to reducers
}

func (w *worker) HeartBeat(ctx context.Context, _ *pb.EmptyMessage) (*pb.EmptyMessage, error) {
	return &pb.EmptyMessage{}, nil
}

func (w *worker) ReceiveKvStream(stream pb.Worker_ReceiveKvStreamServer) error {
	if w.interMediateFile == nil {
		var err error
		w.interMediateFile, err = os.CreateTemp("", "intermediate-map")
		if err != nil {
			err = fmt.Errorf("failed to create intermediate file: %v", err)
			fmt.Println(err)
			return err
		}
	}
	if w.mapChan == nil {
		w.mapChan = make(chan *pb.Kv, 100)
	}

	for {
		kv, err := stream.Recv()
		if err != nil && err.Error() == "EOF" {
			log.Println("Received all key-value pairs from master")
			break
		}
		w.mapChan <- kv
	}

	close(w.mapChan) // HOW COULD I FORGET? AAAAAAAAAAAAAA

	return stream.SendAndClose(&pb.Ack{
		Msg:     "Received all key-value pairs from master",
		Success: true,
	})
}

func (w *worker) emitIntermediate(kv *pb.Kv) error {
	w.intermediateFileLock.Lock()
	defer w.intermediateFileLock.Unlock()

	_, err := w.interMediateFile.WriteString(fmt.Sprintf("%s:%s\n", kv.Key, kv.Value))
	if err != nil {
		return fmt.Errorf("failed to write to intermediate file: %v", err)
	}

	return nil
}

func (w *worker) emitOutput(kv *pb.Kv) error {
	w.intermediateFileLock.Lock()
	defer w.intermediateFileLock.Unlock()

	_, err := w.interMediateFile.WriteString(fmt.Sprintf("%s:%s\n", kv.Key, kv.Value))
	if err != nil {
		return fmt.Errorf("failed to write to intermediate file: %v", err)
	}
	return nil
}

func (w *worker) Map(kv *pb.Kv) {
	// For each word in value, emit (word, "1").
	for _, word := range strings.Fields(kv.Value) {
		if err := w.emitIntermediate(&pb.Kv{
			Key:   word,
			Value: "1",
		}); err != nil {
			log.Printf("failed to emit intermediate: %v", err)
		}

	}

}

func (w *worker) StartMap(ctx context.Context, _ *pb.EmptyMessage) (*pb.Ack, error) {
	defer w.resetWorkerState()

	if w.mapChan == nil {
		return &pb.Ack{
			Msg:     "no key-value pairs received",
			Success: false,
		}, nil
	}

	// Execute map on all key-value pairs
	var mapWg sync.WaitGroup
	for kv := range w.mapChan {
		mapWg.Add(1)
		go func(kv *pb.Kv) {
			defer mapWg.Done()
			w.Map(kv)
		}(kv)
	}

	// Wait for all map tasks to finish
	mapWg.Wait()

	w.intermediateFileLock.Lock()
	if err := w.interMediateFile.Sync(); err != nil {
		return &pb.Ack{
			Msg:     "failed to commit writes to intermediate file",
			Success: false,
		}, nil
	}
	w.intermediateFileLock.Unlock()

	// Open reducer streams
	reducerStreams := utils.NewSafeSlice[pb.Worker_ReducerReceiveKvStreamClient]()
	for _, reducerConf := range w.reducerConfs {
		conn, err := grpc.Dial(fmt.Sprintf("%s:%s", reducerConf.Address, reducerConf.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			err = fmt.Errorf("failed to dial reducer: %v", err)
			return &pb.Ack{
				Msg:     err.Error(),
				Success: false,
			}, err
		}

		reducerClient := pb.NewWorkerClient(conn)
		stream, err := reducerClient.ReducerReceiveKvStream(context.Background())
		if err != nil {
			err = fmt.Errorf("failed to open reducer stream: %v", err)
			return &pb.Ack{
				Msg:     err.Error(),
				Success: false,
			}, err
		}

		reducerStreams.Append(stream)

	}

	// Send intermediate KVs to reducers
	w.intermediateFileLock.Lock()
	w.interMediateFile.Seek(0, 0)
	for _, kv := range readFile(w.interMediateFile) {

		reducerIndex := int(hashString(kv.Key)) % len(w.reducerConfs)
		if kv.Key == "this" {
			fmt.Printf("Sending %s to reducer %s\n", kv.Key, w.reducerConfs[reducerIndex].Port)
		}
		stream := *reducerStreams.Get(int(reducerIndex))

		if err := stream.Send(kv); err != nil {
			log.Printf("failed to send: %v", err)

		}
	}
	w.interMediateFile.Seek(0, 0)
	w.intermediateFileLock.Unlock()

	// Close all reducer streams
	time.Sleep(300 * time.Millisecond) //Added to ensure all data is received before closing the stream. TODO: Find a better way
	var closeStreamWg sync.WaitGroup
	for i := 0; i < reducerStreams.Len(); i++ {
		stream := *reducerStreams.Get(i)
		closeStreamWg.Add(1)
		go func() {
			defer closeStreamWg.Done()
			if err := stream.CloseSend(); err != nil {
				log.Printf("failed to close send: %v", err)
			}
		}()
	}

	closeStreamWg.Wait()

	return &pb.Ack{
		Msg:     "map tasks completed and intermediate KV pairs sent to reducers",
		Success: true,
	}, nil
}

// Naive implementation of sorting intermediate file. TODO: Use external sort
func sortFile(file *os.File) {
	file.Seek(0, 0)

	// Reset file pointer after sorting
	defer file.Seek(0, 0)

	// Read all lines from intermediate file
	lines := make([]string, 0)
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		lines = append(lines, line)
	}

	// Sort lines
	sorter := func(i, j int) bool {
		return lines[i] < lines[j]
	}

	sort.Slice(lines, sorter)

	fmt.Printf("here: %v", lines)
	// Write sorted lines back to the file
	file.Seek(0, 0)
	writer := bufio.NewWriter(file)
	for _, line := range lines {
		writer.WriteString(line)
	}
	writer.Flush()
}

func (w *worker) Reduce(redReq *pb.ReduceRequest) {
	// For each key K, emit (K, sum(V)).
	sum := 0
	for _, v := range redReq.Values {
		parsedInt, err := strconv.Atoi(v)
		if err != nil {
			log.Printf("failed to parse int: %v", err)
			continue
		}

		sum += parsedInt
	}
	fmt.Printf("Emitting Output: %s:%d\n", redReq.Key, sum)

	if err := w.emitOutput(&pb.Kv{
		Key:   redReq.Key,
		Value: strconv.Itoa(sum),
	}); err != nil {
		log.Printf("failed to emit output: %v", err)
	}
}

func (w *worker) ReducerInfo(ctx context.Context, in *pb.ReducerInfoRequest) (*pb.Ack, error) {
	w.reducerConfs = append(w.reducerConfs, in.Workers...)

	return &pb.Ack{
		Msg:     "reducer info received",
		Success: true,
	}, nil
}

func (w *worker) StartReduce(_ *pb.EmptyMessage, outputStream pb.Worker_StartReduceServer) error {
	defer w.resetWorkerState()

	// Execute reduce on all key-value pairs
	var reduceWg sync.WaitGroup
	var prevKey *string
	values := make([]string, 0)

	// Make a copy of the intermediate file and delete contents in the original one to make space for output
	w.intermediateFileLock.Lock()
	tmpFile, err := duplicateFile(w.interMediateFile)
	w.intermediateFileLock.Unlock()
	if err != nil {
		return fmt.Errorf("failed to copy intermediate file: %v", err)
	}

	defer os.Remove(tmpFile.Name())

	w.intermediateFileLock.Lock()
	w.interMediateFile.Truncate(0)
	w.interMediateFile.Seek(0, 0)
	w.intermediateFileLock.Unlock()

	sortFile(tmpFile)

	tmpFile.Seek(0, 0)
	// Start processing the intermediate KV pairs
	for _, kv := range readFile(tmpFile) {

		// Combine values for the same key assuming they are sorted and reduce them
		if prevKey == nil {
			prevKey = &kv.Key
			values = append(values, kv.Value)
			continue
		}

		if *prevKey != kv.Key {
			reduceWg.Add(1)
			func() {
				defer reduceWg.Done()

				// fmt.Printf("Prev: %s, Curr: %s\n", *prevKey, kv.Key)
				// fmt.Println("Reducing: ", *prevKey, values)
				w.Reduce(&pb.ReduceRequest{
					Key:    *prevKey,
					Values: values,
				})
			}()

			values = make([]string, 0)

			prevKey = &kv.Key
		}

		values = append(values, kv.Value)

	}
	tmpFile.Seek(0, 0)

	// Process the last key outside the loop
	if prevKey != nil {
		reduceWg.Add(1)
		func() {
			defer reduceWg.Done()

			// fmt.Println("Reducing: ", *prevKey, values)
			w.Reduce(&pb.ReduceRequest{
				Key:    *prevKey,
				Values: values,
			})
		}()

	}

	// Wait for all reduce tasks to finish
	reduceWg.Wait()

	// Read the intermediate file in 64KB chunks and send to master
	w.intermediateFileLock.Lock()
	defer w.intermediateFileLock.Unlock()

	w.interMediateFile.Seek(0, 0)

	reader := bufio.NewReader(w.interMediateFile)
	for {
		chunk := make([]byte, 64*1024)
		n, err := reader.Read(chunk)

		chunk = chunk[:n]

		if err != nil && err.Error() == "EOF" {
			break
		}

		if err := outputStream.Send(&pb.Chunk{
			Data: chunk,
		}); err != nil {
			return fmt.Errorf("failed to send chunk: %v", err)
		}
	}

	return nil
}

func (w *worker) ReducerReceiveKvStream(stream pb.Worker_ReducerReceiveKvStreamServer) error {
	if w.interMediateFile == nil {
		fmt.Println("REDUCER: creating intermediate file")
		w.interMediateFile, _ = os.CreateTemp("", "intermediate-reduce")
	}
	for {
		kv, err := stream.Recv()
		if err != nil && err.Error() == "EOF" {
			break
		}

		w.intermediateFileLock.Lock()
		w.interMediateFile.WriteString(fmt.Sprintf("%s:%s\n", kv.Key, kv.Value))
		w.intermediateFileLock.Unlock()
	}

	w.intermediateFileLock.Lock()
	if err := w.interMediateFile.Sync(); err != nil {
		return fmt.Errorf("failed to commit writes to intermediate file: %v", err)
	}
	w.intermediateFileLock.Unlock()

	return stream.SendAndClose(&pb.Ack{
		Msg:     "Received all key-value pairs from mappers",
		Success: true,
	})
}

func readFile(file *os.File) []*pb.Kv {
	lines := make([]*pb.Kv, 0)
	reader := bufio.NewReader(file)
	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		kv := strings.Split(string(line), ":")

		lines = append(lines, &pb.Kv{
			Key:   kv[0],
			Value: kv[1],
		})
	}

	return lines
}

func duplicateFile(file *os.File) (*os.File, error) {
	file.Seek(0, 0)

	tempFile, err := os.CreateTemp("", "intermediate-reduce-copy")
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary file: %v", err)
	}

	file.Seek(0, 0)
	_, err = file.WriteTo(tempFile)
	if err != nil {
		return nil, fmt.Errorf("failed to copy intermediate file: %v", err)
	}

	return tempFile, nil
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (w *worker) resetWorkerState() {
	w.interMediateFile = nil
	w.reducerConfs = nil
	w.mapChan = nil
}
