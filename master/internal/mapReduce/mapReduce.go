package mapReduce

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/SaadAhmedGit/goMapReduceMaster/internal/proto"
	"github.com/SaadAhmedGit/goMapReduceMaster/internal/utils"
)

type MapReduceClient struct {
	M        int
	mappers  []worker
	R        int
	reducers []worker

	workers []worker

	outputFileMutex sync.Mutex
	outputFile      *os.File

	pingRoutineWg sync.WaitGroup
}

func (mrc *MapReduceClient) WaitForPingRoutine() {
	mrc.pingRoutineWg.Wait()
}

func NewMapReduceClient(_M, _R int, workerConfFilePath, outputFilePath string) (*MapReduceClient, error) {
	if _, err := os.Stat(outputFilePath); err == nil {
		os.Remove(outputFilePath)
	}

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return nil, err
	}

	mrc := MapReduceClient{
		M:          _M,
		R:          _R,
		workers:    []worker{},
		outputFile: outputFile,

		outputFileMutex: sync.Mutex{},
		pingRoutineWg:   sync.WaitGroup{},
	}

	mrc.startWorkersFromConf(workerConfFilePath)

	return &mrc, nil

}

func (mrc *MapReduceClient) StartMapReduce(kvPairs []pb.Kv) error {

	var mapStreamsWg sync.WaitGroup

	mapStreams := utils.NewSafeSlice[pb.Worker_ReceiveKvStreamClient]()
	for i := 0; i < mrc.M; i++ {
		log.Printf("mapper %d started\n", i)
		mapStreamsWg.Add(1)
		go func(i int) {
			defer mapStreamsWg.Done()

			ctx := context.Background()
			// defer cancel()

			stream, err := mrc.mappers[i].client.ReceiveKvStream(ctx)
			if err != nil {
				log.Printf("failed to open stream: %v", err)
				return
			}
			mapStreams.Append(stream)
		}(i)
	}

	// Wait till all streams are opened
	mapStreamsWg.Wait()

	for i := 0; i < len(kvPairs); i++ {
		mapperIndex := int(hashString(kvPairs[i].Key)) % mrc.M
		mappedStream := mapStreams.Get(mapperIndex)

		if err := (*mappedStream).Send(&kvPairs[i]); err != nil {
			log.Printf("failed to send: %v", err)
		}

	}

	time.Sleep(300 * time.Millisecond) //Added to ensure all data is sent before closing the stream

	// Send EOF to all streams
	for i := 0; i < mapStreams.Len(); i++ {
		mapStreamsWg.Add(1)
		stream := *mapStreams.Get(i)
		go func() {
			defer mapStreamsWg.Done()
			if err := stream.CloseSend(); err != nil {
				log.Printf("failed to close send: %v", err)
			}

		}()
	}

	// Wait for all map workers to finish receiving data
	mapStreamsWg.Wait()

	// Create pb.WorkerInfo list for mrc.reducers ([]worker)
	var reducerList []*pb.WorkerInfo
	for _, r := range mrc.reducers {
		reducerList = append(reducerList, &pb.WorkerInfo{
			Address: r.workerConf.Host,
			Port:    r.workerConf.Port,
		})
	}

	// Send reducer list to all mappers (needed for the mappers to know which reducer to send data to)
	var reducerListWg sync.WaitGroup
	reducerListWg.Add(mrc.M)
	for _, m := range mrc.mappers {
		go func(m worker) {
			defer reducerListWg.Done()

			ack, err := m.client.ReducerInfo(context.Background(), &pb.ReducerInfoRequest{
				Workers: reducerList,
			})
			if err != nil {
				log.Printf("failed to send reducer list: %v", err)
			}
			if !ack.Success {
				log.Printf("failed to send reducer list: %v", ack.Msg)
			}
		}(m)

	}

	// Wait for all mappers to finish receiving reducer list
	reducerListWg.Wait()

	// Start all map tasks
	var mapTaskWg sync.WaitGroup
	mapTaskWg.Add(mrc.M)
	for i := 0; i < mrc.M; i++ {
		go func(i int) {
			defer mapTaskWg.Done()
			ack, err := mrc.mappers[i].client.StartMap(context.Background(), &pb.EmptyMessage{})
			if err != nil {
				log.Printf("failed to start map: %v", err)
			}
			if ack.Success {
				log.Printf("Map task done: %v", ack.Msg)
			}
		}(i)
	}

	// Wait for all map tasks to finish
	mapTaskWg.Wait()

	// Start all reduce tasks
	var reduceTaskWg sync.WaitGroup
	reduceTaskWg.Add(mrc.R)
	for _, r := range mrc.reducers {
		go func(r worker) {
			defer reduceTaskWg.Done()

			resultStream, err := r.client.StartReduce(context.Background(), &pb.EmptyMessage{})
			if err != nil {
				log.Printf("failed to start reduce: %v", err)
			}

			// Write to output file in a thread-safe manner as the data is being received
			for {
				chunk, err := resultStream.Recv()
				if err != nil && err.Error() == "EOF" {
					break
				}

				// TODO: Try running this in a separate goroutine
				mrc.outputFileMutex.Lock()
				_, err = mrc.outputFile.Write(chunk.Data)
				mrc.outputFileMutex.Unlock()
				if err != nil {
					log.Printf("failed to write to output file: %v\n", err)
				}
			}
		}(r)
	}

	reduceTaskWg.Wait()
	fmt.Println("All reduce tasks done!")
	if err := mrc.outputFile.Close(); err != nil {
		log.Printf("failed to close output file: %v", err)
	}
	fmt.Println("Data written to file, map reduce done!")

	return nil
}

var (
	_WORKER_PING_FREQUENCY = 3
	_WORKER_TIMEOUT        = 5
	_MAX_WORKER_RETRY      = 3
)

type worker struct {
	conn       *grpc.ClientConn
	client     pb.WorkerClient
	retryCount int

	workerConf *workerConf
}

type workerConf struct {
	Host string
	Port string
}

func (mrc *MapReduceClient) validateWorkersWithMapReduce(workerConfs []workerConf) (*[]workerConf, error) {
	if mrc.M+mrc.R > len(workerConfs) {
		return &workerConfs, fmt.Errorf("M(%d)+R(%d) should be less than or equal to the number of workers (%d)", mrc.M, mrc.R, len(workerConfs))
	} else if mrc.M+mrc.R <= 0 {
		return &workerConfs, fmt.Errorf("M(%d)+R(%d) should be greater than 0", mrc.M, mrc.R)
	} else if mrc.M+mrc.R < len(workerConfs) {
		fmt.Printf("Warning: M(%d)+R(%d) is less than the number of workers (%d). Some workers will be unused.\n", mrc.M, mrc.R, len(workerConfs))

		// Remove unused workers
		workerConfs = workerConfs[:mrc.M+mrc.R]
		return &workerConfs, nil
	}

	return &workerConfs, nil

}

func (mrc *MapReduceClient) startWorkersFromConf(jsonConfFile string) error {
	data, err := os.ReadFile(jsonConfFile)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}

	workerConfs := []workerConf{}
	err = json.Unmarshal(data, &workerConfs)
	if err != nil {
		return fmt.Errorf("failed to unmarshal json: %v", err)
	}

	workerConfsPtr, err := mrc.validateWorkersWithMapReduce(workerConfs)
	if err != nil {
		return fmt.Errorf("failed to validate workers: %v", err)
	}
	workerConfs = *workerConfsPtr

	mrc.workers, err = dialWorkers(workerConfs)
	if err != nil {
		return fmt.Errorf("failed to dial workers: %v", err)
	}

	mrc.mappers = mrc.workers[:mrc.M]
	mrc.reducers = mrc.workers[mrc.M:]

	mrc.pingRoutineWg.Add(1)
	go func() {
		defer mrc.pingRoutineWg.Done()
		defer releaseWorkers(mrc.workers)
		startHealthCheckRoutine(mrc.workers)
	}()

	return nil
}

func dialWorkers(workerConfs []workerConf) ([]worker, error) {
	workers := []worker{}

	for _, wConf := range workerConfs {
		conn, err := grpc.Dial(fmt.Sprintf("%s:%s", wConf.Host, wConf.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to dial: %v", err)
		}

		workers = append(workers, worker{
			conn:       conn,
			client:     pb.NewWorkerClient(conn),
			retryCount: 0,
			workerConf: &wConf,
		})
	}

	return workers, nil
}

func releaseWorkers(workers []worker) {
	for _, w := range workers {
		w.conn.Close()
	}
}

func startHealthCheckRoutine(workers []worker) {
	var workerWg sync.WaitGroup
	workerWg.Add(len(workers))

	for _, w := range workers {
		go func(w worker) {
			defer workerWg.Done()

			for {
				ticker := time.NewTicker(time.Duration(_WORKER_PING_FREQUENCY) * time.Second)
				defer ticker.Stop()

				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(_WORKER_TIMEOUT)*time.Second)
				defer cancel()

				_, err := w.client.HeartBeat(ctx, &pb.EmptyMessage{})
				if err != nil {
					if w.retryCount >= _MAX_WORKER_RETRY {
						log.Printf("Failed to ping worker %s:%s (max retry count reached): %v", w.workerConf.Host, w.workerConf.Port, err)
						break
					}
					log.Printf("Failed to ping worker %s:%s (retry count: %d): %v", w.workerConf.Host, w.workerConf.Port, w.retryCount, err)
					w.retryCount++
				} else {
					if w.retryCount > 0 {
						log.Printf("Worker %s:%s is back online", w.workerConf.Host, w.workerConf.Port)
					}
					w.retryCount = 0
				}
				<-ticker.C
			}
		}(w)

		// Some delay to avoid all workers pinging at the same time
		UNIFORM_PING_ROUTINE_TIME_OFFSET := _WORKER_PING_FREQUENCY / len(workers)
		time.Sleep(time.Duration(UNIFORM_PING_ROUTINE_TIME_OFFSET) * time.Second)
	}

	workerWg.Wait()
}

func hashString(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
