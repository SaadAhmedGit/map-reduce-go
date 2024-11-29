package main

import (
	"log"

	"github.com/SaadAhmedGit/goMapReduceMaster/internal/mapReduce"
	pb "github.com/SaadAhmedGit/goMapReduceMaster/internal/proto"
)

func main() {
	mrc, err := mapReduce.NewMapReduceClient(2, 3, "workers.json", "output.txt")
	if err != nil {
		log.Fatalf("Failed to create map reduce client: %v", err)
	}

	document1 :=
		`
	Simulated annealing (SA) is a probabilistic technique for approximating the global optimum of a given function.
	Specifically, it is a metaheuristic to approximate global optimization in a large search space for an optimization problem.
	For large numbers of local optima, SA can find the global optima.
	It is often used when the search space is discrete (for example the traveling salesman problem, the boolean satisfiability problem, protein structure prediction, and job-shop scheduling).
	For problems where finding an approximate global optimum is more important than finding a precise local optimum in a fixed amount of time, simulated annealing may be preferable to exact algorithms such as gradient descent or branch and bound.
	`
	document2 :=
		`
	Stochastic gradient descent (often abbreviated SGD) is an iterative method for optimizing an objective function with suitable smoothness properties (e.g. differentiable or subdifferentiable).
	It can be regarded as a stochastic approximation of gradient descent optimization, since it replaces the actual gradient (calculated from the entire data set) by an estimate thereof (calculated from a randomly selected subset of the data).
	Especially in high-dimensional optimization problems this reduces the very high computational burden, achieving faster iterations in exchange for a lower convergence rate.[1] 
	`

	document3 :=
		`
		Simulated annealing is an optimization algorithm inspired by the physical process of annealing in metallurgy, where a material is heated and then slowly cooled to decrease defects, thereby minimizing the system's energy.
		The algorithm begins with a randomly chosen solution and explores the solution space by making small random changes.
		Each potential new solution is evaluated, and if it improves the objective function, it is accepted.
		If it worsens the objective function, it may still be accepted with a probability that decreases over time, controlled by a "temperature" parameter.
		This probability is higher at the start, allowing the algorithm to escape local optima by accepting worse solutions.
		As the algorithm progresses, the temperature is gradually lowered, reducing the acceptance probability of worse solutions and focusing the search on the most promising areas.
		This approach effectively balances exploration and exploitation, making simulated annealing suitable for solving complex optimization problems in fields such as operations research, engineering, and artificial intelligence.
	`

	document4 :=
		`
	Capybaras are the largest rodents in the world, native to South America.
	They are highly social animals, often found in groups ranging from a few individuals to large herds of up to a hundred.
	Capybaras have a robust, barrel-shaped body, short head, and a reddish-brown to grayish fur coat.
	They are semiaquatic mammals, which means they are well-adapted to both land and water environments.
	Capybaras have partially webbed feet, making them excellent swimmers, and they often take to water to escape predators or to keep cool.
	Their diet primarily consists of grasses and aquatic plants, but they are also known to eat fruit and bark.
	Capybaras play a crucial role in their ecosystems by influencing vegetation and serving as prey for various predators, including jaguars, anacondas, and caimans.
	Despite their size, capybaras are gentle and have a placid temperament, which has led to them being domesticated in some areas and kept as exotic pets.
	Their social nature and ability to live harmoniously with other species, including birds and small animals, further highlight their unique ecological niche.
	`
	kvPairs := []pb.Kv{
		{
			Key:   "1",
			Value: document1,
		},
		{
			Key:   "2",
			Value: document2,
		},
		{
			Key:   "3",
			Value: document3,
		},
		{
			Key:   "4",
			Value: document4,
		},
	}

	err = mrc.StartMapReduce(kvPairs)
	if err != nil {
		log.Fatalf("Failed to start map reduce: %v", err)
	}

	mrc.WaitForPingRoutine()

}
