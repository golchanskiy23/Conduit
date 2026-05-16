package scheduler

import (
	"fmt"
	"maps"
	"sync"
)

type DAG struct {
	mu        sync.Mutex
	nodes     map[string]struct{}
	dependents  map[string][]string
	waitingFor     map[string]int
}

func NewDAG() *DAG {
	return &DAG{
		nodes:    make(map[string]struct{}),
		dependents: make(map[string][]string),
		waitingFor:    make(map[string]int),
	}
}

func hasCycle(deg map[string][]string, cnt map[string]int) bool{
	cntClone := maps.Clone(cnt)

	var queue []string
	for id, degree := range cntClone{
		if degree == 0{
			queue = append(queue, id)
		} 
	}

	processed := 0
	for len(queue) > 0{
		size := len(queue)
		for i := 0; i < size; i++{
			id := queue[0]
			queue = queue[1:]
			processed++
			for _, dep := range deg[id]{
				cntClone[dep]--
				if cntClone[dep] == 0{
					queue = append(queue, dep)
				}
			}
		}
	}

	return processed != len(cnt)
}

func (graph *DAG) Add(id string, depends []string) error{
	graph.mu.Lock()
	defer graph.mu.Unlock()

	if _, ok := graph.nodes[id]; ok{
		return fmt.Errorf("%w: %s", ErrAlreadyExists, id)
	}

	nodesCopy := maps.Clone(graph.nodes)
	indegreeCopy := maps.Clone(graph.dependents)
	inCntCopy := maps.Clone((graph.waitingFor))

	nodesCopy[id] = struct{}{}
	inCntCopy[id] = len(depends)
	for _, dep := range depends{
		if _, ok := nodesCopy[dep]; !ok {
			return fmt.Errorf("unknown dependency: %s", dep)
		}
		indegreeCopy[dep] = append(indegreeCopy[dep], id)
	}

	if hasCycle(indegreeCopy, inCntCopy){
		return fmt.Errorf("%w: %s", ErrCyclicDependency, id)
	}

	graph.nodes = nodesCopy
	graph.dependents = indegreeCopy
	graph.waitingFor = inCntCopy

	return nil
}

func (g *DAG) OnComplete(id string) []string {
	g.mu.Lock()
	defer g.mu.Unlock()

	var unlocked []string
	for _, dependent := range g.dependents[id] {
		g.waitingFor[dependent]--
		if g.waitingFor[dependent] == 0 {
			unlocked = append(unlocked, dependent)
		}
	}
	delete(g.waitingFor, id)
	delete(g.dependents, id)
	delete(g.nodes, id)
	return unlocked
}