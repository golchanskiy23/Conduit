package graph

import (
	"fmt"
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

func (graph *DAG) Add(id string, depends []string) error {
    graph.mu.Lock()
    defer graph.mu.Unlock()

    if _, ok := graph.nodes[id]; ok {
        return fmt.Errorf("%w: %s", ErrAlreadyExists, id)
    }

    for _, dep := range depends {
        if _, ok := graph.nodes[dep]; !ok {
            return fmt.Errorf("unknown dependency: %s", dep)
        }
    }

    graph.nodes[id] = struct{}{}
    graph.waitingFor[id] = len(depends)
    for _, dep := range depends {
        graph.dependents[dep] = append(graph.dependents[dep], id)
    }

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