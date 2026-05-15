package sheduler

import (
	"fmt"
	"sync"
	"maps"
)

type DAG struct{
	mu sync.Mutex
	nodes map[string]struct{}
	Indegree map[string][]string
	InCnt map[string]int
}

func NewDAG() *DAG{
    return &DAG{
        nodes: make(map[string]struct{}),
        Indegree: make(map[string][]string),
	    InCnt: make(map[string]int),
    }
}

func (d *DAG) hasCycle() bool {
    degree := maps.Clone(d.InCnt)
    queue := []string{}
    for id, deg := range degree {
        if deg == 0 {
            queue = append(queue, id)
        }
    }
    processed := 0
    for len(queue) > 0 {
        id := queue[0]
        queue = queue[1:]
        processed++
        for _, dep := range d.Indegree[id] {
            degree[dep]--
            if degree[dep] == 0 {
                queue = append(queue, dep)
            }
        }
    }
    return processed != len(d.InCnt)
}

func (g *DAG) Add(id string, depends []string) error{
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.nodes[id]; ok{
		// return custom errror
		return fmt.Errorf("custom already exists error")
	}

	g.nodes[id] = struct{}{}
	g.InCnt[id] = len(depends)
	for _, dependency := range depends{
		g.Indegree[dependency] = append(g.Indegree[dependency], id)
	}

	if g.hasCycle(){
		delete(g.InCnt, id)
		delete(g.nodes, id)
		for _, dependency := range depends{
			size := len(g.Indegree[dependency])
			g.Indegree[dependency] = g.Indegree[dependency][:size-1]
		}
		return ErrCyclicDependency
	}

	return nil
}


func (d *DAG) OnComplete(id string) []string {
    d.mu.Lock()
    defer d.mu.Unlock()

    var unlocked []string
    for _, dependent := range d.Indegree[id] {
        d.InCnt[dependent]--
        if d.InCnt[dependent] == 0 {
            unlocked = append(unlocked, dependent)
        }
    }
    delete(d.InCnt, id)
    delete(d.Indegree, id)
    return unlocked
}