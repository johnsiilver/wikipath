// Package graph provides a graph for doing SPF calculations on wiki articles
// between two articles. It uses BFS as the search algorithm.
package graph

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/google/btree"
	"github.com/johnsiilver/wikipath/crawler/internal/graph/nodify"
	"github.com/mabu/algo/graph/bfs"
)

type internalGraph []*nodify.Node

func (i internalGraph) Adjacent(v int) []int {
	if v-1 >= len(i) {
		return nil
	}
	// Convert from node.ID to int.
	// TODO(johnsiilver): Maybe convert out of uint64 that wiki uses to int early
	// on so we don't have to do the whole copy thing here.
	sl := make([]int, 0, len(i))
	for _, l := range i[v-1].Links {
		sl = append(sl, int(l))
	}
	return sl
}

type graphValue struct {
	toID  nodify.TitleToID
	graph internalGraph
}

// Articles converts a Wikipedia XML file into a directed graph for the purposes
// of finding the shortest path between two articles if it exists.
type Articles struct {
	// This is the current graph.
	current atomic.Value // graphValue

	// These are used when processing a new graph.
	edgesCh    chan *nodify.Node
	edgesToID  nodify.TitleToID
	processing sync.WaitGroup

	mu sync.Mutex // Protects .Update()
}

// New is the constructor for Articles.
func New(p string) (*Articles, error) {
	g := &Articles{edgesCh: make(chan *nodify.Node, 100)}

	for i := 0; i < runtime.NumCPU(); i++ {
		go g.edgesProcesser()
	}

	if err := g.Update(p); err != nil {
		return nil, err
	}
	return g, nil
}

// SPF finds the shortest path from an article to an article via the article links.
// It is safe to call .Update() while calling SPF.  It is thread-safe.
func (g *Articles) SPF(from string, to string) ([]string, error) {
	value := g.current.Load().(graphValue)
	fromID, ok := value.toID[from]
	if !ok {
		return nil, fmt.Errorf("cannot locate from article %q", from)
	}
	toID, ok := value.toID[to]
	if !ok {
		return nil, fmt.Errorf("cannot locate to article %q", to)
	}

	p := bfs.Path(value.graph, int(fromID), int(toID))
	if p == nil {
		return nil, fmt.Errorf("no path exists from article %q to %q", from, to)
	}
	s := make([]string, 0, len(p))
	for _, id := range p {
		s = append(s, value.graph[int(id)-1].Title)
	}
	return s, nil
}

// Update says to update the Graph model from the XML data at file path "p.".
// This is safe to call while also calling SPF.  It is thread-safe.
func (g *Articles) Update(p string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	glog.Infof("Starting file parsing")
	w := nodify.New()
	tree, toID, err := w.Parse(p)
	if err != nil {
		return fmt.Errorf("error during parsing: %s", err)
	}

	glog.Infof("Starting edge parsing")
	g.edgesToID = toID
	tree.Descend(g.createEdges)

	glog.Infof("Waiting for edge parsing")
	g.processing.Wait()

	// This contains all of our nodes that will be stored at index Node.ID -1 (because this starts with ID 1).
	// Their may be a bunch of empty entries because of incomplete data sets.
	// This is a read optimization costing space instead of processing by using the btree for lookups O(1) vs O(log n).
	graph := make([]*nodify.Node, tree.Max().(*nodify.Node).ID)
	tree.Descend(
		func(i btree.Item) bool {
			n := i.(*nodify.Node)
			graph[int(n.ID)-1] = n
			return true
		},
	)
	value := graphValue{
		toID:  toID,
		graph: graph,
	}
	g.current.Store(value)
	return nil
}

func (g *Articles) createEdges(i btree.Item) bool {
	g.processing.Add(1)
	g.edgesCh <- i.(*nodify.Node)
	return true
}

func (g *Articles) edgesProcesser() {
	for node := range g.edgesCh {
		node.Links = make([]nodify.ID, 0, len(node.TitleLinks))
		for _, link := range node.TitleLinks {
			id, ok := g.edgesToID[link]
			if !ok {
				// Note: There are a lot of links like images and files we don't care about.
				// Also, some of the wiki files like simplewiki we are using for smaller
				// file sizes does not contain all the articles that there are links for.
				// Example: "William Clothier" is referenced in links but there is no article for it.
				glog.V(1).Infof("cannot convert link %q to ID", link)
				continue
			}
			node.Links = append(node.Links, id)
		}
		// We don't need these any more, so lets get rid of them.
		node.TitleLinks = nil

		g.processing.Done()
	}
}
