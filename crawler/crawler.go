// Package crawler provides a Wiki crawler for downloading, caching, and
// updating data that forms a graph from a Wikipedia XML dump.
// This data can then be used to do SPF queries against the graph.
package crawler

// TODO(johnsiilver): Write the graph out as a gob so that we can reload
// without having to parse it to, similar to the watcher.

import (
	"fmt"
	"sync"

	"github.com/golang/glog"
	"github.com/johnsiilver/golib/signal"
	"github.com/johnsiilver/wikipath/crawler/internal/graph"
	"github.com/johnsiilver/wikipath/crawler/internal/watch"
)

// SPFer provides for doing Shortest Path First queries against our corpus.
type SPFer interface {
	// SPF takes article titles and finds the shortest path between from and to.
	SPF(from string, to string) ([]string, error)
}

// Wiki is a Wiki crawler that populates a graph we can use to do SPF.
type Wiki struct {
	mu       sync.Mutex
	watcher  watch.XMLWiki
	watchSig signal.Signaler
	graph    *graph.Articles
}

// New is the constructor for Wiki.
func New() (*Wiki, error) {
	return &Wiki{}, nil
}

// Start starts the Wiki crawler. This will not return until an error or
// the first crawl is done.  The Signaler returns a *graph.Articles object.
func (w *Wiki) Start() (SPFer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.watcher != nil {
		return nil, nil
	}

	var err error
	w.watcher, err = watch.NewHTTP(watch.SimpleEnWiki)
	if err != nil {
		return nil, err
	}
	w.watchSig, err = w.watcher.Watch()
	if err != nil {
		w.stop()
		return nil, err
	}

	if err := w.processGraph(); err != nil {
		w.stop()
		return nil, err
	}
	return w.graph, nil
}

func (w *Wiki) processGraph() error {
	recv := <-w.watchSig.Receive()
	if recv.Data() == nil {
		return fmt.Errorf("watcher never sent anything")
	}
	p := recv.Data().(string)
	if err := w.makeGraph(p); err != nil {
		return err
	}

	go func() {
		for recv := range w.watchSig.Receive() {
			if recv.Data() == nil {
				return
			}
			p = recv.Data().(string)
			if err := w.makeGraph(p); err != nil {
				glog.Errorf("problems making a new graph from %s: %s", p, err)
				continue
			}
		}
	}()

	return nil
}

func (w *Wiki) makeGraph(p string) error {
	if w.graph == nil {
		var err error
		w.graph, err = graph.New(p)
		if err != nil {
			return err
		}
		return nil
	}
	if err := w.graph.Update(p); err != nil {
		return err
	}
	return nil
}

// Stop stops the Crawler.  Thread-safe.
func (w *Wiki) Stop() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.stop()
}

// stop stops the Crawler and resets everything.
// Must be protected by w.mu(mutex).
func (w *Wiki) stop() error {
	w.watchSig.Close()

	if w.watcher != nil {
		return w.watcher.Close()
	}

	return nil
}
