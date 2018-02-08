package server

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/johnsiilver/golib/cache/lru"
	"github.com/johnsiilver/wikipath/crawler"
	"github.com/johnsiilver/wikipath/crawler/graph"
	pb "github.com/johnsiilver/wikipath/proto"
	"golang.org/x/net/context"
)

// WikiSPF provides a gRPC service for doing article to article SPF calculations
// on various wikipedia files.
type WikiSPF struct {
	spf crawler.SPFer
	lru atomic.Value // lru.Cache
}

// New is the constructor for WikiSPF.
func New() (pb.SPFServiceServer, error) {
	wiki, err := crawler.New()
	if err != nil {
		return nil, err
	}

	cache, err := lru.New(lru.NumberLimit(1000))
	if err != nil {
		return nil, err
	}

	query, err := wiki.Start()
	if err != nil {
		return nil, err
	}

	w := &WikiSPF{spf: query}
	w.lru.Store(cache)

	// Drain any notification that have already come across.
	select {
	case <-w.spf.(*graph.Articles).UpdateNotify():
	default:
	}

	go w.cacheNullify()

	return w, nil
}

// Search implements pb.Search().
func (w *WikiSPF) Search(ctx context.Context, req *pb.SPFRequest) (*pb.SPFResponse, error) {
	if cacheHit, ok := w.lru.Load().(lru.Cache).Get(w.cacheKey(req.From, req.To)); ok {
		glog.Infof("cache hit for %s:%s", req.From, req.To)
		return &pb.SPFResponse{Path: cacheHit.([]string)}, nil
	}

	start := time.Now()
	path, err := w.spf.SPF(req.From, req.To)
	if err != nil {
		return nil, err
	}
	glog.Infof("SPF crawl took %v", time.Now().Sub(start))
	if err := w.lru.Load().(lru.Cache).Set(w.cacheKey(req.From, req.To), path); err != nil {
		glog.Errorf("problem populating LRU cache: %s", err)
	}

	return &pb.SPFResponse{Path: path}, nil
}

func (w *WikiSPF) cacheNullify() {
	g := w.spf.(*graph.Articles)
	for _ = range g.UpdateNotify() {
		cache, err := lru.New(lru.NumberLimit(1000))
		if err != nil {
			glog.Errorf("can't make new cache: %s", err)
			continue
		}
		w.lru.Store(cache)
	}
}

func (*WikiSPF) cacheKey(from, to string) string {
	return fmt.Sprintf("%s:%s", from, to)
}
