package server

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/johnsiilver/golib/cache/lru"
	"github.com/johnsiilver/wikipath/crawler"
	pb "github.com/johnsiilver/wikipath/proto"
	"golang.org/x/net/context"
)

// WikiSPF provides a gRPC service for doing article to article SPF calculations
// on various wikipedia files.
type WikiSPF struct {
	spf crawler.SPFer
	lru lru.Cache
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

	return &WikiSPF{spf: query, lru: cache}, nil
}

// Search implements pb.Search().
func (w *WikiSPF) Search(ctx context.Context, req *pb.SPFRequest) (*pb.SPFResponse, error) {
	if cacheHit, ok := w.lru.Get(w.cacheKey(req.From, req.To)); ok {
		glog.Infof("cache hit for %s:%s", req.From, req.To)
		return &pb.SPFResponse{Path: cacheHit.([]string)}, nil
	}

	start := time.Now()
	path, err := w.spf.SPF(req.From, req.To)
	if err != nil {
		return nil, err
	}
	glog.Infof("SPF crawl took %v", time.Now().Sub(start))
	if err := w.lru.Set(w.cacheKey(req.From, req.To), path); err != nil {
		glog.Errorf("problem populating LRU cache: %s", err)
	}

	return &pb.SPFResponse{Path: path}, nil
}

func (*WikiSPF) cacheKey(from, to string) string {
	return fmt.Sprintf("%s:%s", from, to)
}
