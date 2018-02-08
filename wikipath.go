/*
This starts a gRPC service to listen for requests looking for the SPF from
one Wikipedia article to another.

Basically a traveling salesman problem.  Find the minimum number of links
that gets you from a source article to a destination article.

HUGE NOTE:
  This has a lot of sloppy code and no tests.  This likely works, but it probably
  has some bugs.  I certainly didn't have time to do exhaustive testing of BFS
  and other parts.  Think of this as a rough proof of concept.

  Also, while the underlying code actually supports the wikipedia main corpus,
  we are cureently yanking the simpleEn corpus.  It is significantly smaller
  for downloading but still decompresses to around a GiB.  Wikipedia's is more
  like 30GiB.

Try it out notes:
  go run wikipath.go --logtostderr
  go run ./client/cmd/cmd.go --from="Space Shuttle" --to="Prussia"
  go run ./client/cmd/cmd.go --from="Space Shuttle" --to="Deepak Chopra"
  go run ./client/cmd/cmd.go --from="Umberto Eco" --to="Deepak Chopra"
  go run ./client/cmd/cmd.go --from="Steve Jobs" --to="Mahatma Gandhi"

  The server will take a bit to load up the corpus from the download site
  and do the parsing, it will tell you when its serving.  It does cache the
  download.

  Also, only runs on unix variants.  I used mmap in here.

  We de-reference redirects.  So if you do the query above, you will not see
  "United States" in the list, but not on the page.  It is because "America"
  redirects to "United States".

  Use https://simple.wikipedia.org/wiki/ to check out the pathing.

Design notes:
  1. We are not using the XML index with byte offsets for the data file.
  This would allow for the ability to spread conversion of node links across
  many machines if the size of the wiki became a problem.
  2. The caching layer could instead be micro-services sitting behind RPC
  load-balancers that are behind IP load-balancers.
  3. There only would need to be one crawler per wiki. If the crawler was a
  micro-service, it could dump the graph as a blob and allow the frontends to
  pick it up or stream it to connected frontends.
  4. There are litterally at least a hundred optimizations that could be done,
  depending on the corpus size and what kinds of search.  Ours is simple, so
  it doesn't require much except for our XML to graph conversion.
  5. We are using local techniques where in a micro-service we'd do things like
  GroupCache instead of an LRU cache.  Goroutines where we might use 100+
  machines + Goroutines (or a map-reduce).
  6. We could calculate all possible SPFs and cache the result.  This could be
  easily generated over a bunch of machine.

Interesting notes:
  1. The wiki page IDs I think are mutable between versions.  If this is so,
  it is unfortunate because it means you can't do some cool optimizations in
  your cache when the data updates.  You just have to expire the entire cache.
*/
package main

import (
	"flag"
	"fmt"
	"net"

	"google.golang.org/grpc"

	"github.com/golang/glog"
	pb "github.com/johnsiilver/wikipath/proto"
	"github.com/johnsiilver/wikipath/server"
)

var (
	port = flag.Int("port", 2730, "The port to run on")
)

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		glog.Exitf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	serv, err := server.New()
	if err != nil {
		glog.Exit(err)
	}

	pb.RegisterSPFServiceServer(grpcServer, serv)
	glog.Infof("Starting server on :%d", *port)
	grpcServer.Serve(lis)

	/*
		p, err := g.SPF("Space Shuttle", "Prussia")
		if err != nil {

		}
		fmt.Printf("shortestPath:\n\t%s\n", strings.Join(p, "\n\t"))
	*/
}
