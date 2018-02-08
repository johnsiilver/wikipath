package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/golang/glog"
	"github.com/johnsiilver/wikipath/client"
)

var (
	addr = flag.String("addr", "127.0.0.1", "The address of the server, defaults to local host")
	port = flag.Int("port", 2730, "The port the server is on, defaults to 2730")
	from = flag.String("from", "", "The from article title, case sensitive")
	to   = flag.String("to", "", "The to article title, case sensitive")
)

func main() {
	flag.Parse()

	if *from == "" {
		glog.Exitf("cannot have an empty --from")
	}
	if *to == "" {
		glog.Exitf("cannot have an empty --to")
	}

	c, err := client.New(*addr, *port)
	if err != nil {
		glog.Exit(err)
	}

	p, err := c.SPF(context.Background(), *from, *to)
	if err != nil {
		glog.Exit(err)
	}
	fmt.Printf("Shortest path from %q to %q:\n\t%s\n", *from, *to, strings.Join(p, "\n\t"))
}
