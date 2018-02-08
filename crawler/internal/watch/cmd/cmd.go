// Just a cmdline tool that calls our watcher for testing it works since I'm
// not writing any tests.
package main

import (
	"flag"
	"fmt"

	"github.com/golang/glog"
	"github.com/johnsiilver/wikipath/crawler/internal/watch"
)

func main() {
	flag.Parse()
	w, err := watch.NewHTTP(watch.SimpleEnWiki)
	if err != nil {
		glog.Exit(err)
	}
	defer w.Close()

	sig, err := w.Watch()
	if err != nil {
		glog.Exit(err)
	}

	for r := range sig.Receive() {
		fmt.Println("data updated: ", r.Data().(string))
	}
}
