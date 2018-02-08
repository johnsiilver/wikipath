// Package watch provides an XMLWiki watcher that provides for watching
// an XML file on some storage system and signaling when that file changes.
package watch

import "github.com/johnsiilver/golib/signal"

// XMLWiki provides for watching a wikipedia xml file and signaling when the
// file changes.
type XMLWiki interface {
	// Watch provides signaling when our source data has updated and we have
	// successfully retrieved it. Signaler.Retrieve() will return the local
	// file path to the XML data as a string.
	// Watch should only be called once per instance.
	Watch() (signal.Signaler, error)
	// Close stops the watcher. May only be called once and the XMLWiki
	// should not have Watch called again.
	Close() error
}
