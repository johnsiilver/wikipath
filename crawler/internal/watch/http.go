package watch

import (
	"bytes"
	"compress/bzip2"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	httpLib "net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/cavaliercoder/grab"
	"github.com/golang/glog"
	"github.com/johnsiilver/golib/signal"
)

// Our file names in our directory.
const (
	currentXML   = "current.xml"
	compresseXML = "working.xml.bz2"
	workingXML   = "working.xml"
	currentMeta  = "current_meta"
	workingMeta  = "working_meta"
	dir          = "wikipath"
)

// WikiType represents the type of Wiki we are downloading.
type WikiType int

func (WikiType) isWikiType() {}

const (
	// UnknownWiki indicates that the type is not set.
	UnknownWiki WikiType = iota
	// EnWiki indicates that we are downloading the large english wikipedia.
	EnWiki
	// SimpleEnWiki indicates we are downloading the simple english wikipedia.
	SimpleEnWiki
)

// These represent default http file locations and file names.
const (
	enWikiLoc       = "https://dumps.wikimedia.org/enwiki/latest/"
	simpleEnWikiLoc = "https://dumps.wikimedia.org/simplewiki/latest/"
	enWikiFile      = "enwiki-latest-pages-meta-current.xml.bz2"
	simpleWikiFile  = "simplewiki-latest-pages-meta-current.xml.bz2"
)

// The locations of our dump files on the filesystem.
var (
	dirLoc         = path.Join(os.TempDir(), dir)
	dataLoc        = path.Join(dirLoc, currentXML)
	metaLoc        = path.Join(dirLoc, currentMeta)
	compressedLoc  = path.Join(dirLoc, compresseXML)
	workingDataLoc = path.Join(dirLoc, workingXML)
	workingMetaLoc = path.Join(dirLoc, workingMeta)
)

func init() {
	if err := os.MkdirAll(dirLoc, 0766); err != nil {
		panic(fmt.Sprintf("can't make our storage directory(%s): %s", dirLoc, err))
	}

	go func() {
		for {
			if flag.Parsed() {
				glog.Infof("data location: %s", dataLoc)
				glog.Infof("meta location: %s", metaLoc)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

// Meta includes meta data about our XML file.
// Note: The XML file probably has this data, but I don't want to read it for this purpose.
type Meta struct {
	// Time is the time the file was generated.
	Time time.Time
}

// http implements watcher.
type http struct {
	urlStr   string
	fileName string
	fileTime time.Time

	sig     signal.Signaler
	closeCh chan struct{}
}

// HTTPOption provides optional arguments to the NewHTTP constructor.
type HTTPOption func(h *http)

// Location is a custom location to locate the XML file.  Must be a directory
// listing.
func Location(url string) HTTPOption {
	return func(h *http) {
		h.urlStr = url
	}
}

// NewHTTP returns an XMLWiki that fetches the file from HTTP file servers.
// This watcher can retrieve a stored file after a system crash in order to
// avoid having to download the file again.
func NewHTTP(t WikiType, options ...HTTPOption) (XMLWiki, error) {
	w := &http{closeCh: make(chan struct{})}
	switch t {
	case EnWiki:
		w.urlStr = enWikiLoc
		w.fileName = enWikiFile
	case SimpleEnWiki:
		w.urlStr = simpleEnWikiLoc
		w.fileName = simpleWikiFile
	default:
		return nil, fmt.Errorf("NewHTTP arguement WikiType is not one we understand: %v", t)
	}

	for _, o := range options {
		o(w)
	}

	currTime, err := recovery()
	if err != nil {
		return nil, err
	}
	w.fileTime = currTime

	return w, nil
}

// Close implements XMLWiki.Close().
// May only be called once and the
func (h *http) Close() error {
	close(h.closeCh)
	return nil
}

// Watch implements XMLWiki.Watch().
// TODO(johnsiilver): Probably be good to add some timeouts by using context.
func (h *http) Watch() (signal.Signaler, error) {
	if _, err := h.fetch(); err != nil {
		return signal.Signaler{}, err
	}

	sig := signal.New()
	sig.Signal(dataLoc)
	go func() {
		defer sig.Close()
		tick := time.NewTicker(1 * time.Minute)
		for {
			select {
			case <-h.closeCh:
				return
			case <-tick.C:
				updated, err := h.fetch()
				if err != nil {
					glog.Errorf("problems doing a fetch: %s", err)
					continue
				}
				if updated {
					sig.Signal(dataLoc)
				}
			}
		}
	}()
	return sig, nil
}

// fetch will fetch a new copy of the source data and put it in our storage
// location if the source data is newer than our current data.
func (h *http) fetch() (updated bool, err error) {
	// TODO(johnsiilver): We should probably do this only once in New().
	ref, err := url.Parse(h.fileName)
	if err != nil {
		return false, fmt.Errorf("problem parsing ref url(%s): %s", h.fileName, err)
	}
	dlURL, err := url.Parse(h.urlStr)
	if err != nil {
		return false, fmt.Errorf("problem parsing base url(%s): %s", h.urlStr, err)
	}

	dlPath := dlURL.ResolveReference(ref).String()

	if err := removeWorking(); err != nil {
		return false, err
	}

	currentTime, err := h.indexEntry()
	if err != nil {
		return false, err
	}

	if !currentTime.After(h.fileTime) {
		glog.Infof("file on source is not newer than current data")
		return false, nil
	}

	glog.Infof("file at source is newer(%v) than current data(%v), grabbing(%s)...", dlPath, currentTime, h.fileTime)
	_, err = grab.Get(compressedLoc, dlPath)
	if err != nil {
		return false, err
	}

	if err := unbzip2(); err != nil {
		return false, err
	}

	if err := writeMeta(currentTime); err != nil {
		return false, err
	}

	if err := makeCurrent(); err != nil {
		return false, err
	}
	h.fileTime = currentTime
	return true, nil
}

// indexEntry grabs the latest download file for the xml file we are watching.
func (h *http) indexEntry() (time.Time, error) {
	glog.Infof("fetching index from %s", h.urlStr)
	resp, err := httpLib.Get(h.urlStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("could not grab the file index at %s: %s", h.urlStr, err)
	}
	defer resp.Body.Close()

	// TODO(johnsiilver): We can do this on a single pass without the buffer copy.
	var buff bytes.Buffer
	if _, err := io.Copy(&buff, resp.Body); err != nil {
		return time.Time{}, fmt.Errorf("problems copying the index from %s: %s", h.urlStr, err)
	}

	glog.Infof("parsing the index")
	// TODO(johnsiilver): Replace with lexer/parser or a suitable library.
	for _, line := range strings.Split(buff.String(), "\n") {
		if strings.Contains(line, h.fileName) {
			line = strings.TrimSpace(line)
			col := strings.Fields(line)
			if len(col) != 5 {
				glog.Infof("columns: %#+v", col)
				return time.Time{}, fmt.Errorf("could not correctly split the line:\n%s\nwant 4 columns and got %d columns", line, len(col))
			}
			col = col[1:] // Drop the <a

			return time.Parse("02-Jan-2006 15:04", fmt.Sprintf("%s %s", col[1], col[2]))
		}
	}
	return time.Time{}, fmt.Errorf("could not find an index entry that conformed the our file")
}

func removeWorking() error {
	for _, loc := range []string{workingMetaLoc, workingDataLoc, compressedLoc} {
		if _, err := os.Stat(loc); err != nil {
			continue
		}
		glog.Infof("found working file that must be removed: %s", loc)
		if err := os.Remove(loc); err != nil {
			return fmt.Errorf("cannot remove file %s", loc)
		}
	}
	return nil
}

func writeMeta(t time.Time) error {
	glog.Infof("writing the meta data to: %s", workingMetaLoc)
	defer glog.Infof("%s written", workingMetaLoc)
	f, err := os.OpenFile(workingMetaLoc, os.O_CREATE+os.O_EXCL+os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("could not write working meta file %s: %s", workingMeta, err)
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(Meta{Time: t}); err != nil {
		return fmt.Errorf("problem encoding our Meta data: %s", err)
	}
	return nil
}

// makeCurrent moves our files from the working location to the current location.
// Ordering here is important, it must mirror recovery so we can tell if
// a service error killed us in the middle of a switch.
// TODO(johnsiilver): Consider making this journalled instead.
func makeCurrent() error {
	glog.Infof("renaming file %s to %s", workingMetaLoc, metaLoc)
	if err := os.Rename(workingMetaLoc, metaLoc); err != nil {
		return err
	}
	glog.Infof("renaming file %s to %s", workingDataLoc, dataLoc)
	if err := os.Rename(workingDataLoc, dataLoc); err != nil {
		return err
	}
	return nil
}

// recovery is used to recovery our current data if it exists on the filesystem
// and fix any file renaming issues.
func recovery() (time.Time, error) {
	glog.Infof("recovery started")
	defer glog.Infof("reccovery completed")

	if _, err := os.Stat(workingMetaLoc); err == nil {
		glog.Infof("found working meta data")
		if err := os.Rename(workingMetaLoc, metaLoc); err != nil {
			return time.Time{}, fmt.Errorf("problem doing recovery on our meta data: %s", err)
		}
	}
	if _, err := os.Stat(workingDataLoc); err == nil {
		glog.Infof("found working data")
		if err := os.Rename(workingDataLoc, dataLoc); err != nil {
			return time.Time{}, fmt.Errorf("problemdoing recovery on our xml data: %s", err)
		}
	}

	// Not having a file generally means the file system does contain our data
	// from a run.  We will just create new ones.
	if _, err := os.Stat(dataLoc); err != nil {
		glog.Infof("no data to recover")
		return time.Time{}, nil
	}
	if _, err := os.Stat(metaLoc); err != nil {
		glog.Infof("no data to recover")
		return time.Time{}, nil
	}

	// Okay, retrieve our Meta.
	f, err := os.Open(metaLoc)
	if err != nil {
		return time.Time{}, fmt.Errorf("meta file(%s) exists but we can't open it: %s", metaLoc, err)
	}
	m := Meta{}
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&m); err != nil {
		return time.Time{}, fmt.Errorf("meta file(%s) doesn't decode: %s", metaLoc, err)
	}
	glog.Infof("latest data time is: %v", m.Time)
	return m.Time, nil
}

func unbzip2() error {
	glog.Infof("unbzip2 the source data")

	src, err := os.Open(compressedLoc)
	if err != nil {
		return fmt.Errorf("problem unbzipping the xml file: %s", err)
	}
	defer os.Remove(compressedLoc)
	defer src.Close() // This may happen twice, that's ok.

	dst, err := os.OpenFile(workingDataLoc, os.O_CREATE+os.O_EXCL+os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("problem unbzipping the xml file, can't create the tmpfile: %s", err)
	}
	defer dst.Close() // This may happen twice, that's ok.

	r := bzip2.NewReader(src)
	if _, err = io.Copy(dst, r); err != nil {
		return fmt.Errorf("problem unbizipping, can't copy uncompressed data into tmpfile: %s", err)
	}
	// If we get this far, we have to close them before doing a rename.
	src.Close()
	dst.Close()

	return nil
}
