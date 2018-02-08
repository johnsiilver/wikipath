/*
Package nodify reads in the XML wiki and converts it to a set of nodes
in a B-Tree(2-3-4).

Nodes are then converted to a graph.
*/
package nodify

// TODO(johnsiilver): Performance optimize.  We are only able to get 2 CPUs
// used.  This might be due to streaming from disk.

// TODO(johnsiilver): Figure out where the index is.  Documentation seems to
// indicate there is an index for these files that has byte offsets for entries.
// That would allow splitting the file very simply over multiple machines.

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	parse "github.com/dustin/go-wikiparse"
	"github.com/golang/glog"
	"github.com/google/btree"
	"github.com/johnsiilver/golib/mmap"
)

// ID is a unique numeric identifier.  It implements btree.Item.
type ID uint64

// Less implements btree.Item.Less().
func (i ID) Less(than btree.Item) bool {
	if i < than.(ID) {
		return true
	}
	return false
}

// Node represents a page in the Wiki. Provides mutex locking around the
// Node when mutating the data in multiple threads.
type Node struct {
	sync.Mutex
	// ID is the numeric identifier for a page.
	ID ID
	// Title is the tile of that page.
	Title string
	// Links is a list of IDs the Node connects to.
	Links []ID
	// TitleLinks is a list of Node.Titles that this Node connects to.
	TitleLinks []string
}

// Less implements llrb.Item.
func (n *Node) Less(than btree.Item) bool {
	n2 := than.(*Node)
	if n.ID < n2.ID {
		return true
	}
	return false
}

var (
	errRedirect = fmt.Errorf("Page is a redirect")
	errNoData   = fmt.Errorf("Page had no data")
	errNoTitle  = fmt.Errorf("Page had no title")
)

// from converts a Page into a Node.  It returns an error if p == nil or
// the Page is a redirect.
func (n *Node) from(p *parse.Page) error {
	switch {
	case p.Title == "":
		return errNoTitle
	case p.Redir.Title != "":
		return errRedirect
	case len(p.Revisions) == 0:
		return errNoData
	}

	n.Title = p.Title
	n.ID = ID(p.ID)
	// TODO(johnsiilver): Need to verify that 0 is the latest revision.
	// Or that revisions even have an order.
	n.TitleLinks = parse.FindLinks(p.Revisions[0].Text)
	return nil
}

type TitleToID map[string]ID

// Wiki provides methods for creating graph nodes in a 2-3-4 tree out of
// Wikipedia XML.
type Wiki struct {
	nodifyCh   chan *parse.Page
	insertCh   chan *Node
	processing sync.WaitGroup

	treeMu sync.Mutex // Protects grouping below.
	tree   *btree.BTree
	toID   TitleToID

	eMU    sync.Mutex // Protects errors.
	errors []error

	pMU sync.Mutex // Prevents Parse from being called more than one at a time.
}

// New is the constructor for Wiki.  This spins up goroutines that last forever.
// There is no Close method because it is assumed that this will be used
// continuously after creation.
func New() *Wiki {
	w := &Wiki{
		nodifyCh: make(chan *parse.Page, 1000), // Set based on playing with testing blocking.
		insertCh: make(chan *Node, 10000),
		tree:     btree.New(4),
		toID:     make(TitleToID, 30000),
	}

	for i := 0; i < runtime.NumCPU(); i++ {
		go w.process()
	}
	go w.insert()

	return w
}

// Parse parses a file of wikipedia xml and puts it into a 2-3-4 tree.
// Can be reused, but only one Parse will be run at a time per object.
func (w *Wiki) Parse(p string) (*btree.BTree, TitleToID, error) {
	w.pMU.Lock()
	defer w.pMU.Unlock()

	f, err := os.Open(p)
	if err != nil {
		return nil, nil, fmt.Errorf("problems opening the file %s: %s", p, err)
	}
	defer f.Close()

	// MMAP the file for speed.
	m, err := mmap.NewMap(f, mmap.Prot(mmap.Read), mmap.Prot(mmap.Write), mmap.Flag(mmap.Private))
	if err != nil {
		return nil, nil, fmt.Errorf("problems mmapping the file %s: %s", p, err)
	}
	defer m.Close()

	parser, err := parse.NewParser(m)
	if err != nil {
		return nil, nil, fmt.Errorf("xml parser had problem opening file: %s", err)
	}

	for {
		page, err := parser.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, fmt.Errorf("parsing error: %s", err)
		}
		w.processing.Add(1)
		// TODO(johnsiilver): Remove this later, just used to optimize channel size.
		for {
			select {
			case w.nodifyCh <- page:
			default:
				glog.Infof("nodifyCh is blocking")
				continue
			}
			break
		}
	}

	glog.Infof("Waiting for processing to finish")
	w.processing.Wait()

	tree := w.tree
	w.tree = nil
	toID := w.toID
	w.toID = nil

	return tree, toID, nil
}

func (w *Wiki) process() {
	for p := range w.nodifyCh {
		w.pageToNode(p)
	}
}

func (w *Wiki) pageToNode(p *parse.Page) {
	defer w.processing.Done() // TODO(johnsiilver): do this better.

	n := &Node{}
	switch n.from(p) {
	case nil:
	case errRedirect:
		return
	case errNoTitle:
		w.appendErr(fmt.Errorf("page ID %d had no title", p.ID))
		return
	case errNoData:
		w.appendErr(fmt.Errorf("page(%d) %s: has no data", p.ID, p.Title))
		return
	}

	// TODO(johnsiilver): Remove this later, just used to optimize channel size.
	w.processing.Add(1)
	for {
		select {
		case w.insertCh <- n:
		default:
			glog.Infof("insertCh is blocking")
			continue
		}
		break
	}
}

func (w *Wiki) insert() {
	for n := range w.insertCh {
		// TODO(johnsiilver): A bulk insert method on the tree would be useful.
		i := w.tree.ReplaceOrInsert(n)
		if i != nil {
			panic(fmt.Sprintf("DANGER WILL ROBINSON: Page(%s) and Page(%s) share ID %d", n.Title, i.(*Node).Title, n.ID))
		}
		w.toID[n.Title] = n.ID
		w.processing.Done()
	}
}

func (w *Wiki) appendErr(e error) {
	w.eMU.Lock()
	defer w.eMU.Unlock()
	w.errors = append(w.errors, e)
}
