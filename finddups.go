package main

import (
	"bufio"
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	humanize "github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"
)

type hashFilename struct {
	hash     [16]byte // md5 hash
	filename string   // filename
}

type config struct {
	minFileSize int64 // the minimum file size we care about
	nWorkers    int   // number of workers to spawn
}

// md5sum returns the md5 hash of a file.
func md5sum(file string) ([16]byte, error) {
	h := md5.New()
	f, err := os.Open(file)
	if err != nil {
		return [16]byte{}, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 2*1024*1024)

	io.Copy(h, r)
	var y [16]byte
	copy(y[:], h.Sum(nil))
	return y, nil
}

// launchWorker creates a worker that takes a file from a channel of
// filenames, calculates the md5 of the file, and puts an md5Filename
// struct to an output channel.
func launchWorker(in <-chan string, out chan<- hashFilename, wg *sync.WaitGroup) {
	defer wg.Done()
	for file := range in {
		md5, err := md5sum(file)
		if err == nil {
			hashinfo := hashFilename{hash: md5, filename: file}
			out <- hashinfo
		}
	}
}

// traverseDirectory walks a directory tree from a root and
// checks file sizes. If two or more files have the same size,
// the file names are pushed onto a channel for md5 hashing.
func traverseDirectory(root string, fileChan chan<- string, cfg config) {

	defer close(fileChan)
	sizePath := make(map[int64][]string)

	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.Mode().IsRegular() {
			s := info.Size()
			if s > cfg.minFileSize {
				e := append(sizePath[s], path)
				sizePath[s] = e
				if len(e) > 1 { // this is the first or subsequent size match, send the path
					fileChan <- path
				}
				if len(e) == 2 { // this is our first duplicate; send out the first file
					fileChan <- e[0]
				}
			}
		}
		return nil
	})
}

// closer waits for all workers to exit and then closes the md5Filename channel.
func closer(ch chan hashFilename, wg *sync.WaitGroup) {
	defer close(ch)
	wg.Wait()
}

func main() {
	formatter := &log.TextFormatter{
		FullTimestamp: true,
	}

	nCPU := runtime.NumCPU()
	nWorkers := flag.Int("n", nCPU, "number of workers")
	minSizeStr := flag.String("min", "0", "minimum size of files to consider (default 0)")
	flag.Parse()
	minSize, err := humanize.ParseBytes(*minSizeStr)
	if err != nil {
		minSize = 0
		log.Warn("Invalid minimum size specified; defaulting to 0")
	}

	cfg := config{nWorkers: *nWorkers, minFileSize: int64(minSize)}
	log.SetFormatter(formatter)

	log.Info("Starting")
	start := time.Now()
	// make channels.
	fileChan := make(chan string, cfg.nWorkers)
	hashChan := make(chan hashFilename, cfg.nWorkers)

	var wg sync.WaitGroup

	wg.Add(cfg.nWorkers)

	// launch workers to take file names and calculate md5s
	for i := 0; i < cfg.nWorkers; i++ {
		go launchWorker(fileChan, hashChan, &wg)
	}

	go closer(hashChan, &wg)

	root := flag.Args()[0]

	go traverseDirectory(root, fileChan, cfg)

	// we wait for md5 results to show up on the channel,
	// and then add them to a map. The files that have the
	// same md5s are presumed to be identical.
	hashPath := make(map[[16]byte][]string)
	for hashedfile := range hashChan {
		hashPath[hashedfile.hash] = append(hashPath[hashedfile.hash], hashedfile.filename)
	}

	// go through the map, and for every md5 hash that has
	// more than one file associated with it, print the files.
	for _, dup := range hashPath {
		if len(dup) > 1 {
			fmt.Println("duplicate files: ", dup)
		}
	}
	elapsed := time.Since(start)
	log.Infof("Took %s.\n", elapsed.Round(1*time.Millisecond))
}
