package main

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
)

type md5Filename struct {
	md5      [16]byte
	filename string
}

// md5sum returns the md5sum of a file.
func md5sum(file string) ([16]byte, error) {
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		log.Warn("error calculating md5sum for ", file)
		return [16]byte{}, err
	}
	return md5.Sum(contents), nil
}

func md5sum2(file string) ([16]byte, error) {
	h := md5.New()
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	r := bufio.NewReaderSize(f, 2*1024*1024)

	io.Copy(h, r)
	var y [16]byte
	copy(y[:], h.Sum(nil))
	return y, nil
}

// processFile takes a path and updates sizePath, which is a map of
// file size to the path.
func processFile(path string, info os.FileInfo, sizePath map[int64][]string, totalFiles *int) error {
	// st, err := os.Stat(path)
	// if err != nil {
	// 	log.Warn("skipping invalid file ", path)
	// 	return err
	// }
	// don't try to get the size of directories
	if !info.Mode().IsRegular() {
		return nil
	}

	size := info.Size()
	sizePath[size] = append(sizePath[size], path)
	*totalFiles++
	return nil
}

func spawnWorker(filechan <-chan string, md5chan chan<- *md5Filename, md5fn func(string) ([16]byte, error)) {
	for filename := range filechan {
		filemd5, err := md5fn(filename)
		if err == nil {
			md5chan <- &md5Filename{filemd5, filename}
		} else {
			md5chan <- nil
		}
	}
}

// processSizePath takes the sizePath map and
// returns a list of lists, where the inner list is
// a list of duplicate files.
func processSizePath(sizePath map[int64][]string, md5fn func(string) ([16]byte, error)) ([][]string, int) {

	md5chan := make(chan *md5Filename, 100) // can be nil
	filechan := make(chan string, 100)
	nWorkers := 10

	for i := 0; i < nWorkers; i++ {
		go spawnWorker(filechan, md5chan, md5sum2)
	}
	dups := make([][]string, 0, 10)
	pathmd5 := map[[16]byte][]string{}
	totalmd5s := 0
	for _, paths := range sizePath {
		if len(paths) < 2 {
			continue
		}

		// at this point we have 2 or more files with the same size.
		// put 'em in the channel.

		for _, p := range paths {
			filechan <- p
			totalmd5s++
		}
	}

	for i := 0; i < totalmd5s; i++ {
		md5ForFile := <-md5chan
		if md5ForFile != nil {
			pathmd5[md5ForFile.md5] = append(pathmd5[md5ForFile.md5], md5ForFile.filename)
		}
	}

	for _, dupPaths := range pathmd5 {
		dups = append(dups, dupPaths)
	}

	// cull all lists with only one file. There are no duplicates.
	prunedDups := make([][]string, 0, 10)
	for _, dup := range dups {
		if len(dup) > 1 {
			prunedDups = append(prunedDups, dup)
		}
	}
	return prunedDups, totalmd5s
}

func main() {
	formatter := &log.TextFormatter{
		FullTimestamp: true,
	}
	log.SetFormatter(formatter)

	log.Info("Starting")
	start := time.Now()
	sizePath := make(map[int64][]string)
	totalFiles := 0

	root := os.Args[1]
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		return processFile(path, info, sizePath, &totalFiles)
	})
	if err != nil {
		log.Debug("walkpath error")
	}
	// dups, totalmd5s := processSizePath(sizePath)
	dups, totalmd5s := processSizePath(sizePath, md5sum2)
	for _, dup := range dups {
		fmt.Println("duplicate files: ", dup)
	}
	elapsed := time.Since(start)
	log.Infof("Total files processed: %d.", totalFiles)
	log.Infof("Total md5s: %d.", totalmd5s)
	log.Infof("Took %s.\n", elapsed.Round(1*time.Millisecond))
}
