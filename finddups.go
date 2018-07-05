package main

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
)

// md5sum returns the md5sum of a file.
func md5sum(file string) ([16]byte, error) {
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		log.Warn("error calculating md5sum for ", file)
		return [16]byte{}, err
	}
	return md5.Sum(contents), nil
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

// processSizePath takes the sizePath map and
// returns a list of lists, where the inner list is
// a list of duplicate files.
func processSizePath(sizePath map[int64][]string) ([][]string, int) {
	totalmd5s := 0
	dups := make([][]string, 0, 10)
	for _, paths := range sizePath {
		if len(paths) < 2 {
			continue
		}
		pathmd5 := map[[16]byte][]string{}
		for _, p := range paths {
			md5p, err := md5sum(p)
			totalmd5s++
			if err == nil {
				pathmd5[md5p] = append(pathmd5[md5p], p)
			}
		}
		for _, dupPaths := range pathmd5 {
			dups = append(dups, dupPaths)
		}
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
	_, totalmd5s := processSizePath(sizePath)
	for _, dup := range dups {
		fmt.Println("duplicate files: ", dup)
	}
	elapsed := time.Since(start)
	log.Infof("Total files processed: %d.", totalFiles)
	log.Infof("Total md5s: %d.", totalmd5s)
	log.Infof("Took %s.\n", elapsed.Round(1*time.Millisecond))
}
