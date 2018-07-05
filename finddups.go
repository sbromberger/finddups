package main

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

// md5sum returns the md5sum of a file.
func md5sum(file string) ([16]byte, error) {
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println("error calculating md5sum for ", file)
		return [16]byte{}, err
	}
	return md5.Sum(contents), nil
}

// processFile takes a path and updates sizePath, which is a map of
// file size to the path.
func processFile(path string, info os.FileInfo, err error, sizePath map[int64][]string, totalFiles *int) error {
	if err != nil {
		fmt.Println("error traversing ", path)
		return err
	}
	st, err := os.Stat(path)
	if err != nil {
		fmt.Println("error: cannot stat ", path)
		return err
	}
	// don't try to get the size of directories
	if st.IsDir() {
		return nil
	}
	size := st.Size()
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
	start := time.Now()
	sizePath := make(map[int64][]string)
	totalFiles := 0

	fmt.Println("Starting")
	root := os.Args[1]
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		return processFile(path, info, err, sizePath, &totalFiles)
	})
	if err != nil {
		fmt.Println("walkpath error")
	}
	dups, totalmd5s := processSizePath(sizePath)
	for _, dup := range dups {
		fmt.Println("duplicate files: ", dup)
	}
	elapsed := time.Since(start)
	fmt.Printf("total files processed: %d. Total md5s: %d. Took %s.\n", totalFiles, totalmd5s, elapsed)
}
