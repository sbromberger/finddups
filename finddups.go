package main

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

func md5sum(file string) ([16]byte, error) {
	dat, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println("error calculating md5sum for ", file)
		return [16]byte{}, err
	}
	return md5.Sum(dat), nil
}

func processFile(path string, info os.FileInfo, err error, sizePath map[int64][]string) error {
	if err != nil {
		fmt.Println("error traversing ", path)
		return err
	}
	st, err := os.Stat(path)
	if err != nil {
		fmt.Println("error: cannot stat ", path)
		return err
	}
	if st.IsDir() {
		return nil
	}
	size := st.Size()
	sizePath[size] = append(sizePath[size], path)
	return nil
}

func processSizePath(sizePath map[int64][]string) [][]string {
	dups := make([][]string, 0, 10)
	for _, paths := range sizePath {
		pathmd5 := map[[16]byte][]string{}
		for _, p := range paths {
			md5p, err := md5sum(p)
			if err == nil {
				pathmd5[md5p] = append(pathmd5[md5p], p)
			}
		}
		for _, dupPaths := range pathmd5 {
			dups = append(dups, dupPaths)
		}
	}
	prunedDups := make([][]string, 0, 10)
	for _, dup := range dups {
		if len(dup) > 1 {
			prunedDups = append(prunedDups, dup)
		}
	}
	return prunedDups
}

func main() {
	sizePath := make(map[int64][]string)
	fmt.Println("Starting")
	root := os.Args[1]
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		return processFile(path, info, err, sizePath)
	})
	if err != nil {
		fmt.Println("walkpath error")
	}
	dups := processSizePath(sizePath)
	for _, dup := range dups {
		fmt.Println("duplicate files: ", dup)
	}
}
