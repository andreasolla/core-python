package main

import (
	"C"
	"github.com/colinmarc/hdfs/v2"
	"bufio"
	"strings"
	"fmt"
)

var client *hdfs.Client = nil
var write_files map[int]*hdfs.FileWriter = make(map[int]*hdfs.FileWriter)
var writefilemap_code int = 0

var lines map[int]*bufio.Scanner = make(map[int]*bufio.Scanner)
var read_files map[int]*hdfs.FileReader = make(map[int]*hdfs.FileReader)
var sizes map[int]int = make(map[int]int)
var readfilemap_code int = 0

//export NewHdfsClient
func NewHdfsClient(hostPath *C.char) int {
	hp := C.GoString(hostPath)
	c, err := hdfs.New(hp)
	if err != nil {
		fmt.Println(err)
		return -1
	}
	client = c
	return 0
}

//export Close
func Close() int {
	// Close all open files
	for _, f := range write_files {
		f.Close()
	}
	for _, f := range read_files {
		f.Close()
	}
	
	err := client.Close()
	if err != nil {
	return -1
	}
	client = nil
	return 0
}

// //export Create
// func Create(filePath *C.char) int {
// 	fp := C.GoString(filePath)
// 	writer, err := client.Create(fp)
// 	if err != nil {
// 		return -1
// 	}
// 	map_code++
// 	filemap[map_code] = writer
// 	return map_code
// }

func Exists(filePath *C.char) bool {
	fp := C.GoString(filePath)
	_, err := client.Stat(fp)
	if err != nil {
		return false
	}
	return true
}

func isDir(filePath *C.char) bool {
	fp := C.GoString(filePath)
	f, err := client.Stat(fp)
	if err != nil {
		return false
	}
	return f.IsDir()
}

//export Open
func Open(filePath *C.char, blockSize int, mode int) int {
	fp := C.GoString(filePath)

	if mode == 114 {
		file, err := client.Open(fp)
		if err != nil {
			return -1
		}
		
		readfilemap_code++
		read_files[readfilemap_code] = file
		sizes[readfilemap_code] = blockSize

		return readfilemap_code
	} else {
		f, err := client.Stat(fp)
		writer := new(hdfs.FileWriter)

		if f != nil {
			writer, err = client.Append(fp)
		} else {
			dir := strings.Split(fp, "/")
			dir = dir[:len(dir)-1]
			dirpath := strings.Join(dir, "/") + "/"
			fmt.Println(dirpath)
			_ = client.MkdirAll(dirpath, 0777)
			writer, err = client.Create(fp)
		}
		
		if err != nil {
			return -1
		}

		writefilemap_code++
		write_files[writefilemap_code] = writer
		return writefilemap_code
	}
}

//export ReadLine
func ReadLine(file int) *C.char {
	scanner := lines[file]
	
	if scanner == nil {
		Seek(file, 0, 0)
		scanner = lines[file]
	}

	if scanner.Scan() {
		l := scanner.Text()
		return C.CString(l + "\n")
	}
	return nil
}

//export Seek
func Seek(file int, offset int, whence int) int {
	f := read_files[file]
	r, err := f.Seek(int64(offset), whence)

	if(err != nil) {
		return -1
	}

	data := make([]byte, sizes[file])
	_, err2 := f.Read(data)
	if err2 != nil {
		return -1
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	lines[file] = scanner

    return int(r)
}

//export Size
func Size(filePath *C.char) int64 {
	fp := C.GoString(filePath)
	
	f, err := client.Stat(fp)
	if err != nil {
		return -1
	}

	return f.Size()
}

//export Write
func Write(file int, line *C.char) int {
	f := write_files[file]
    // writer, err := client.Create(fp)
    // if err != nil {
    //     return -1
    // }
    // defer writer.Close()
	fmt.Println(C.GoString(line))
    data:= []byte(C.GoString(line))
	fmt.Println(data)
    _, err := f.Write(data)
    if err != nil {
        return -1
    }
    return 0
}

func main() {}