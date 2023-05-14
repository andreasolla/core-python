package main

/*
#include <stdlib.h>
typedef struct {
	int		blockid;
	int		numbytes;
	char 	*ipaddr;
} BlockInfo;
*/
import "C"
import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unsafe"

	"github.com/andreasolla/hdfs/v2"
)

type BlockInfoGo struct {
	BlockId  int
	NumBytes int
	IpAddr   *C.char
}

// Mapas sincronizados para evitar problemas de concurrencia
type SafeMap struct {
	mu sync.RWMutex
	m  map[int]interface{}
}

func (sm *SafeMap) Put(key int, value interface{}) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}

func (sm *SafeMap) Get(key int) (interface{}, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	value, ok := sm.m[key]
	return value, ok
}

func (sm *SafeMap) Delete(key int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.m, key)
}

// Ints sincronizados para evitar problemas de concurrencia
type SafeInt struct {
	mu sync.RWMutex
	i  int
}

func (i *SafeInt) AddAndGet() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.i = i.i + 1
	return i.i
}

func (i *SafeInt) SubAndGet() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.i = i.i - 1
	return i.i
}

// Client sincronizado para evitar problemas de concurrencia
type SafeClient struct {
	mu     sync.RWMutex
	client *hdfs.Client
}

//Variables globales

var connected_client SafeClient = SafeClient{client: nil}
var active_threads_count SafeInt = SafeInt{i: 0}

var write_files SafeMap = SafeMap{m: make(map[int]interface{})}
var writefilemap_code SafeInt = SafeInt{i: 0}

var read_files SafeMap = SafeMap{m: make(map[int]interface{})}
var positions SafeMap = SafeMap{m: make(map[int]interface{})}
var readfilemap_code SafeInt = SafeInt{i: 0}

//--------------------Funciones exportadas--------------------

//export NewHdfsClient
func NewHdfsClient(hostPath *C.char) int {
	connected_client.mu.Lock()
	defer connected_client.mu.Unlock()

	active_threads_count.AddAndGet()

	if connected_client.client == nil {
		hp := C.GoString(hostPath)
		c, err := hdfs.New(hp)
		if err != nil {
			fmt.Println(err)
			return -1
		}
		connected_client.client = c
	}
	return 0
}

//export CloseConnection
func CloseConnection() int {
	connected_client.mu.Lock()
	defer connected_client.mu.Unlock()

	count := active_threads_count.SubAndGet()

	// Si no hay más threads activos, se cierra la conexión
	if connected_client.client != nil && count == 0 {
		err := connected_client.client.Close()
		if err != nil {
			return -1
		}
		connected_client.client = nil
	}
	return 0
}

//export Open
func Open(filePath *C.char, mode int) int {
	connected_client.mu.Lock()
	defer connected_client.mu.Unlock()

	fp := C.GoString(filePath)

	// r=114 ASCII code
	if mode == 114 {
		file, err := connected_client.client.Open(fp)
		if err != nil {
			return -1
		}

		position := readfilemap_code.AddAndGet()
		read_files.Put(position, file)
		prueba, _ := read_files.Get(position)
		p := prueba.(*hdfs.FileReader)
		fmt.Println(p)
		return position
	} else {
		f, err := connected_client.client.Stat(fp)
		writer := new(hdfs.FileWriter)

		if f != nil {
			writer, err = connected_client.client.Append(fp)
		} else {
			dir := strings.Split(fp, "/")
			dir = dir[:len(dir)-1]
			dirpath := strings.Join(dir, "/") + "/"
			_ = connected_client.client.MkdirAll(dirpath, 0777)
			writer, err = connected_client.client.Create(fp)
		}

		if err != nil {
			return -1
		}

		position := writefilemap_code.AddAndGet()
		write_files.Put(position, writer)
		return position
	}
}

//export Close
func Close(file int, mode int) int {
	connected_client.mu.Lock()
	defer connected_client.mu.Unlock()

	// r=114 ASCII code
	if mode == 114 {
		f, _ := read_files.Get(file)
		if f != nil {
			f := f.(*hdfs.FileReader)

			err := f.Close()
			if err != nil {
				return -1
			}
			read_files.Delete(file)
			positions.Delete(file)
		}
	} else {
		f, _ := write_files.Get(file)
		if f != nil {
			f := f.(*hdfs.FileWriter)
			err := f.Close()
			if err != nil {
				return -1
			}
			write_files.Delete(file)
		}
	}

	return 0
}

//export GetBlockInfo
func GetBlockInfo(file int, size *C.int) *C.BlockInfo {
	f, _ := read_files.Get(file)
	fmt.Println(f)
	if f != nil {
		f := f.(*hdfs.FileReader)
		fmt.Println("GetBlocks")
		blocks, err := f.GetBlocks()

		if err != nil {
			return nil
		}

		blockInfos := make([]BlockInfoGo, len(blocks))
		fmt.Println(len(blocks))
		for i, block := range blocks {
			ip := block.GetIpAddr()[0]
			fmt.Println(ip)
			fmt.Println(block.GetBlockId())
			fmt.Println(block.GetNumBytes())
			blockInfos[i] = BlockInfoGo{BlockId: int(block.GetBlockId()), NumBytes: int(block.GetNumBytes()), IpAddr: C.CString(ip)}
		}

		array := (*C.BlockInfo)(C.malloc(C.size_t(len(blockInfos)) * C.size_t(unsafe.Sizeof(C.BlockInfo{}))))
		fmt.Println(len(blockInfos))
		for i, item := range blockInfos {
			ptr := (*C.BlockInfo)(unsafe.Pointer(uintptr(unsafe.Pointer(array)) + uintptr(i)*unsafe.Sizeof(C.BlockInfo{})))
			ptr.blockid = C.int(item.BlockId)
			ptr.numbytes = C.int(item.NumBytes)
			ptr.ipaddr = item.IpAddr

			fmt.Println(ptr.blockid)
			fmt.Println(ptr.numbytes)
			fmt.Println(ptr.ipaddr)
			fmt.Println(item.IpAddr)
		}

		*size = C.int(len(blockInfos))
		return array
	}
	return nil
}

//export FreeBlockInfo
func FreeBlockInfo(array *C.BlockInfo, size int) {
	if array != nil {
		sliceHeader := reflect.SliceHeader{
			Data: uintptr(unsafe.Pointer(array)),
			Len:  int(size),
			Cap:  int(size),
		}
		blockInfos := *(*[]C.BlockInfo)(unsafe.Pointer(&sliceHeader))
		for _, blockInfo := range blockInfos {
			C.free(unsafe.Pointer(blockInfo.ipaddr))
		}
		C.free(unsafe.Pointer(array))
	}
}

//export ReadLine
func ReadLine(file int) *C.char {
	f, _ := read_files.Get(file)
	offset, _ := positions.Get(file)

	if offset == nil {
		offset = 0
	}

	if f != nil {
		f := f.(*hdfs.FileReader)
		offset := offset.(int)
		l, _ := f.ReadLine(int64(offset))
		line := C.CString(l + "\n")

		pos := offset + len(l) + 1
		positions.Put(file, pos)

		return line
	}

	return nil
}

//export Seek
func Seek(file int, offset int, whence int) int {
	f, _ := read_files.Get(file)
	if f != nil {
		positions.Put(file, offset)
		return int(0)
	}
	return -1
}

//export Size
func Size(filePath *C.char) int64 {
	connected_client.mu.Lock()
	defer connected_client.mu.Unlock()

	fp := C.GoString(filePath)

	f, err := connected_client.client.Stat(fp)
	if err != nil {
		return -1
	}

	return f.Size()
}

//export Write
func Write(file int, line *C.char) int {
	f, _ := write_files.Get(file)

	data := []byte(C.GoString(line) + "\n")
	if f != nil {
		f := f.(*hdfs.FileWriter)
		_, err := f.Write(data)
		if err != nil {
			return -1
		}
	}
	return 0
}

func main() {}
