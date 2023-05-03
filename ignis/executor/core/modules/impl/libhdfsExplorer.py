import ctypes
import os

hdfs_lib = ctypes.CDLL(os.environ.get('IGNIS_HOME') + "/core/python/libhdfsExplorer.so")

def NewHdfsClient(host):
    hdfs_lib.NewHdfsClient.argtypes = [ctypes.c_char_p]
    return hdfs_lib.NewHdfsClient(host.encode('utf-8'))

def Close():
    hdfs_lib.Close()

#create

#exists

#isDir

def Open(path, size, mode):
    hdfs_lib.Open.argtypes = [ctypes.c_char_p, ctypes.c_int, ctypes.c_int]
    hdfs_lib.Open.restype = ctypes.c_int
    return hdfs_lib.Open(ctypes.c_char_p(path.encode('utf-8')), size, ord(mode))

def ReadLine(file):
    hdfs_lib.ReadLine.argtypes = [ctypes.c_int]
    hdfs_lib.ReadLine.restype = ctypes.c_char_p
    return hdfs_lib.ReadLine(file)

def Seek(file, offset, whence):
    hdfs_lib.Seek.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int]
    hdfs_lib.Seek.restype = ctypes.c_int
    return hdfs_lib.Seek(file, offset, whence)

def Size(path):
    hdfs_lib.Size.argtypes = [ctypes.c_char_p]
    hdfs_lib.Size.restype = ctypes.c_int
    return hdfs_lib.Size(ctypes.c_char_p(path.encode('utf-8')))

def Write(file, line):
    hdfs_lib.Write.argtypes = [ctypes.c_int, ctypes.c_char_p]
    hdfs_lib.Write.restype = ctypes.c_int
    return hdfs_lib.Write(file , ctypes.c_char_p(line.encode('utf-8')))
