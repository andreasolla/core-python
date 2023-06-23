import ctypes
import os

hdfs_lib = ctypes.CDLL(os.environ.get('IGNIS_HOME') + "/core/python/libhdfsExplorer.so")
class CBlockInfo(ctypes.Structure):
    _fields_ = [("BlockID", ctypes.c_int),
                ("NumBytes", ctypes.c_int),
                ("IpAddr", ctypes.c_char_p)]

class BlockInfo():
    def __init__(self, BlockID, NumBytes, IpAddr):
        self.BlockID = BlockID
        self.NumBytes = NumBytes
        self.IpAddr = IpAddr.decode('utf-8')

def NewHdfsClient(host):
    hdfs_lib.NewHdfsClient.argtypes = [ctypes.c_char_p]
    hdfs_lib.Close.restype = ctypes.c_int
    return hdfs_lib.NewHdfsClient(host.encode('utf-8'))

def Close(file, mode):
    hdfs_lib.Close.argtypes = [ctypes.c_int, ctypes.c_int]
    hdfs_lib.Close.restype = ctypes.c_int
    return hdfs_lib.Close(file, ord(mode))

def CloseConnection():
    hdfs_lib.CloseConnection()

def FreeBlockInfo(estructura, size):
    hdfs_lib.FreeBlockInfo.argtypes = [ctypes.POINTER(CBlockInfo), ctypes.c_int]
    hdfs_lib.FreeBlockInfo(estructura, size)

def GetBlocks(file):
    hdfs_lib.GetBlockInfo.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_int)]
    hdfs_lib.GetBlockInfo.restype = ctypes.POINTER(CBlockInfo)
    size = ctypes.c_int()
    Cblocks = hdfs_lib.GetBlockInfo(file, ctypes.byref(size))

    #copio los datos de la estructura a una lista de objetos BLockInfo
    blocks = []
    for i in range(size.value):
        blocks.append(BlockInfo(Cblocks[i].BlockID, Cblocks[i].NumBytes, Cblocks[i].IpAddr))
    FreeBlockInfo(Cblocks, size.value)
    return blocks

def Open(path, mode):
    hdfs_lib.Open.argtypes = [ctypes.c_char_p, ctypes.c_int]
    hdfs_lib.Open.restype = ctypes.c_int
    return hdfs_lib.Open(ctypes.c_char_p(path.encode('utf-8')), ord(mode))

def ReadLine(file):
    hdfs_lib.ReadLine.argtypes = [ctypes.c_int]
    hdfs_lib.ReadLine.restype = ctypes.c_char_p
    return hdfs_lib.ReadLine(file)

def Seek(file, offset, whence):
    hdfs_lib.Seek.argtypes = [ctypes.c_int, ctypes.c_int64, ctypes.c_int]
    hdfs_lib.Seek.restype = ctypes.c_int
    return hdfs_lib.Seek(file, offset, whence)

def Size(path):
    hdfs_lib.Size.argtypes = [ctypes.c_char_p]
    hdfs_lib.Size.restype = ctypes.c_int64
    return hdfs_lib.Size(ctypes.c_char_p(path.encode('utf-8')))

def Write(file, line):
    hdfs_lib.Write.argtypes = [ctypes.c_int, ctypes.c_char_p]
    hdfs_lib.Write.restype = ctypes.c_int
    return hdfs_lib.Write(file , ctypes.c_char_p((line + "\n").encode('utf-8')))