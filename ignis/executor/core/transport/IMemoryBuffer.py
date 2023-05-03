import ctypes
import ctypes.util
import sys
from enum import Enum

from thrift.transport.TTransport import TTransportBase, CReadableTransport, TTransportException


class IBuffer:
    __libc = ctypes.cdll.LoadLibrary(ctypes.util.find_library('msvcrt') or ctypes.util.find_library('c'))
    __malloc = __libc.malloc
    __malloc.argtypes = [ctypes.c_size_t]
    __malloc.restype = ctypes.c_void_p
    __realloc = __libc.realloc
    __realloc.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    __realloc.restype = ctypes.c_void_p
    __free = __libc.free
    __free.argtypes = [ctypes.c_void_p]
    __free.restype = None

    def __init__(self, sz, address=None):
        self.__sz = sz
        self.__own = address is None
        if self.__own:
            self.__address = self.__malloc(sz)
        else:
            self.__address = address

    def __len__(self):
        return self.__sz

    __sizeof__ = __len__

    def __getitem__(self, index):
        return ctypes.string_at(self.__address + index.start, index.stop - index.start)

    def __setitem__(self, index, value):
        ctypes.memmove(self.__address + index.start, value, index.stop - index.start)

    def realloc(self, sz):
        if self.__own:
            self.__sz = sz
            self.__address = self.__realloc(self.__address, sz)
        else:
            raise BufferError("no buffer own")

    def free(self):
        if self.__own:
            self.__own = False
            self.__free(self.__address)

    def address(self, offset=0):
        return (ctypes.c_byte * (self.__sz - offset)).from_address(self.__address + offset)

    def offset(self, n):
        return IBuffer(self.__sz - n, self.__address + n)

    __del__ = free

    def __repr__(self):
        return self[0:self.__sz].hex()

    def padding(self, n, sz=None):
        ctypes.memmove(self.__address + n, self.__address, sz or (self.__sz - n))


class IMemoryBuffer(TTransportBase, CReadableTransport):
    """
    A shared memory buffer is a transport that simply reads from and writes to an
    in memory buffer. Anytime you call write on it, the data is simply placed
    into a buffer, and anytime you call read, data is read from that buffer.

    The buffers are allocated using shared memory.
    """

    defaultSize = 1024

    class MemoryPolicy(Enum):
        """
        This enum specifies how a ISharedMemoryBuffer should treat
        memory passed to it via constructors or resetBuffer.

        OBSERVE:
        ISharedMemoryBuffer will simply store a pointer to the memory.
        It is the callers responsibility to ensure that the pointer
        remains valid for the lifetime of the ISharedMemoryBuffer,
        and that it is properly cleaned up.
        Note that no data can be written to observed buffers.

        COPY:
        ISharedMemoryBuffer will make an internal copy of the buffer.
        The caller has no responsibilities.

        TAKE_OWNERSHIP:
        ISharedMemoryBuffer will become the "owner" of the buffer,
        and will be responsible for freeing it.
        The memory must have been allocated with malloc.
        """
        OBSERVE = 1
        COPY = 2
        TAKE_OWNERSHIP = 3

    def __init__(self, sz=None, buf=None, policy=MemoryPolicy.OBSERVE):
        """
        Construct a ISharedMemoryBuffer

        @param buf The initial contents of the buffer.
                ISharedMemoryBuffer will not write to it if policy == OBSERVE.
        @param sz     The size of @c buf.
        @param policy See @link MemoryPolicy @endlink .
        """
        if buf is None:
            if sz is None:
                self.__initCommon(None, IMemoryBuffer.defaultSize, True, 0)
            else:
                self.__initCommon(None, sz, True, 0)
        else:
            if policy == IMemoryBuffer.MemoryPolicy.OBSERVE or IMemoryBuffer.MemoryPolicy.TAKE_OWNERSHIP:
                self.__initCommon(buf, sz, policy == IMemoryBuffer.MemoryPolicy.TAKE_OWNERSHIP, sz)
            elif policy == IMemoryBuffer.MemoryPolicy.COPY:
                self.__initCommon(None, sz, True, 0)
                self.write(buf, sz)
            else:
                raise TTransportException(message="Invalid MemoryPolicy for IMemoryBuffer")

    def isOpen(self):
        return True

    def peek(self):
        return self.readEnd() < self.writeEnd()

    def open(self):
        pass

    def close(self):
        pass

    def getBuffer(self):
        return self.__buffer

    def getBufferAsBytes(self):
        return self.__buffer[0:self.__wBase]

    def resetBuffer(self, sz=None, buf=None, policy=MemoryPolicy.OBSERVE):
        if sz is None:
            self.__rBase = 0
            # It isn't safe to write into a buffer we don't own.
            if self.__owner:
                self.__wBase = 0
        else:
            self.__init__(sz, buf, policy)

    def readEnd(self):
        """
        return number of bytes read
        """
        return self.__rBase

    def writeEnd(self):
        """
        Return number of bytes written
        """
        return self.__wBase

    def availableRead(self):
        return self.writeEnd() - self.readEnd()

    def availableWrite(self):
        return self.getBufferSize() - self.writeEnd()

    def getBufferSize(self):
        return len(self.__buffer)

    def getMaxBufferSize(self):
        return self.__maxBufferSize

    def setMaxBufferSize(self, maxSize):
        if maxSize < self.__maxBufferSize:
            raise TTransportException(message="Maximum buffer size would be less than current buffer size")
        self.__maxBufferSize = maxSize

    def setBufferSize(self, new_size):
        if not self.__owner:
            raise TTransportException(message="resize in buffer we don't own")
        if new_size > self.__maxBufferSize:
            raise TTransportException(message="resize more than Maximum buffer size")
        self.__buffer.realloc(new_size)
        self.__size = new_size
        self.__rBase = min(self.__rBase, new_size)
        self.__wBase = min(self.__wBase, new_size)

    def read(self, sz):
        # Decide how much to give.
        give = min(sz, self.availableRead())

        old_rBase = self.__rBase
        self.__rBase = old_rBase + give
        return self.__buffer[old_rBase:self.__rBase]

    def write(self, buf):
        self._ensureCanWrite(len(buf))
        old_wBase = self.__wBase
        self.__wBase = old_wBase + len(buf)
        self.__buffer[old_wBase:self.__wBase] = buf

    def consume(self, sz):
        if sz <= self.__wBase - self.__rBase:
            self.__rBase_ += sz
        else:
            raise TTransportException(message="consumed more than available")

    def flush(self):
        pass

    def setReadBuffer(self, pos):
        if pos <= self.writeEnd():
            self.__rBase = pos
        else:
            raise TTransportException(message="seek read more than available")

    def setWriteBuffer(self, pos):
        if pos <= self.getBufferSize():
            self.__wBase = pos
        else:
            raise TTransportException(message="seek write more than available")

    def _ensureCanWrite(self, sz):
        # Check available space
        avail = self.availableWrite()
        if sz <= avail:
            return

        if not self.__owner:
            raise TTransportException(message="Insufficient space in external MemoryBuffer")

        # Grow the buffer as necessary.
        new_size = len(self.__buffer)
        while sz > avail:
            if new_size > self.__maxBufferSize / 2:
                if self.availableWrite() + self.__maxBufferSize - len(self.__buffer) < sz:
                    raise TTransportException(message="Internal buffer size overflow")
                new_size = self.__maxBufferSize
            new_size = new_size * 2 if new_size > 0 else 1
            avail = self.availableWrite() + (new_size - len(self.__buffer))
        self.setBufferSize(new_size)

    def __initCommon(self, buf, size, owner, wPos):
        self.__maxBufferSize = sys.maxsize

        if buf is None:
            buf = IBuffer(size)
        self.__buffer = buf
        self.__rBase = wPos
        self.__wBase = wPos
        self.__owner = owner

    # Implement the CReadableTransport interface.
    @property
    def cstringio_buf(self):
        return self.__buffer

    def cstringio_refill(self, partialread, reqlen):
        # only one shot at reading...
        raise EOFError()
