import zlib

from thrift.transport.TZlibTransport import TZlibTransport, BufferIO


class IZlibTransport(TZlibTransport):

    def __init__(self, trans, compresslevel=6):
        super().__init__(trans, compresslevel)
        self.__trans = trans
        self.__in_compression = compresslevel
        self.__winit = False
        self.__rinit = False
        self.comp_buffer = True

    def reset(self):
        super().__init__(self.__trans, self.compresslevel)
        self.__in_compression = self.compresslevel
        self.__winit = False
        self.__rinit = False

    def readComp(self, sz):
        if not self.comp_buffer:
            return super().readComp(sz)
        old = self.bytes_in
        while not super().readComp(max(sz, 256)) and old != self.bytes_in:
            old = self.bytes_in
        return True

    def flush(self):
        """Flush any queued up data in the write buffer and ensure the
        compression buffer is flushed out to the underlying transport
        """
        if not self.__winit:
            self.write(bytes(0))
        elif self.compresslevel == 0:
            self.__trans.flush()
            return

        wout = self._TZlibTransport__wbuf.getvalue()
        if len(wout) > 0:
            zbuf = self._zcomp_write.compress(wout)
            self.bytes_out += len(wout)
            self.bytes_out_comp += len(zbuf)
        else:
            zbuf = b''  # Fix thrift base error
        ztail = self._zcomp_write.flush(zlib.Z_FULL_FLUSH)  # like c++ Z_FULL_FLUSH make flushed block independents
        self.bytes_out_comp += len(ztail)
        if (len(zbuf) + len(ztail)) > 0:
            self._TZlibTransport__wbuf = BufferIO()
            self._TZlibTransport__trans.write(zbuf + ztail)
        self._TZlibTransport__trans.flush()

    def getTransport(self):
        return self.__trans

    def getInCompression(self):
        if not self.__rinit:
            self.read(0)
        return self.__in_compression

    def isOpen(self):
        if self.__in_compression > 0:
            return super().isOpen()
        else:
            return self.__trans.isOpen()

    def read(self, sz):
        if not self.__rinit:
            self.__in_compression = self.__trans.readAll(1)[0]
            self.__rinit = True
        if self.__in_compression > 0:
            return super().read(sz)
        else:
            return self.__trans.read(sz)

    def write(self, buf):
        if not self.__winit:
            self.__winit = True
            self.__trans.write(bytes([self.compresslevel]))
        if self.compresslevel > 0:
            super().write(buf)
            if self._TZlibTransport__wbuf.tell() > self.DEFAULT_BUFFSIZE:
                self.flush()
        else:
            self.__trans.write(buf)

    @property
    def cstringio_buf(self):
        if self.__in_compression > 0:
            return super().cstringio_buf()
        else:
            return self.__trans.cstringio_buf()

    def cstringio_refill(self, partialread, reqlen):
        if self.__in_compression > 0:
            return super().cstringio_refill(self, partialread, reqlen)
        else:
            return self.__trans.cstringio_refill(partialread, reqlen)


class IZlibTransportFactory:

    def __init__(self, compression=6):
        self.__compression = compression

    def getTransport(self, trans):
        return IZlibTransport(trans, self.__compression)


class TZlibTransportFactoryExt:

    def __init__(self, compression=6):
        self.__compression = compression

    def getTransport(self, trans):
        return TZlibTransport(trans, self.__compression)
