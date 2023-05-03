from ignis.executor.core.transport.IZlibTransport import IZlibTransport
from ignis.executor.core.transport.IMemoryBuffer import IMemoryBuffer
from ignis.executor.core.protocol.IObjectProtocol import IObjectProtocol


class IPartitionTest:

	def create(self):
		raise NotImplementedError()

	def elemens(self, n):
		raise NotImplementedError()

	# from ... to ...
	def _writeIterator(self, elems, part):
		it = part.writeIterator()
		for e in elems:
			it.write(e)

	def _readIterator(self, part):
		elems = list()
		it = part.readIterator()
		while it.hasNext():
			elems.append(it.next())
		return elems

	def _read(self, elems, part, native):
		buffer = IMemoryBuffer()
		zlib = IZlibTransport(buffer)
		proto = IObjectProtocol(zlib)
		proto.writeObject(obj=elems, native=native, listHeader=True)
		zlib.flush()
		part.read(buffer)

	def _write(self, part, native):
		buffer = IMemoryBuffer()
		part.write(buffer, 0, native)
		zlib = IZlibTransport(buffer)
		proto = IObjectProtocol(zlib)
		return proto.readObject()

	# tests

	def test_itWriteItRead(self):
		part = self.create()
		elems = self.elemens(100)
		self._writeIterator(elems, part)
		self.assertEqual(len(elems), len(part))
		result = self._readIterator(part)
		self.assertEqual(elems, result)

	def __itWriteTransRead(self, w_native):
		part = self.create()
		elems = self.elemens(100)
		self._writeIterator(elems, part)
		self.assertEqual(len(elems), len(part))
		result = self._write(part, w_native)
		self.assertEqual(elems, result)

	def test_itWriteTransRead(self):
		self.__itWriteTransRead(False)

	def test_itWriteTransNativeRead(self):
		self.__itWriteTransRead(True)

	def __transWriteItRead(self, r_native):
		part = self.create()
		elems = self.elemens(100)
		self._read(elems, part, r_native)
		self.assertEqual(len(elems), len(part))
		result = self._readIterator(part)
		self.assertEqual(elems, result)

	def test_transWriteItRead(self):
		self.__transWriteItRead(False)

	def test_transNativeWriteItRead(self):
		self.__transWriteItRead(True)

	def __transWriteTransRead(self, w_native, r_native):
		part = self.create()
		elems = self.elemens(100)
		self._read(elems, part, w_native)
		self.assertEqual(len(elems), len(part))
		result = self._write(part, r_native)
		self.assertEqual(elems, result)

	def test_transWriteTransRead(self):
		self.__transWriteTransRead(False, False)

	def test_transNativeWriteTransRead(self):
		self.__transWriteTransRead(True, False)

	def test_transWriteTransNativeRead(self):
		self.__transWriteTransRead(False, True)

	def test_transNativeWriteTransNativeRead(self):
		self.__transWriteTransRead(True, True)

	def test_clear(self):
		part = self.create()
		elems = self.elemens(100)
		self._writeIterator(elems, part)
		self.assertEqual(len(elems), len(part))
		part.clear()
		self.assertEqual(0, len(part))
		result = self._readIterator(part)
		self.assertEqual(0, len(result))

	def __append(self, r1_native, r2_native):
		part = self.create()
		elems = self.elemens(200)
		self._read(elems[0:100], part, r1_native)
		self._read(elems[100:200], part, r2_native)
		self.assertEqual(len(elems), len(part))
		result = self._readIterator(part)
		self.assertEqual(elems, result)

	def test_appendIgnisIgnis(self):
		self.__append(False, False)

	def test_appendIgnisNative(self):
		self.__append(False, True)

	def test_appendNativeIgnis(self):
		self.__append(True, False)

	def test_appendNativeNative(self):
		self.__append(True, True)

	def test_copy(self):
		part = self.create()
		part2 = self.create()
		elems = self.elemens(100)
		self._writeIterator(elems, part)
		part.copyTo(part2)
		self.assertEqual(len(elems), len(part))
		self.assertEqual(len(elems), len(part2))
		result = self._readIterator(part)
		result2 = self._readIterator(part2)
		self.assertEqual(elems, result)
		self.assertEqual(elems, result2)

	def test_move(self):
		part = self.create()
		part2 = self.create()
		elems = self.elemens(100)
		self._writeIterator(elems, part)
		part.moveTo(part2)
		self.assertEqual(0, len(part))
		self.assertEqual(len(elems), len(part2))
		result = self._readIterator(part)
		result2 = self._readIterator(part2)
		self.assertEqual(0, len(result))
		self.assertEqual(elems, result2)
