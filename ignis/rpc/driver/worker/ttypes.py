#
# Autogenerated by Thrift Compiler (0.15.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import sys
import ignis.rpc.driver.exception.ttypes
import ignis.rpc.driver.dataframe.ttypes
import ignis.rpc.source.ttypes

from thrift.transport import TTransport
all_structs = []


class IWorkerId(object):
    """
    Attributes:
     - cluster
     - worker

    """


    def __init__(self, cluster=None, worker=None,):
        self.cluster = cluster
        self.worker = worker

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.cluster = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I64:
                    self.worker = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('IWorkerId')
        if self.cluster is not None:
            oprot.writeFieldBegin('cluster', TType.I64, 1)
            oprot.writeI64(self.cluster)
            oprot.writeFieldEnd()
        if self.worker is not None:
            oprot.writeFieldBegin('worker', TType.I64, 2)
            oprot.writeI64(self.worker)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.cluster is None:
            raise TProtocolException(message='Required field cluster is unset!')
        if self.worker is None:
            raise TProtocolException(message='Required field worker is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(IWorkerId)
IWorkerId.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'cluster', None, None, ),  # 1
    (2, TType.I64, 'worker', None, None, ),  # 2
)
fix_spec(all_structs)
del all_structs
