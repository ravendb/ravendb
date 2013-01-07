#-----------------------------------------------------------------------
# <copyright file="column_tests.py" company="Microsoft Corporation">
# Copyright (c) Microsoft Corporation.
# </copyright>
#-----------------------------------------------------------------------

# TODO:
# Add method for append
# Add R/O tests (take a populated stream, restricted set of actions)
# Verify data from read
#    - allocate buffer for read
#    - compare them

import clr
import System
from System import Array, Random
from System.Diagnostics import Stopwatch
from System.IO import Stream, MemoryStream, FileStream, Path, SeekOrigin, FileMode, File
from System.Security.Cryptography import SHA512Managed
from System.Text import StringBuilder

clr.AddReferenceByPartialName('Esent.Interop')
from Microsoft.Isam.Esent.Interop import (
    Api,
    ColumnStream,
    CommitTransactionGrbit,
    CreateDatabaseGrbit,
    Instance,
    JET_COLUMNDEF,
    JET_coltyp,
    JET_prep,
    OpenTableGrbit,
    Session,
    )

import random

class StreamOperator(object):

    def __init__(self, maxsize):
        self._maxsize = maxsize
        self._data = Array.CreateInstance(System.Byte, self._maxsize)
        Random().NextBytes(self._data)
        self._log = StringBuilder()
        self._currentoffset = 0
        self._currentlength = 0

    def setLength(self, streams):
        newlength = random.randrange(0,self._maxsize)
        self._currentlength = newlength
        self._currentoffset = min(self._currentoffset, newlength)
        self._trace('stream.SetLength(%d)' % newlength)
        for s in streams:
            s.SetLength(newlength)
            
    def setPosition(self, streams):
        newposition = random.randrange(0,self._maxsize)
        self._currentoffset = newposition
        self._trace('stream.Position = %d' % newposition)
        for s in streams:
            s.Position = newposition    

    def seek(self, streams):
        origin = random.choice([SeekOrigin.Begin, SeekOrigin.Current, SeekOrigin.End])
        newoffset = random.randrange(0,self._maxsize)
        if SeekOrigin.Begin == origin:
            delta = newoffset - 0
        elif SeekOrigin.Current == origin:
            delta = newoffset - self._currentoffset
        elif SeekOrigin.End == origin:
            delta = newoffset - self._currentlength
        self._currentoffset = newoffset
        self._trace('stream.Seek(%d, %s)' % (delta, origin))
        for s in streams:
            p = s.Seek(delta, origin)
            self._check(p == self._currentoffset, 'got offset %d from seek. expected %d' % (p, self._currentoffset))
        
    def write(self, streams):
        count = random.randrange(0, self._maxsize - self._currentoffset)
        maxoffset = self._maxsize - count
        offset = random.randrange(0, maxoffset)
        self._currentoffset += count
        self._currentlength = max(self._currentlength, self._currentoffset)
        self._trace('stream.Write(data, %d, %d)' % (offset, count))
        for s in streams:
            s.Write(self._data, offset, count)
        
    def read(self, streams):
        count = random.randrange(0, self._maxsize)
        buffer = Array.CreateInstance(System.Byte, self._maxsize)
        maxoffset = self._maxsize - count
        offset = random.randrange(0, maxoffset)
        toread = min(count, self._currentlength - self._currentoffset)
        toread = max(0, toread)
        self._currentoffset += toread    
        self._trace('stream.Read(data, %d, %d)' % (offset, count))
        for s in streams:
            r = s.Read(buffer, offset, count)
            self._check(r == toread, 'got %d bytes from read. expected %d' % (r, toread))
        
    def checkLength(self, streams):
        for s in streams:
            l = s.Length
            self._check(l == self._currentlength, 'stream is %d bytes. expected %d' % (l, self._currentlength))

    def checkPosition(self, streams):
        for s in streams:
            p = s.Position
            self._check(p == self._currentoffset, 'position is %d bytes. expected %d' % (p, self._currentoffset))
            
    def rewind(self, streams):
        self._currentoffset = 0
        self._trace('stream.Seek(0, SeekOrigin.Begin)')
        for s in streams:
            p = s.Seek(0, SeekOrigin.Begin)
            self._check(p == self._currentoffset, 'got offset %d from seek. expected %d' % (p, self._currentoffset))
            
    def clear(self, streams):
        self._currentlength = 0
        self._trace('stream.SetLength(0)')
        self._currentoffset = 0
        self._trace('stream.Seek(0, SeekOrigin.Begin)')
        for s in streams:
            s.SetLength(0)
            p = s.Seek(0, SeekOrigin.Begin)
            self._check(p == self._currentoffset, 'got offset %d from seek. expected %d' % (p, self._currentoffset))
    
    def compare(self, streams):
        self.checkLength(streams)
        self.checkPosition(streams)
        self.rewind(streams)
        expected = None
        hash = SHA512Managed()
        for s in streams:            
            actual = hash.ComputeHash(s)
            if None <> expected:
                self._check(self._compareHashValues(expected, actual), 'hash mismatch %s/%s' % (expected, actual))    
            expected = actual
    
    def _compareHashValues(self, a, b):
        for i in xrange(len(a)):
            if a[i] <> b[i]:
                return False
        return True
        
    def _trace(self, s):
        self._log.AppendLine('%s # position = %d, length = %d' % (s, self._currentoffset, self._currentlength))
        
    def _check(self, condition, message):
        if not condition:
            print message
            print self._log
            raise AssertionError

def randomOperations(numops, streams):
    length = random.randrange(0,1024*1024)
    operator = StreamOperator(length)
    actions = [    operator.setLength, operator.seek, operator.write, operator.read,
                operator.checkLength, operator.checkPosition, operator.rewind,
                operator.clear, operator.setPosition]
    for i in xrange(numops):
        f = random.choice(actions)
        f(streams)
    operator.compare(streams)

def testMemoryStreams():
    print 'Verifying MemoryStreams'
    stopwatch = Stopwatch.StartNew()
    for i in xrange(64):
        streams = [MemoryStream() for x in xrange(2)]
        randomOperations(128, streams)
    stopwatch.Stop()
    print '%s' % stopwatch.Elapsed

def testFileStream():
    print 'Verifying FileStream against MemoryStream'
    stopwatch = Stopwatch.StartNew()
    for i in xrange(128):
        m = MemoryStream()
        file = Path.GetTempFileName()
        f = FileStream(file, FileMode.Open)
        try:
            streams = [m, f]
            randomOperations(128, streams)
        finally:
            f.Close()
            File.Delete(file)
    stopwatch.Stop()
    print '%s' % stopwatch.Elapsed

def testColumnStream():
    print 'Verifying ColumnStream against MemoryStream'
    timer = Stopwatch.StartNew()
    instance = Instance('ColumnStreamTest')
    instance.Parameters.MaxVerPages = 1024
    instance.Parameters.CircularLog = True
    instance.Init()
    
    bookmark = Array.CreateInstance(System.Byte, 255)    
    
    try:
        session = Session(instance)
        dbid = Api.JetCreateDatabase(session, 'ColumnStream.db', '', CreateDatabaseGrbit.OverwriteExisting)
        Api.JetBeginTransaction(session)
        tableid = Api.JetCreateTable(session, dbid, 'table', 0, 100)

        columndef = JET_COLUMNDEF(coltyp=JET_coltyp.LongBinary)
        columnid = Api.JetAddColumn(session, tableid, 'LvColumn', columndef, None, 0)

        Api.JetCloseTable(session, tableid)
        Api.JetCommitTransaction(session, CommitTransactionGrbit.LazyFlush)
        tableid = Api.JetOpenTable(session, dbid, 'table', None, 0, OpenTableGrbit.None)

        for i in xrange(64):
            runtimer = Stopwatch.StartNew()
            
            Api.JetBeginTransaction(session)
            Api.JetPrepareUpdate(session, tableid, JET_prep.Insert)
            bookmarksize = Api.JetUpdate(session, tableid, bookmark, 255)
            Api.JetCommitTransaction(session, CommitTransactionGrbit.LazyFlush)
            Api.JetGotoBookmark(session, tableid, bookmark, bookmarksize)

            Api.JetBeginTransaction(session)
            Api.JetPrepareUpdate(session, tableid, JET_prep.Insert)
            streams = [MemoryStream(), ColumnStream(session, tableid, columnid)]
            randomOperations(64, streams)
            Api.JetUpdate(session, tableid)
            Api.JetCommitTransaction(session, CommitTransactionGrbit.LazyFlush)

            runtimer.Stop()
            print '\t%s' % runtimer.Elapsed
            
    finally:
        instance.Dispose()
    timer.Stop()
    print '%s' % timer.Elapsed

        
if __name__ == '__main__':
    testMemoryStreams()
    testColumnStream()
    testFileStream()    