#-----------------------------------------------------------------------
# <copyright file="esedb.py" company="Microsoft Corporation">
# Copyright (c) Microsoft Corporation.
# </copyright>
#-----------------------------------------------------------------------

# TODO
#
# Shelve support:
#   Tests
#
# use 'with' for locks
# private names: one underscore or two?
# use str() or repr()?
# Tests
#    - read-only tests
# Set read-only flag in cursor
#    - use decorator to check
# Overwriting a database (flag='n') should fail if it is already open

"""Provides a simple dictionary interface to an esent database. This requires
the ManagedEsent interop dll.

>>> x = open('wdbtest.db', flag='nf')
>>> x['a'] = 'somedata'
>>> x['a']
'somedata'
>>> x.has_key('b')
False
>>> x['b'] = 'somemoredata'
>>> x['c'] = 'deleteme'
>>> del x['c']
>>> x.keys()
['a', 'b']
>>> x.values()
['somedata', 'somemoredata']
>>> x.close()

>>> db = open('wdbtest.db', 'n')
>>> for i in range(10): db['%d'%i] = '%d'% (i*i)
...
>>> db['3']
'9'
>>> db.keys()
['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
>>> db.first()
('0', '0')
>>> db.next()
('1', '1')
>>> db.last()
('9', '81')
>>> db.set_location('2')
('2', '4')
>>> db.previous()
('1', '1')
>>> for k, v in db.iteritems():
...     print k, v
0 0
1 1
2 4
3 9
4 16
5 25
6 36
7 49
8 64
9 81
>>> '8' in db
True
>>> db.sync()
>>> db.close()

"""

from __future__ import with_statement

import thread
import System
import clr

from System import Array
from System.Globalization import CompareOptions, CultureInfo
from System.IO import File, Path, Directory
from System.Text import Encoding

clr.AddReferenceByPartialName('Esent.Interop')
from Microsoft.Isam.Esent.Interop import Api

from Microsoft.Isam.Esent.Interop import JET_INSTANCE
from Microsoft.Isam.Esent.Interop import JET_SESID
from Microsoft.Isam.Esent.Interop import JET_DBID
from Microsoft.Isam.Esent.Interop import JET_TABLEID
from Microsoft.Isam.Esent.Interop import JET_COLUMNID
from Microsoft.Isam.Esent.Interop import JET_COLUMNDEF
from Microsoft.Isam.Esent.Interop import JET_UNICODEINDEX
from Microsoft.Isam.Esent.Interop import JET_INDEXCREATE
from Microsoft.Isam.Esent.Interop import JET_CP
from Microsoft.Isam.Esent.Interop import JET_coltyp
from Microsoft.Isam.Esent.Interop import JET_param
from Microsoft.Isam.Esent.Interop import JET_prep

from Microsoft.Isam.Esent.Interop import AttachDatabaseGrbit
from Microsoft.Isam.Esent.Interop import CloseDatabaseGrbit
from Microsoft.Isam.Esent.Interop import ColumndefGrbit
from Microsoft.Isam.Esent.Interop import CommitTransactionGrbit
from Microsoft.Isam.Esent.Interop import CreateDatabaseGrbit
from Microsoft.Isam.Esent.Interop import CreateIndexGrbit
from Microsoft.Isam.Esent.Interop import EndSessionGrbit
from Microsoft.Isam.Esent.Interop import InitGrbit
from Microsoft.Isam.Esent.Interop import MakeKeyGrbit
from Microsoft.Isam.Esent.Interop import OpenDatabaseGrbit
from Microsoft.Isam.Esent.Interop import OpenTableGrbit
from Microsoft.Isam.Esent.Interop import RollbackTransactionGrbit
from Microsoft.Isam.Esent.Interop import SeekGrbit
from Microsoft.Isam.Esent.Interop import SetColumnGrbit

from Microsoft.Isam.Esent.Interop import InstanceParameters
from Microsoft.Isam.Esent.Interop import SystemParameters
from Microsoft.Isam.Esent.Interop import EsentVersion
from Microsoft.Isam.Esent.Interop import Conversions

from Microsoft.Isam.Esent.Interop.Server2003 import Server2003Grbits

from Microsoft.Isam.Esent.Interop.Vista import VistaParam

from Microsoft.Isam.Esent.Interop.Windows7 import Windows7Param
from Microsoft.Isam.Esent.Interop.Windows7 import Windows7Grbits

_unspecified = object()

#-----------------------------------------------------------------------
class _EseTransaction(object):
#-----------------------------------------------------------------------
    """Wrapper for an esent transaction. This object can be used in
    a with statement. If the 'with' block ends normally the transaction
    will be committed, otherwise it will rollback.
    
    """
    
    def __init__(self, sesid):
        self._sesid = sesid
        self._inTransaction = False
        
    def __enter__(self):
        self.begin()
        return self
        
    def __exit__(self, etyp, einst, etb):
        if self._inTransaction:
            if None == etyp:
                self.commit()
            else:
                # Abnormal exit, rollback the transaction
                self.rollback()
            
    def begin(self):
        assert not self._inTransaction, 'already in a transaction'
        Api.JetBeginTransaction(self._sesid)
        self._inTransaction = True
        self._updatesThisBatch = 0
    
    def commit(self, lazyflush=False):
        assert self._inTransaction, 'not in a transaction'
        if lazyflush:
            commitgrbit = CommitTransactionGrbit.LazyFlush
        else:
            commitgrbit = CommitTransactionGrbit.None        
        Api.JetCommitTransaction(self._sesid, commitgrbit)
        self._inTransaction = False
        
    def rollback(self):
        assert self._inTransaction, 'not in a transaction'
        Api.JetRollback(self._sesid, RollbackTransactionGrbit.None)
        self._inTransaction = False      

    def pulse(self):
        assert self._inTransaction, 'not in a transaction'
        self._updatesThisBatch = self._updatesThisBatch + 1
        if (self._updatesThisBatch == 1000):
            self.commit(lazyflush=True)
            self.begin()
        

#-----------------------------------------------------------------------
class _EseUpdate(object):
#-----------------------------------------------------------------------
    """Wrapper for an esent update. This object can be used in
    a with statement. If the 'with' block ends normally the update
    will be done, otherwise it will be cancelled.
    
    """
    
    def __init__(self, sesid, tableid, prep):
        self._sesid = sesid
        self._tableid = tableid
        self._prep = prep
        self._inUpdate = False
        
    def __enter__(self):
        self.prepareUpdate()
        return self
        
    def __exit__(self, etyp, einst, etb):
        if self._inUpdate:
            if None == etyp:
                # A normal exit, update the record
                self.update()
            else:
                # Abnormal exit, cancel the update
                self.cancelUpdate()
            
    def prepareUpdate(self):
        assert not self._inUpdate, 'already in an update'
        Api.JetPrepareUpdate(self._sesid, self._tableid, self._prep)
        self._inUpdate = True
    
    def update(self):
        assert self._inUpdate, 'not in an update'
        Api.JetUpdate(self._sesid, self._tableid, None, 0)
        self._inUpdate = False
        
    def cancelUpdate(self):
        assert self._inUpdate, 'not in an update'
        Api.JetPrepareUpdate(self._sesid, self._tableid, JET_prep.Cancel)
        self._inUpdate = False    

    
#-----------------------------------------------------------------------
class _EseDBRegistry(object):
#-----------------------------------------------------------------------
    """EseDBCursor registry. This stores a collection of _EseDB objects
    and provides a mapping from path => EseDB. To deal with multi-threaded
    creation/deletion the object needs to be locked.
    
    There should be one global instance of this class.
        
    """
    
    def __init__(self):
        self._databases = dict()
        self._critsec = thread.allocate_lock()
        self._instancenum = 0

    def lock(self):
        """Lock the object. All register, unregister and lookup operations
        must be performed the the registry object locked.
        
        """
        self._critsec.acquire()
        self.assertLocked()
        
    def unlock(self):
        """Unlock the object."""
        self.assertLocked()
        self._critsec.release()

    def newInstanceName(self):
        """Return a new esent instance name. This is guaranteed to be unique from
        all other esent instance names created by this function.
        
        """
        self.assertLocked()
        instancename = 'esedb_instance_%d' % self._instancenum
        self._instancenum += 1
        return instancename
        
    def hasDB(self, filename):    
        """Returns true if the given database is registered."""
        self.assertLocked()
        return self._databases.has_key(filename)
        
    def getDB(self, filename):
        """Gets the database object with the given path."""
        self.assertLocked()
        return self._databases[filename]
        
    def registerDB(self, esedb):
        """Registers the specified database object."""
        self.assertLocked()
        self._databases[esedb.filename] = esedb
        
    def unregisterDB(self, esedb):
        """Unregisters the specified database object."""
        self.assertLocked()
        del self._databases[esedb.filename]
        
    def assertLocked(self):
        """Assert this object is locked."""
        assert self._critsec.locked(), 'registry is not locked'


#-----------------------------------------------------------------------
class Counter(object):
#-----------------------------------------------------------------------
    """A counter that can be incremented, decremented and retrieved by
    multiple threads. It starts out as None and has to be explicitly
    initialized. Incrementing and decrementing when the count is None
    has no effect. 
    
    As this is a counter we do not expect it to become a negative number.
    
    """

    def synchronized(func):
        def locked_func(*args, **kwargs):
            args[0]._critsec.acquire()
            r = func(*args, **kwargs)
            args[0]._critsec.release()
            return r
        # Promote the documentation so doctest will work
        locked_func.__doc__ = func.__doc__
        return locked_func
    
    def __init__(self):
        self._critsec = thread.allocate_lock()
        self._n = None
        
    @synchronized
    def set(self, n):
        """Sets the counter.
        
        >>> c = Counter()
        >>> c.set(7)
        >>> c.get()    
        7
        
        """
        self._n = n
    
    @synchronized
    def get(self):
        """Gets the counter.

        >>> c = Counter()
        >>> c.set(1)
        >>> c.get()    
        1
        
        This will return None if the counter has not been set.

        >>> c = Counter()
        >>> c.get()
        
        """
        return self._n

    @synchronized
    def increment(self):
        """Increments the counter, if it has been set.

        >>> c = Counter()
        >>> c.set(1)
        >>> c.increment()
        >>> c.get()    
        2
        
        Incrementing a counter that has not been set has
        no effect.

        >>> c = Counter()
        >>> c.increment()
        >>> c.get()            
        
        """
        if None <> self._n:
            assert self._n >= 0, 'counter is negative'
            self._n += 1
            assert self._n > 0, 'counter should be positive'
    
    @synchronized
    def decrement(self):
        """Decrements the counter, if it has been set.

        >>> c = Counter()
        >>> c.set(1)
        >>> c.decrement()
        >>> c.get()    
        0
        
        Decrementing a counter that has not been set has
        no effect.

        >>> c = Counter()
        >>> c.decrement()
        >>> c.get()            
        
        """
        if None <> self._n:
            assert self._n > 0, 'trying to decrement non-positive counter'
            self._n -= 1
            assert self._n >= 0, 'counter has become negative'


#-----------------------------------------------------------------------
class _EseDB(object):
#-----------------------------------------------------------------------
    """An esedb database. A database contains one table, which has two
    columns, providing the key => value mappings. This class contains
    a JET_INSTANCE and will open/create the database.
    
    Insert/Delete/Update/Lookup functionality is provided by the 
    EseDBCursor class. One database can have many cursors and the
    database is automatically closed when the last cursor is closed.
    
    The EseDB contains a lock used to synchronize updates by 
    multiple cursors. This isn't really needed, as esent supports highly 
    concurrent access to data, but if we allow multiple threads to update the 
    database at the same time then we will face the problem of two threads 
    updating the same record at the same time, which will generate a 
    write-conflict error. Instead of dealing with those complexities we 
    simply restrict updates to one thread at a time. For read operations the
    snapshot isolation provided by esent transactions is sufficient.
    
    """
    
    def __init__(self, instancename, filename):
        self._filename = filename
        self._directory = Path.GetDirectoryName(filename)
        self._instancename = instancename
        self._datatable = 'esedb_data'
        self._keycolumn = 'key'
        self._valuecolumn = 'value'
        self._numCursors = 0
        self._critsecs = [thread.allocate_lock() for i in range(31)]
        self._instance = None    
        self._basename = 'wdb'
        self.cachedRecordCount = Counter()
        
    def openCursor(self, flag, lazyflush):
        """Creates a new cursor on the database. This function will
        initialize esent and create the database if necessary.
        
        This routine is synchronized by the global registry object.
        Cursors are opened while the registry is locked.
        
        """
        _registry.assertLocked()
        readonly = False
        create = False
        if flag == 'r':
            readonly = True
        if flag == 'c' and not File.Exists(self._filename):
            create = True
        if flag == 'n':
            self._deleteDatabaseAndLogfiles()
            create = True            
                    
        if None == self._instance:
            self._instance = self._createInstance()    
            grbit = InitGrbit.None
            if EsentVersion.SupportsWindows7Features:
                grbit = Windows7Grbits.ReplayIgnoreLostLogs
            Api.JetInit2(self._instance, grbit)
            
        if create:
            try:
                self._createDatabase()
            except:
                # Don't leave a partially created database lying around
                Api.JetTerm(self._instance)
                self._deleteDatabaseAndLogfiles()
                raise
                
        cursor = self._createCursor(readonly, lazyflush)
        return cursor
        
    def closeCursor(self, esedbCursor):
        _registry.lock()
        try:
            self._numCursors -= 1
            if 0 == self._numCursors:
                # The last cursor on the database has been closed
                # unregister this object and terminate esent
                _registry.unregisterDB(self)
                Api.JetTerm(self._instance)
                self._instance = None
        finally:
            _registry.unlock()
            
    def getWriteLock(self, hash=None):
        """
        Gets a write-lock on the database. If no hash value is specified
        then all locks are taken.

        """
        self._lock(lambda l: l.acquire(), hash)
            
    def unlock(self, hash=None):
        """
        Releases a write-lock on the database. If no hash value is specified
        then all locks are released.

        """
        self._lock(lambda l: l.release(), hash)

    def _lock(self, f, hash=None):
        """
        Applies a function to a database lock. If no hash value is specified
        the function is applied to all locks.

        """
        if None == hash:
            for l in self._critsecs:            
                f(l)
        else:
            i = hash % len(self._critsecs)
            f(self._critsecs[i])
            
    def _createInstance(self):
        """Create the JET_INSTANCE and set the system parameters. The
        important changes here are to reduce the logfile size, turn
        on circular logging and turn off the temporary database.
        
        """
        instance = Api.JetCreateInstance(self._instancename)
        parameters = InstanceParameters(instance)
        parameters.WaypointLatency = 1
        parameters.SystemDirectory = self._directory
        parameters.TempDirectory = self._directory
        parameters.LogFileDirectory = self._directory
        parameters.BaseName = self._basename
        parameters.CircularLog = True
        parameters.NoInformationEvent = True
        parameters.CreatePathIfNotExist = True
        parameters.LogFileSize = 1024
        parameters.MaxTemporaryTables = 0
                    
        return instance

    def _deleteDatabaseAndLogfiles(self):
        """Delete the database and logfiles."""
        if File.Exists(self._filename):
            File.Delete(self._filename)
        self._deleteFilesMatching(self._directory, '%s*.log' % self._basename)
        self._deleteFilesMatching(self._directory, '%s.chk' % self._basename)
            
    def _deleteFilesMatching(self, directory, pattern):
        """Delete files in the directory matching the pattern."""
        if Directory.Exists(directory):
            files = Directory.GetFiles(directory, pattern)
            for f in files:
                File.Delete(f)
        
    def _createIndex(self, sesid, tableid):
        indexdef = '+%s\0\0' % self._keycolumn

        idxUnicode = JET_UNICODEINDEX(
            lcid = CultureInfo.CurrentCulture.LCID,
            dwMapFlags = Conversions.LCMapFlagsFromCompareOptions(CompareOptions.None))

        indexcreate = JET_INDEXCREATE(
            szIndexName = 'primary',
            szKey = indexdef,
            cbKey = indexdef.Length,
            grbit = CreateIndexGrbit.IndexUnique | CreateIndexGrbit.IndexPrimary,
            cbKeyMost = SystemParameters.KeyMost,
            pidxUnicode = idxUnicode)
        indexcreates = Array[JET_INDEXCREATE]([indexcreate])

        Api.JetCreateIndex2(sesid, tableid, indexcreates, 1);            
    
    def _createDatabase(self):
        """Create the database, table and columns."""
        sesid = Api.JetBeginSession(self._instance, '', '')        
        try:
            dbid = Api.JetCreateDatabase(
                sesid,
                self._filename,
                '',
                CreateDatabaseGrbit.OverwriteExisting)
            with _EseTransaction(sesid) as trx:
                tableid = Api.JetCreateTable(
                    sesid,
                    dbid,
                    self._datatable,
                    32,
                    100)
                self._addTextColumn(sesid, tableid, self._keycolumn)
                self._addTextColumn(sesid, tableid, self._valuecolumn)
                self._createIndex(sesid, tableid)
                Api.JetCloseTable(sesid, tableid)
                trx.commit(lazyflush=True)
            Api.JetCloseDatabase(sesid, dbid, CloseDatabaseGrbit.None)
            Api.JetDetachDatabase(sesid, self._filename)
            
            # As the database is newly created we know there are no records
            self.cachedRecordCount.set(0)
        finally:
            Api.JetEndSession(sesid, EndSessionGrbit.None)

    def _addTextColumn(self, sesid, tableid, column):
        """Add a new text column to the given table."""
        grbit = ColumndefGrbit.None
        if EsentVersion.SupportsWindows7Features:
            grbit = Windows7Grbits.ColumnCompressed        
        columndef = JET_COLUMNDEF(
            cp = JET_CP.Unicode,
            coltyp = JET_coltyp.LongText,
            grbit = grbit)
        Api.JetAddColumn(
            sesid,
            tableid,
            column,
            columndef,
            None,
            0)
            
    def _createCursor(self, readonly, lazyflush):
        """Creates a new EseDBCursor."""
        sesid = Api.JetBeginSession(self._instance, '', '')
        if readonly:
            grbit = AttachDatabaseGrbit.ReadOnly
        else:
            grbit = AttachDatabaseGrbit.None
        Api.JetAttachDatabase(sesid, self._filename, grbit)
        if readonly:
            grbit = OpenDatabaseGrbit.ReadOnly
        else:
            grbit = OpenDatabaseGrbit.None
        (wrn, dbid) = Api.JetOpenDatabase(sesid, self._filename, '', grbit)
        tableid = Api.JetOpenTable(
            sesid,
            dbid,
            self._datatable,
            None,
            0,
            OpenTableGrbit.None)
        keycolumnid = self._getColumnid(sesid, tableid, self._keycolumn)
        valuecolumnid = self._getColumnid(sesid, tableid, self._valuecolumn)
        cursor = EseDBCursor(self, sesid, tableid, lazyflush, keycolumnid, valuecolumnid)
        self._numCursors += 1
        return cursor

    def _getColumnid(self, sesid, tableid, column):
        """Returns the columnid of the column."""
        columndef = clr.Reference[JET_COLUMNDEF]()
        Api.JetGetTableColumnInfo(
            sesid,
            tableid,
            column,
            columndef)
        return columndef.Value.columnid
        
    def _filename(self):
        """Returns the path of the database"""
        return self._filename
        
    filename = property(_filename, doc='path of the database')


#-----------------------------------------------------------------------
class EseDBError(Exception):
#-----------------------------------------------------------------------
    """Esedb exception"""
    
    def __init__(self, message):
        self._message = message
        
    def __repr__(self):
        return 'EseDBError(%s)' % self._message

    __str__ = __repr__

    
#-----------------------------------------------------------------------
class EseDBCursorClosedError(EseDBError):
#-----------------------------------------------------------------------
    """Raised when a method is called on a closed cursor."""
    
    def __init__(self):
        EseDBError.__init__(self, 'cursor is closed')
    
    
#-----------------------------------------------------------------------
class EseDBCursor(object):
#-----------------------------------------------------------------------
    """A cursor on an esedb database. A cursor contains a JET_SESID and
    a JET_TABLEID along with a reference to the underlying EseDB.
    
    """
        
    # Decorator that checks self (args[0]) isn't closed
    def cursorMustBeOpen(func):
        def checked_func(*args, **kwargs):
            args[0]._checkNotClosed()
            return func(*args, **kwargs)
        # Promote the documentation so doctest will work
        checked_func.__doc__ = func.__doc__
        return checked_func
        
    def __init__(self, database, sesid, tableid, lazyflush, keycolumnid, valuecolumnid):
        """Initialize a new EseDBCursor on the specified database."""
        self._database = database
        self._sesid = sesid
        self._tableid = tableid
        self._lazyflush = lazyflush
        self._keycolumnid = keycolumnid
        self._valuecolumnid = valuecolumnid
        self._isopen = True
        self._encoding = Encoding.Unicode
        
    def __del__(self):
        """Called when garbage collection is removing the object. Close it."""
        self.close()
        
    @cursorMustBeOpen
    def __getitem__(self, key): 
        """Returns the value of the record with the specified key.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['a'] = 'somedata'
        >>> x['a']
        'somedata'
        >>> x.close()

        If the key isn't present in the database then a KeyError
        is raised.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['a']
        Traceback (most recent call last):
        ...
        KeyError: key 'a' was not found
        >>> x.close()
        
        """
        with _EseTransaction(self._sesid):
            self._seekForKey(key)
            return self._retrieveCurrentRecordValue()

    @cursorMustBeOpen
    def __setitem__(self, key, value): 
        """Sets the value of the record with the specified key.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x['key'] = 'value'
        >>> x.close()                
        
        """
        key = str(key)
        self._database.getWriteLock(hash=key.GetHashCode())
        try:
            with _EseTransaction(self._sesid) as trx:
                self._insertOrUpdate(key, value)
                trx.commit(self._lazyflush)
        finally:
            self._database.unlock(hash=key.GetHashCode())
            
    @cursorMustBeOpen
    def __delitem__(self, key): 
        """Deletes the record with the specified key.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['key'] = 'value'
        >>> del x['key']
        >>> x.close()

        If the key isn't present in the database then a KeyError
        is raised.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> del x['a']
        Traceback (most recent call last):
        ...
        KeyError: key 'a' was not found
        >>> x.close()
        
        """
        key = str(key)
        self._database.getWriteLock(hash=key.GetHashCode())
        try:
            with _EseTransaction(self._sesid) as trx:
                self._seekForKey(key)
                self._deleteCurrentRecord()
                trx.commit(self._lazyflush)
        finally:
            self._database.unlock(hash=key.GetHashCode())

    @cursorMustBeOpen
    def __len__(self):
        """Returns the number of records in the database.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> len(x)
        0
        >>> x['foo'] = 'bar'
        >>> len(x)
        1
        >>> x.close()
        
        """
        # If there is no cached length we have to scan the database
        if None == self._database.cachedRecordCount.get():
            self._database.getWriteLock()
            if None == self._database.cachedRecordCount.get():
                try:
                    with _EseTransaction(self._sesid) as trx:
                        if Api.TryMoveFirst(self._sesid, self._tableid):
                            self._database.cachedRecordCount.set(Api.JetIndexRecordCount(self._sesid, self._tableid, 0))
                        else:
                            self._database.cachedRecordCount.set(0)
                finally:
                    self._database.unlock()
        return self._database.cachedRecordCount.get()
            
    @cursorMustBeOpen
    def __contains__(self, key):
        """Returns True if the database contains the specified key,
        otherwise returns False.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['foo'] = 'bar'
        >>> 'foo' in x
        True
        >>> 'baz' in x
        False
        >>> 'baz' not in x
        True
        >>> x.close()
            
        """
        return self.has_key(key)
        
    def close(self):
        """Close the database. After being closed this object can no
        longer be used

        >>> x = open('wdbtest.db', flag='nf')
        >>> x.close()                
        >>> x.has_key('a')
        Traceback (most recent call last):
        ...
        EseDBCursorClosedError: EseDBError(cursor is closed)
        
        """
        if self._isopen:
            Api.JetCloseTable(self._sesid, self._tableid)
            self._tableid = None
            Api.JetEndSession(self._sesid, EndSessionGrbit.None)
            self._sesid = None
            # Tell the database this cursor has been closed. The database is
            # refcounted and closing the last cursor will close the database.
            self._database.closeCursor(self)
            self._isopen = False
        
    @cursorMustBeOpen
    def clear(self):
        """Removes all records from the database.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['key'] = 'value'
        >>> x['anotherkey'] = 'anothervalue'
        >>> x.clear()
        >>> len(x)
        0
        >>> 'key' in x
        False
        >>> x.close()
        
        """   
        # clear() could be optimized by just deleting and
        # recreating the table
        self._database.getWriteLock()
        try:
            n = 0
            # Do deletes in batches to improve performance
            with _EseTransaction(self._sesid) as trx:
                Api.MoveBeforeFirst(self._sesid, self._tableid)
                while Api.TryMoveNext(self._sesid, self._tableid):
                    n += 1
                    if 0 == (n%100):
                        trx.commit(lazyflush=True)
                        trx.begin()                    
                    self._deleteCurrentRecord()
        finally:
            self._database.unlock()    
        
    @cursorMustBeOpen
    def iterkeys(self):
        """Returns each key contained in the database. These
        are returned in sorted order.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['c'] = 64
        >>> x['b'] = 128
        >>> x['a'] = 256
        >>> for k in x.iterkeys():
        ...        print k    
        ...        
        a
        b
        c
        >>> x.close()
        
        """
        return self._iterateAndYield(self._retrieveCurrentRecordKey)
            
    @cursorMustBeOpen
    def keys(self):
        """Returns a list of all keys in the database. The
        list is in sorted order.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['c'] = 64
        >>> x['b'] = 128
        >>> x['a'] = 256
        >>> x.keys()
        ['a', 'b', 'c']
        >>> x.close()                
    
        """
        return list(self.iterkeys())

    @cursorMustBeOpen
    def itervalues(self):
        """Returns each value contained in the database. These
        are returned in key order.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['c'] = 64
        >>> x['b'] = 128
        >>> x['a'] = 256
        >>> for k in x.itervalues():
        ...        print k    
        ...        
        256
        128
        64
        >>> x.close()
        
        """
        return self._iterateAndYield(self._retrieveCurrentRecordValue)
        
    @cursorMustBeOpen
    def values(self):
        """Returns a list of all values in the database. The
        values are returned in key order.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['c'] = 64
        >>> x['b'] = 128
        >>> x['a'] = 256
        >>> x.values()
        ['256', '128', '64']
        >>> x.close()    
        
        """
        return list(self.itervalues())
            
    @cursorMustBeOpen
    def iteritems(self):
        """Return each key/value pair contained in the database. These
        are returned in key order.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x['c'] = 64
        >>> x['b'] = 128
        >>> x['a'] = 256
        >>> for (k,v) in x.iteritems():
        ...        print '%s => %s' % (k,v)    
        ...        
        a => 256
        b => 128
        c => 64
        >>> x.close()

        """
        return self._iterateAndYield(self._retrieveCurrentRecord)
            
    __iter__ = iteritems

    @cursorMustBeOpen
    def items(self):
        """Returns a list of all items in the database as a list of
        (key, value) tuples. The items are returned in key order.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['c'] = 64
        >>> x['b'] = 128
        >>> x['a'] = 256
        >>> x.items()
        [('a', '256'), ('b', '128'), ('c', '64')]
        >>> x.close()    
                
        """
        return list(self.iteritems())
            
    @cursorMustBeOpen
    def has_key(self, key):
        """Returns True if the database contains the specified key,
        otherwise returns False.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['key'] = 'value'
        >>> x.has_key('key')
        True
        >>> x.has_key('not_a_key')
        False
        >>> x.close()
            
        """
        with _EseTransaction(self._sesid):   
            return self._has_key(key)
                
    @cursorMustBeOpen
    def set_location(self, key):
        """Sets the cursor to the record specified by the key and returns
        a pair (key, value) for the record.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x['key'] = 'value'
        >>> x.set_location('key')
        ('key', 'value')
        >>> x.close()        
        
        If the key doesn't exist in the database then the location is set
        to the next highest key and that record is returned.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x['b'] = 'value'
        >>> x.set_location('a')
        ('b', 'value')
        >>> x.close()                
        
        If no matching key is found then KeyError is raised.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['a'] = 'value'
        >>> x.set_location('b')
        Traceback (most recent call last):
        ...
        KeyError: no key matching 'b' was found
        >>> x.close()                
        
        """
        with _EseTransaction(self._sesid):        
            self._makeKey(key)
            if not Api.TrySeek(self._sesid, self._tableid, SeekGrbit.SeekGE):
                raise KeyError('no key matching \'%s\' was found' % key)
            return self._retrieveCurrentRecord()

    @cursorMustBeOpen
    def first(self):
        """Sets the cursor to the first record in the database and returns
        a (key, value) for the record.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x['b'] = 128
        >>> x['a'] = 256
        >>> x.first()
        ('a', '256')
        >>> x.close()            
        
        If the database is empty a KeyError is raised.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x.first()
        Traceback (most recent call last):
        ...
        KeyError: database is empty
        >>> x.close()            
        
        """
        with _EseTransaction(self._sesid):        
            if not Api.TryMoveFirst(self._sesid, self._tableid):
                raise KeyError('database is empty')    
            return self._retrieveCurrentRecord()
    
    @cursorMustBeOpen
    def last(self):
        """Sets the cursor to the last record in the database and returns
        a (key, value) for the record.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['c'] = 64
        >>> x['b'] = 128
        >>> x.last()
        ('c', '64')
        >>> x.close()            

        If the database is empty a KeyError is raised.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x.last()
        Traceback (most recent call last):
        ...
        KeyError: database is empty
        >>> x.close()            
        
        """
        with _EseTransaction(self._sesid):        
            if not Api.TryMoveLast(self._sesid, self._tableid):
                raise KeyError('database is empty')        
            return self._retrieveCurrentRecord()

    @cursorMustBeOpen
    def next(self):
        """Sets the cursor to the next record in the database and returns
        a (key, value) for the record.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['b'] = 128
        >>> x['a'] = 256
        >>> x.first()
        ('a', '256')
        >>> x.next()
        ('b', '128')
        >>> x.close()            

        A KeyError is raised when the end of the table is reached or if 
        the table is empty.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x.next()
        Traceback (most recent call last):
        ...
        KeyError: end of database
        >>> x.close()                    
    
        """
        with _EseTransaction(self._sesid):        
            if not Api.TryMoveNext(self._sesid, self._tableid):
                raise KeyError('end of database')        
            return self._retrieveCurrentRecord()
        
    @cursorMustBeOpen
    def previous(self):
        """Sets the cursor to the previous item in the database and returns
        a (key, value) for the record.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x['c'] = 64
        >>> x['b'] = 128
        >>> x.last()
        ('c', '64')
        >>> x.previous()
        ('b', '128')
        >>> x.close()            

        A KeyError is raised when the end of the table is reached or if 
        the table is empty.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x.previous()
        Traceback (most recent call last):
        ...
        KeyError: end of database
        >>> x.close()                    
        
        """
        with _EseTransaction(self._sesid):        
            if not Api.TryMovePrevious(self._sesid, self._tableid):
                raise KeyError('end of database')        
            return self._retrieveCurrentRecord()        

    @cursorMustBeOpen
    def firstkey(self):
        """Sets the cursor to the first record in the database and returns
        the key of the record.
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x['b'] = 128
        >>> x['a'] = 256
        >>> x.firstkey()
        'a'
        >>> x.close()            
        
        If the database is empty None is returned.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x.firstkey()
        >>> x.close()            
        
        """
        with _EseTransaction(self._sesid):        
            if not Api.TryMoveFirst(self._sesid, self._tableid):
                return None    
            return self._retrieveCurrentRecordKey()
        
    @cursorMustBeOpen
    def nextkey(self, key):
        """Returns the key that follows key in the traversal.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['b'] = 128
        >>> x['a'] = 256
        >>> x.nextkey('a')
        'b'
        >>> x.close()            

        If there are no matching records None is returned.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x.nextkey('x')
        >>> x.close()            
        
        The following code prints every key in the database, without having
        to create a list in memory that contains them all:

        >>> db = open('wdbtest.db', 'n')
        >>> for i in range(10): db['%d'%i] = '%d'% (i*i)
        ...
        >>> k = db.firstkey()
        >>> while k != None:
        ...     print k
        ...     k = db.nextkey(k)
        0
        1
        2
        3
        4
        5
        6
        7
        8
        9
        >>> db.close()
    
        """    
        with _EseTransaction(self._sesid): 
            self._makeKey(key)
            if not Api.TrySeek(self._sesid, self._tableid, SeekGrbit.SeekEQ):
                return None
            if not Api.TryMoveNext(self._sesid, self._tableid):
                return None
            return self._retrieveCurrentRecordKey()
            
    @cursorMustBeOpen
    def pop(self, key, default=_unspecified):
        """If key is in the dictionary, remove it and return its value, else
        return default. 
        
        >>> x = open('wdbtest.db', flag='nf')
        >>> x['a'] = 64
        >>> x.pop('a', 'X')
        '64'
        >>> x.pop('b', 'X')
        'X'
        >>> x.close()              
        
        If default is not given and key is not in the dictionary, a
        KeyError is raised.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['a'] = 64
        >>> x.pop('b')
        Traceback (most recent call last):
        ...
        KeyError: no key matching 'b' was found
        >>> x.close()
        
        """
        self._database.getWriteLock(hash=key.GetHashCode())
        try:
            with _EseTransaction(self._sesid) as trx:
                self._makeKey(key)
                if Api.TrySeek(self._sesid, self._tableid, SeekGrbit.SeekEQ):
                    value = self._retrieveCurrentRecordValue()
                    self._deleteCurrentRecord()
                    trx.commit(self._lazyflush)
                    return value                    
                elif default is _unspecified:
                    raise KeyError('no key matching \'%s\' was found' % key)
                else:
                    return default            
        finally:
            self._database.unlock(hash=key.GetHashCode())        

    @cursorMustBeOpen
    def popitem(self):
        """Remove and return an arbitrary (key, value) pair from the dictionary.
        popitem() is useful to destructively iterate over a dictionary, as often
        used in set algorithms.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['a'] = 64
        >>> x.popitem()
        ('a', '64')

        If the dictionary is empty, calling popitem() raises a KeyError.

        >>> x.popitem()
        Traceback (most recent call last):
        ...
        KeyError: database is empty
        >>> x.close()            
        
        """
        self._database.getWriteLock()
        try:
            with _EseTransaction(self._sesid) as trx:
                if not Api.TryMoveLast(self._sesid, self._tableid):
                    raise KeyError('database is empty')        
                value = self._retrieveCurrentRecord()            
                self._deleteCurrentRecord()
                trx.commit(self._lazyflush)
                return value
        finally:
            self._database.unlock()
            
    @cursorMustBeOpen
    def setdefault(self, key, default=None):
        """If key is in the dictionary, return its value. If not, insert key with
        a value of default and return default. Default defaults to None.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['a'] = 64
        >>> x.setdefault('a')
        '64'
        >>> x.setdefault('b', 'X')
        'X'
        >>> x.setdefault('c')
        >>> x.items()
        [('a', '64'), ('b', 'X'), ('c', None)]
        >>> x.close()            

        """
        self._database.getWriteLock(hash=key.GetHashCode())
        try:
            with _EseTransaction(self._sesid) as trx:
                self._makeKey(key)
                if Api.TrySeek(self._sesid, self._tableid, SeekGrbit.SeekEQ):
                    return self._retrieveCurrentRecordValue()
                else:
                    self._insertItem(key, default)
                    trx.commit(self._lazyflush)
                    return default
        finally:
            self._database.unlock(hash=key.GetHashCode())        

    @cursorMustBeOpen
    def update(self, other=None, **keywords):
        """Updates the dictionary with the key/value pairs from other,
        overwriting existing keys. update() accepts either a dictionary,
        and iterable of key/value pairs or as a set of keyword arguments

        >>> x = open('wdbtest.db', flag='nf')
        >>> x.update(foo=1, bar=2)
        >>> x.items()
        [('bar', '2'), ('foo', '1')]
        >>> d = { 'baz': '3' }
        >>> x.update(d)
        >>> x.items()
        [('bar', '2'), ('baz', '3'), ('foo', '1')]
        >>> i = [ ('qux', '4') ]
        >>> x.update(i)
        >>> x.items()
        [('bar', '2'), ('baz', '3'), ('foo', '1'), ('qux', '4')]
        >>> x.close()
            
        """
        # Lock all keys and use big transactions
        self._database.getWriteLock()
        try:
            with _EseTransaction(self._sesid) as trx:
                if isinstance(other, dict):
                    self._updateItems(other.iteritems(), trx)
                elif other:
                    self._updateItems(other, trx)
                if keywords:
                    self._updateItems(keywords.items(), trx)
                trx.commit(self._lazyflush)
        finally:
            self._database.unlock()     
            
    @cursorMustBeOpen
    def sync(self):
        """Forces any unwritten data to be written to disk. This method
        has no effect when running on Windows XP.

        >>> x = open('wdbtest.db', flag='nf')
        >>> x['a'] = 64
        >>> x.sync()
        >>> x.close()

        """  
        if EsentVersion.SupportsServer2003Features:
            Api.JetCommitTransaction(self._sesid, Server2003Grbits.WaitAllLevel0Commit)
            
    def _checkNotClosed(self):
        """Throw an exception if the cursor has been closed."""
        if not self._isopen:
            raise EseDBCursorClosedError()

    def _iterateAndYield(self, f):
        """Iterate over all the records and yield the result
        of calling f() each time.
        
        The iteration is done inside of a transaction, but
        the yield happens outside of the transaction. This 
        is OK because it is always possible to move off a
        deleted record (if we fall off the end of the table
        then the iteration terminates).
        
        """
        with _EseTransaction(self._sesid) as trx:
            Api.MoveBeforeFirst(self._sesid, self._tableid)
            while Api.TryMoveNext(self._sesid, self._tableid):
                value = f()
                trx.commit()
                yield value
                self._checkNotClosed()
                trx.begin()

    @cursorMustBeOpen
    def _has_key(self, key):
        """Returns True if the database contains the specified key,
        otherwise returns False. The cursor should already be in a
        transaction.
            
        """
        self._makeKey(key)
        return Api.TrySeek(self._sesid, self._tableid, SeekGrbit.SeekEQ)
                
    def _updateItems(self, items, trx):
        """Insert or update the given key/value tuples. A transaction must
        be provided and will be pulsed to prevent VSOOM problems. The cursor
        should already be in a transaction.
        
        """
        for (k,v) in items:
            self._insertOrUpdate(k, v)
            trx.pulse()        
                
    def _insertOrUpdate(self, key, value):
        """Inserts the given key/value if the key doesn't exist. Updates the
        given key with the specified value if the key does exist. The cursor
        should already be in a transaction.
        
        """
        if self._has_key(key):
            self._updateItem(key, value)
        else:
            self._insertItem(key, value)                    
        
    def _updateItem(self, key, value):
        """Update the given key with the specified value. The key must
        exist, the cursor should already be in a transaction and the
        cursor must be positioned on the record.
        
        """
        with _EseUpdate(self._sesid, self._tableid, JET_prep.Replace) as u:
            self._setValueColumn(value)
            u.update()

    def _insertItem(self, key, value):
        """Update the given key with the specified value. The key must
        not exist and the cursor should already be in a transaction.
        
        """
        with _EseUpdate(self._sesid, self._tableid, JET_prep.Insert) as u:
            self._setKeyColumn(key)
            self._setValueColumn(value)
            u.update()
            self._database.cachedRecordCount.increment()

    def _deleteCurrentRecord(self):
        Api.JetDelete(self._sesid, self._tableid)
        self._database.cachedRecordCount.decrement()    
            
    def _retrieveCurrentRecord(self):
        """Returns a tuple of (key, value) for the current record."""
        return (self._retrieveCurrentRecordKey(), self._retrieveCurrentRecordValue())
        
    def _retrieveCurrentRecordKey(self):
        """Gets the key of the current record."""
        return Api.RetrieveColumnAsString(self._sesid, self._tableid, self._keycolumnid)

    def _retrieveCurrentRecordValue(self):
        """Gets the value of the current record."""
        return Api.RetrieveColumnAsString(self._sesid, self._tableid, self._valuecolumnid)
        
    def _setKeyColumn(self, key):
        """Sets the key column. An update should be prepared."""
        Api.SetColumn(self._sesid, self._tableid, self._keycolumnid, str(key), self._encoding)

    def _setValueColumn(self, value):
        """Sets the value column. An update should be prepared."""
        # Here we want to store None as a null column, instead of the string 'None'
        # This is different than the key column, which we store a 'None' (to avoid 
        # null keys in the database).
        if None == value:
            data = None
        else:
            data = str(value)        
        Api.SetColumn(self._sesid, self._tableid, self._valuecolumnid, data, self._encoding, SetColumnGrbit.IntrinsicLV)
                
    def _makeKey(self, key):
        """Construct a key for the given value."""
        Api.MakeKey(self._sesid, self._tableid, str(key), self._encoding, MakeKeyGrbit.NewKey)

    def _seekForKey(self, key):
        """Seek for the specified key. A KeyError exception is raised if the
        key isn't found.
        
        """
        self._makeKey(key)
        if not Api.TrySeek(self._sesid, self._tableid, SeekGrbit.SeekEQ):
            raise KeyError('key \'%s\' was not found' % key)

    
#-----------------------------------------------------------------------
def open(filename, flag='cf', mode=0):
#-----------------------------------------------------------------------
    """Open an esent database and return an EseDBCursor object. Filename is
    the path to the database, including the extension. Flag specifies
    the how to open the file: 'r' opens the database read-only,
    'w' opens the database read-write, 'c' opens the database read-write,
    creating it if necessary, and 'n' always creates a new, empty database.
    
    Either 'f' or 's' may be appended to the flag to control how updates are
    written to the database: 'f' will open the database in fast flag, writes
    will not be synchronized; 's' will open the database in synchronized flag,
    changes to the database will immediately be flushed to disk.
    
    The default flag is 'cf'
    
    The mode argument is ignored.
    
    As well as the database file, this will create transaction logs and
    a checkpoint file in the same directory as the database (if read/write
    access is requested). The logs and checkpoint will start with a prefix
    of 'wdb'.
    
    If lazyflush is true, then the transaction logs will be written in
    a lazy fashion. This will preserve database consistency, but some data
    will be lost if there is an unexpected shutdown (crash).

    >>> db = open('wdbtest.db', 'n')
    >>> for i in range(10): db['%d'%i] = '%d'% (i*i)
    ...
    >>> db['3']
    '9'
    >>> db.keys()
    ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    >>> db.first()
    ('0', '0')
    >>> db.next()
    ('1', '1')
    >>> db.last()
    ('9', '81')
    >>> db.set_location('2')
    ('2', '4')
    >>> db.previous()
    ('1', '1')
    >>> for k, v in db.iteritems():
    ...     print k, v
    0 0
    1 1
    2 4
    3 9
    4 16
    5 25
    6 36
    7 49
    8 64
    9 81
    >>> '8' in db
    True
    >>> db.close()
    
    """
    filename = Path.GetFullPath(filename)
    
    lazyflush = True
    if 1 == len(flag) or 2 == len(flag):
        if not flag[0] in 'rwcn':
            raise EseDBError('invalid flag')
        mode = flag[0]
        if 2 == len(flag):
            if not flag[1] in 'sf':
                raise EseDBError('invalid flag')            
            lazyflush = flag[1] == 'f'
    else:
        raise EseDBError('invalid flag')
    
    _registry.lock()
    try:
        if not _registry.hasDB(filename):
            instancename = _registry.newInstanceName()
            newDB = _EseDB(instancename, filename)
            _registry.registerDB(newDB)
        db = _registry.getDB(filename)
        return db.openCursor(mode, lazyflush)                
    finally:
        _registry.unlock()            

    
# Set global esent options
SystemParameters.Configuration = 1
SystemParameters.EnableAdvanced = True
SystemParameters.DatabasePageSize = 8192
SystemParameters.CacheSizeMin = 8192
SystemParameters.CacheSizeMax = 2**30

# A global object to perform filename => EseDB mappings
_registry = _EseDBRegistry()

if __name__ == '__main__':
    import doctest
    doctest.testmod()
    