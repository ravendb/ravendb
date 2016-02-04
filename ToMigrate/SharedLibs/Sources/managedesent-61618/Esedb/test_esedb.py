#-----------------------------------------------------------------------
# <copyright file="test_esedb.py" company="Microsoft Corporation">
# Copyright (c) Microsoft Corporation.
# </copyright>
#-----------------------------------------------------------------------

import unittest
import random
import threading
import esedb
import System

from System.IO import Directory
from System.IO import Path
from esedb import Counter, EseDBError, EseDBCursorClosedError

import clr
clr.AddReferenceByPartialName('Esent.Interop')
import Microsoft.Isam.Esent.Interop as Esent

def deleteDirectory(directory):
    if Directory.Exists(directory):
        Directory.Delete(directory, True)

class EsedbSingleDBFixture(unittest.TestCase):
    """Basics tests for esedb. This fixture creates an empty database and tests
    individual operations against it.

    """

    def setUp(self):
        self._dataDirectory = 'unittest_data'
        self._deleteDataDirectory()
        self._db = esedb.open(self._makeDatabasePath('test.edb'))

    def tearDown(self):
        self._db.close()
        self._deleteDataDirectory()

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def _makeDatabasePath(self, filename):
        return Path.Combine(self._dataDirectory, filename)
        
    def testInsertAndRetrieveRecord(self):
        self._db['key'] = 'value'
        self.assertEqual(self._db['key'], 'value')

    def testLargeKey(self):
        # esent will truncate the key, but we should be able to set all this data
        key = 'K' * 1024*1024
        self._db[key] = 'value'
        self.assertEqual(self._db[key], 'value')

    def testLargeValue(self):
        value = 'V' * 1024*1024
        self._db['bigstuff'] = value
        self.assertEqual(self._db['bigstuff'], value)

    def testLongKeys(self):
        if Esent.EsentVersion.SupportsLargeKeys:
            key1 = '?'*300 + 'Foo'
            key2 = '?'*300 + 'Bar'
            self._db[key1] = 'value1'
            self._db[key2] = 'value2'
            self.assertEqual(self._db[key1], 'value1')
            self.assertEqual(self._db[key2], 'value2')

    def testNullKey(self):
        self._db[None] = 'value'
        self.assertEqual(self._db[None], 'value')

    def testNullValue(self):
        self._db['key'] = None
        self.assertEqual(self._db['key'], None)

    def testOverwriteRecord(self):
        self._db['key'] = 'value'
        self._db['key'] = 'newvalue'
        self.assertEqual(self._db['key'], 'newvalue')

    def testHasKeyReturnsFalseWhenKeyNotPresent(self):
        self.assertEqual(False, self._db.has_key('key'))

    def testHasKeyReturnsTrueWhenKeyIsPresent(self):
        self._db['key'] = 'value'
        self.assertEqual(True, self._db.has_key('key'))

    def testInReturnsFalseWhenKeyNotPresent(self):
        self.assertEqual(False, ('key' in self._db))

    def testInReturnsTrueWhenKeyIsPresent(self):
        self._db['key'] = 'value'
        self.assertEqual(True, ('key' in self._db))

    def testRetrieveRaisesKeyErrorWhenKeyNotPresent(self):
        self.assertRaises(KeyError, self._db.__getitem__, 'key')

    def testDeleteRemovesKey(self):
        self._db['key'] = 'value'
        del self._db['key']
        self.assertEqual(False, self._db.has_key('key'))

    def testDeleteRaisesKeyErrorWhenKeyNotPresent(self):
        self.assertRaises(KeyError, self._db.__delitem__, 'key')

    def testSetLocationFindsExactMatch(self):
        self._db['key'] = 'value'
        self.assertEqual(('key', 'value'), self._db.set_location('key'))

    def testSetLocationFindsNextHighest(self):
        self._db['key'] = 'value'
        self.assertEqual(('key', 'value'), self._db.set_location('k'))

    def testSetLocationRaisesKeyErrorIfNoMatch(self):
        self._db['key'] = 'value'
        self.assertRaises(KeyError, self._db.set_location, 'x')

    def testFirstRaisesKeyErrorIfEmpty(self):
        self.assertRaises(KeyError, self._db.first)

    def testLastRaisesKeyErrorIfEmpty(self):
        self.assertRaises(KeyError, self._db.last)

    def testNextRaisesKeyErrorIfEmpty(self):
        self.assertRaises(KeyError, self._db.next)

    def testPreviousRaisesKeyErrorIfEmpty(self):
        self.assertRaises(KeyError, self._db.previous)

    def testFirstKeyReturnsNoneIfEmpty(self):
        self.assertEqual(None, self._db.firstkey())

    def testNextKeyReturnsNoneIfEmpty(self):
        self.assertEqual(None, self._db.nextkey('x'))
        
    def testKeysReturnsEmptyListIfEmpty(self):
        self.assertEqual([], self._db.keys())

    def testValuesReturnsEmptyListIfEmpty(self):
        self.assertEqual([], self._db.values())

    def testItemsReturnsEmptyListIfEmpty(self):
        self.assertEqual([], self._db.items())

    def testLenIsZeroWhenDatabaseIsEmpty(self):
        self.assertEqual(0, len(self._db))

    def testLenIncreasesWithInsert(self):
        self._db['a'] = 'a'
        self._db['b'] = 'b'
        self.assertEqual(2, len(self._db))

    def testLenDecreasesWithDelete(self):
        self._db['a'] = 'a'
        self._db['b'] = 'b'
        self._db['c'] = 'c'
        del self._db['b']
        self.assertEqual(2, len(self._db))

    def testClearOnEmptyDatabase(self):
        self._db.clear()
        self.assertEqual(0, len(self._db))

    def testClearRemovesRecords(self):
        self._db['b'] = 'b'
        self._db['a'] = 'a'
        self._db.clear()
        self.assertEqual(0, len(self._db))

    def testPopRaisesKeyErrorIfNotFound(self):
        self.assertRaises(KeyError, self._db.pop, 'a')

    def testPopReturnsDefaultIfNotFound(self):
        self.assertEqual('X', self._db.pop('a', 'X'))

    def testPopReturnsItem(self):
        self._db['a'] = 'a'
        self.assertEqual('a', self._db.pop('a'))

    def testPopRemovesItem(self):
        self._db['a'] = 'a'
        self._db.pop('a')
        self.assertEqual(False, self._db.has_key('a'))        

    def testPopItemRaisesKeyErrorIfEmpty(self):
        self.assertRaises(KeyError, self._db.popitem)

    def testPopItemReturnsItem(self):
        self._db['a'] = 'a'
        self.assertEqual(('a','a'), self._db.popitem())

    def testPopItemRemovesItem(self):
        self._db['a'] = 'a'
        self._db.popitem()
        self.assertEqual(False, self._db.has_key('a'))        

    def testSetDefaultCreatesEntry(self):
        self.assertEqual(self._db.setdefault('a', 'foo'), 'foo')
        self.assertEqual(self._db['a'], 'foo')

    def testSetDefaultReturnsValue(self):
        self._db['a'] = 0
        self.assertEqual(self._db.setdefault('a', 'foo'), '0')
        self.assertEqual(self._db['a'], '0')

    def testSetDefaultUsesNone(self):
        self.assertEqual(self._db.setdefault('a', None), None)
        self.assertEqual(self._db['a'], None)
        
    def testUpdateWithDictionary(self):
        d = { 'a': 'b', 'c': 4 }
        self._db.update(d)
        self.assertEqual(self._db['a'], 'b')
        self.assertEqual(self._db['c'], '4')

    def testUpdateWithIterable(self):
        i = [ ('a', 'b'), ('c', 4) ]
        self._db.update(i)
        self.assertEqual(self._db['a'], 'b')
        self.assertEqual(self._db['c'], '4')

    def testUpdateWithKeywords(self):
        self._db.update(foo=5, bar='a')
        self.assertEqual(self._db['foo'], '5')
        self.assertEqual(self._db['bar'], 'a')
        
    def testSync(self):
        self._db.sync()
        self._db['foo'] = 'bar'
        self._db.sync()
        
class EsedbIterationFixture(unittest.TestCase):
    """Iteration tests for esedb. This fixture creates a database with a
    fixed set of records and iterates over them.

    """

    def setUp(self):
        self._dataDirectory = 'unittest_data'
        self._deleteDataDirectory()
        self._db = esedb.open(self._makeDatabasePath('test.edb'))
        self._db['d'] = '4'
        self._db['a'] = '1'
        self._db['c'] = '3'
        self._db['b'] = '2'

    def tearDown(self):
        self._db.close()
        self._deleteDataDirectory()

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def _makeDatabasePath(self, filename):
        return Path.Combine(self._dataDirectory, filename)

    def testFirstReturnsFirstRecord(self):
        self.assertEqual(('a', '1'), self._db.first())

    def testLastReturnsLastRecord(self):
        self.assertEqual(('d', '4'), self._db.last())

    def testNextReturnsNextRecord(self):
        self._db.first()
        self.assertEqual(('b', '2'), self._db.next())

    def testNextRaisesKeyErrorOnLastRecord(self):
        self._db.last()
        self.assertRaises(KeyError, self._db.next)

    def testPreviousReturnsPreviousRecord(self):
        self._db.last()
        self.assertEqual(('c', '3'), self._db.previous())

    def testPreviousRaisesKeyErrorOnFirstRecord(self):
        self._db.first()
        self.assertRaises(KeyError, self._db.previous)

    def testFirstKeyReturnsFirstKey(self):
        self.assertEqual('a', self._db.firstkey())

    def testNextKeyReturnsNextKey(self):
        self.assertEqual('b', self._db.nextkey('a'))

    def testNextKeyReturnsNoneOnLastKey(self):
        self.assertEqual(None, self._db.nextkey('d'))

    def testNextKeyReturnsNoneOnNonMatchingKey(self):
        self.assertEqual(None, self._db.nextkey('e'))
        
    def testIterkeysReturnsKeys(self):
        self.assertEqual(['a', 'b', 'c', 'd'], list(self._db.iterkeys()))

    def testKeysReturnsKeys(self):
        self.assertEqual(['a', 'b', 'c', 'd'], self._db.keys())

    def testItervaluesReturnsValues(self):
        self.assertEqual(['1', '2', '3', '4'], list(self._db.itervalues()))

    def testValuesReturnsValues(self):
        self.assertEqual(['1', '2', '3', '4'], self._db.values())

    def testIteritemsReturnsItems(self):
        self.assertEqual(
            [('a', '1'), ('b', '2'), ('c', '3'), ('d', '4')],
            list(self._db.iteritems()))

    def testItemsReturnsItems(self):
        self.assertEqual(
            [('a', '1'), ('b', '2'), ('c', '3'), ('d', '4')],
            self._db.items())

    def testLenIncludesAllValues(self):
        self.assertEqual(len(self._db.keys()), len(self._db))

    def testIterateFirstToEnd(self):
        items = [self._db.first()]
        while True:
            try:
                items.append(self._db.next())
            except KeyError:
                break
        self.assertEqual(items, self._db.items())

    def testIterateLastToStart(self):
        items = [self._db.last()]
        while True:
            try:
                items.append(self._db.previous())
            except KeyError:
                break
        items.reverse()
        self.assertEqual(items, self._db.items())

    def testPopAllItems(self):
        expected = self._db.items()
        items = [self._db.popitem()]
        while True:
            try:
                items.append(self._db.popitem())
            except KeyError:
                break
        items.reverse()
        self.assertEqual(items, expected)

    def testIterateAllKeys(self):
        keys = []
        k = self._db.firstkey()
        while None != k:
            keys.append(k)
            k = self._db.nextkey(k)
        self.assertEqual(keys, self._db.keys())
        
class EsedbFixture(unittest.TestCase):
    """Tests for esedb."""

    def setUp(self):
        self._dataDirectory = 'unittest_data'
        self._deleteDataDirectory()

    def tearDown(self):
        self._deleteDataDirectory()

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def _makeDatabasePath(self, filename):
        return Path.Combine(self._dataDirectory, filename)

    def testInvalidFlagRaisesException(self):
        self.assertRaises(EseDBError, esedb.open, 'foo.edb', 'x')

    def testTooShortFlagRaisesException(self):
        self.assertRaises(EseDBError, esedb.open, 'foo.edb', flag='')
        
    def testTooLongFlagRaisesException(self):
        self.assertRaises(EseDBError, esedb.open, 'foo.edb', 'wfx')

    def testInvalidFlushFlagRaisesException(self):
        self.assertRaises(EseDBError, esedb.open, 'foo.edb', 'wx')
        
    def testCloseTwice(self):
        db = esedb.open(self._makeDatabasePath('test.edb'))
        db.close()
        db.close()

    def testMultipleCursorsInsertAndDelete(self):
        db1 = esedb.open(self._makeDatabasePath('test.edb'), 'n')
        db2 = esedb.open(self._makeDatabasePath('test.edb'), 'cs')
        db3 = esedb.open(self._makeDatabasePath('test.edb'), 'wf')
        db_ro = esedb.open(self._makeDatabasePath('test.edb'), 'r')

        db1['hello'] = 'world'

        self.assertEqual('world', db1['hello'])
        self.assertEqual('world', db2['hello'])
        self.assertEqual('world', db3['hello'])
        self.assertEqual('world', db_ro['hello'])

        del db3['hello']

        self.assertEqual(False, db1.has_key('hello'))
        self.assertEqual(False, db2.has_key('hello'))
        self.assertEqual(False, db3.has_key('hello'))
        self.assertEqual(False, db_ro.has_key('hello'))

        db1.close()
        db2.close()
        db3.close()
        db_ro.close()

    def testMultipleCursors(self):
        db1 = esedb.open(self._makeDatabasePath('test.edb'), 'ns')
        db2 = esedb.open(self._makeDatabasePath('test.edb'), 'cf')
        db3 = esedb.open(self._makeDatabasePath('test.edb'), 'w')
        db_ro = esedb.open(self._makeDatabasePath('test.edb'), 'r')

        db1['foo'] = 123
        db2['bar'] = 456
        db3['baz'] = 789
        db1.close()
        db3.close()

        self.assertEqual(['bar', 'baz', 'foo'], db2.keys())
        self.assertEqual(['456', '789', '123'], db_ro.values())
        db2['foo'] = 'xyzzy'
        db2.close()
        self.assertEqual('xyzzy', db_ro['foo'])
        self.assertEqual(3, len(db_ro))
        db_ro.close()

    def testMultipleDatabases(self):
        db1 = esedb.open(self._makeDatabasePath('db1\\test1.edb'), 'n')
        db2 = esedb.open(self._makeDatabasePath('db2\\test2.edb'), 'c')

        db1['hello'] = 'world'
        db2['hello'] = 'there'

        self.assertEqual('world', db1['hello'])
        self.assertEqual('there', db2['hello'])

        db1.close()
        db2.close()

    def testCloseAndReopenWithCreate(self):
        db = esedb.open(self._makeDatabasePath('test.edb'), 'n')
        db['jet blue'] = 'ese'
        db.close()

        db = esedb.open(self._makeDatabasePath('test.edb'), 'c')
        self.assertEqual('ese', db['jet blue'])
        db.close()

    def testCloseAndReopenForWrite(self):
        db = esedb.open(self._makeDatabasePath('test.edb'), 'n')
        db['jet blue'] = 'ese'
        db['ese'] = 'jet blue'
        db.close()

        db = esedb.open(self._makeDatabasePath('test.edb'), 'w')
        self.assertEqual('ese', db['jet blue'])
        self.assertEqual(2, len(db))
        db.close()

    def testCloseAndReopenReadOnly(self):
        db = esedb.open(self._makeDatabasePath('test.edb'), 'n')
        db['jet blue'] = 'ese'
        db.close()

        db = esedb.open(self._makeDatabasePath('test.edb'), 'r')
        self.assertEqual('ese', db['jet blue'])
        self.assertEqual(1, len(db))
        db.close()

    def testCloseAndOverwrite(self):
        db = esedb.open(self._makeDatabasePath('test.edb'), 'n')
        db['stuff'] = 'xxxxxx'
        db.close()

        db = esedb.open(self._makeDatabasePath('test.edb'), 'n')
        self.assertEqual(False, db.has_key('stuff'))
        db.close()

    def testCloseAndReopenEmptyDatabase(self):
        db = esedb.open(self._makeDatabasePath('test.edb'), 'n')
        db.close()

        db = esedb.open(self._makeDatabasePath('test.edb'), 'r')
        self.assertEqual(0, len(db))
        db.close()


class EsedbClosedCursorFixture(unittest.TestCase):
    """Tests for esedb on a closed cursor."""

    def setUp(self):
        self._dataDirectory = 'unittest_data'
        self._deleteDataDirectory()
        self._db = esedb.open(self._makeDatabasePath('test.edb'))
        self._db.close()
        self._deleteDataDirectory()

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def _makeDatabasePath(self, filename):
        return Path.Combine(self._dataDirectory, filename)

    def testGetitemRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.__getitem__, '_')

    def testSetitemRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.__setitem__, '_', '_')

    def testDelitemRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.__delitem__, '_')

    def testLenRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, len, self._db)

    def testContainsRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.__contains__, '_')

    def testClearRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.clear)

    def testIterkeysRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.iterkeys)

    def testKeysRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.keys)

    def testItervaluesRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.itervalues)

    def testValuesRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.values)

    def testIteritemsRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.iteritems)

    def testItemsRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.items)

    def testHaskeyRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.has_key, '_')

    def testSetlocationRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.set_location, '_')

    def testFirstRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.first)

    def testLastRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.last)

    def testNextRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.next)
        
    def testPreviousRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.previous)

    def testFirstKeyRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.firstkey)

    def testNextKeyRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.nextkey)
        
    def testPopRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.pop, 'a')

    def testPopItemRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.popitem)

    def testSetDefaultRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.setdefault)

    def testUpdateRaisesErrorOnClosedCursor(self):
        self.assertRaises(EseDBCursorClosedError, self._db.update)
        
class EsedbDictionaryComparisonFixture(unittest.TestCase):
    """Test esedb against an in-memory dictionary, starting with an empty dictionary.

    """
    def setUp(self):
        self._dataDirectory = 'unittest_data'
        self._deleteDataDirectory()
        self._openDatabase()
        self._expected = {}

    def tearDown(self):
        self._closeDatabase()
        self._deleteDataDirectory()

    def _openDatabase(self):
        self._db = esedb.open(self._makeDatabasePath('test.edb'))

    def _closeDatabase(self):
        self._db.close()

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def _makeDatabasePath(self, filename):
        return Path.Combine(self._dataDirectory, filename)

    def _compareWithExpected(self):
        self.assertEqual(len(self._expected), len(self._db))
        self.assertEqual(self._expected.keys().sort(), self._db.keys().sort())
        for k in self._expected.keys():
            self.assertEqual(self._expected[k], self._db[k])

    def _insert(self, k, v):
        self._expected[k] = v
        self._db[k] = v

    def _delete(self, k):
        del self._expected[k]
        del self._db[k]

    def _pop(self, k):
        a = self._expected.pop(k)
        b = self._db.pop(k)
        self.assertEqual(a, b)

    def _setdefault(self, k, v):
        a = self._expected.setdefault(k,v)
        b = self._db.setdefault(k,v)
        self.assertEqual(a, b)

    def _clear(self):
        self._expected.clear()
        self._db.clear()

    def _insertSomeItems(self):
        self._insert('0', 'the beginning')
        self._insert('1', 'foo')
        self._insert('2', 'bar')
        self._insert('3', 'baz')
        self._insert('4', 'qux')
        self._insert('5', 'the end')

    def testEmptyDb(self):
        self._compareWithExpected()

    def testClear(self):
        for i in xrange(256):
            self._insert(str(i), repr(i))
        self._compareWithExpected()
        self._clear()
        self._compareWithExpected()

    def testInserts(self):
        self._insert('a', '1234')
        self._insert('z', '0xF00D')
        self._insert('mmmmmm', 'donuts')
        self._insert('IronPython', 'rocks')
        self._compareWithExpected()

    def testReplaceDelete(self):
        self._insert('0', '')
        self._insert('1', '1111111111')
        self._insert('2', '222222222')
        self._insert('3', '33333333')
        self._insert('4', '4444444')
        self._insert('5', '555555')
        self._insert('5', '555555')
        self._insert('5', 'foo')
        self._insert('2', 'bar')
        self._delete('4')
        self._compareWithExpected()

    def testPop(self):
        self._insert('a', '1234')
        self._insert('z', '0xF00D')
        self._pop('a')
        self._compareWithExpected()

    def testSetDefault(self):
        self._insert('a', '1234')
        self._setdefault('a', 'x')
        self._setdefault('b', 'x')
        self._compareWithExpected()
        
    def testCloseAndOpen(self):
        for i in xrange(16):
            self._insert(str(i), '?' * i)
        self._compareWithExpected()
        self._closeDatabase()
        self._openDatabase()
        self._compareWithExpected()

    def testKeyIsCaseInsensitive(self):
        self._insert('aaa', 'foo')
        self._insert('aAa', 'bar')
        self._compareWithExpected()

    def testKeyRespectsSpaces(self):
        self._insert(' x', 'foo')
        self._insert('x', 'bar')
        self._insert('x ', 'baz')
        self._compareWithExpected()

    def testKeyRespectsSymbols(self):
        self._insert('QQQ.', 'foo')
        self._insert('QQQ', 'bar')
        self._insert('-QQQ', 'baz')
        self._compareWithExpected()

    def testRandomOperations(self):
        keys = 'abcdefghijklmompqrstuvwzyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        for i in xrange(12000):
            k = random.choice(keys) * random.randint(1,2)
            if random.random() < 0.005:
                self._closeDatabase()
                self._openDatabase()
            elif random.random() < 0.01:
                self._clear()
            elif random.random() < 0.20:
                if k in self._expected:
                    self._delete(k)
                else:
                    self._compareWithExpected()
            else:
                v = random.choice('XYZ#@$%*.') * random.randint(0,1024)
                self._insert(k,v)
        self._compareWithExpected()

    def testModifyingValuesDuringIterkeys(self):
        self._insertSomeItems()
        for k in self._db.iterkeys():
            self._insert(k, 'updated' + k)
        self._compareWithExpected()

    def testDeletingItemsDuringIterkeys(self):
        self._insertSomeItems()
        for k in self._db.iterkeys():
            self._delete(k)
        self._compareWithExpected()

    def testModifyingValuesDuringIteritems(self):
        self._insertSomeItems()
        for k,v in self._db.iteritems():
            self._insert(k, 'updated' + v)
        self._compareWithExpected()

    def testDeletingItemsDuringIteritems(self):
        self._insertSomeItems()
        for k,v in self._db.iteritems():
            self._delete(k)
        self._compareWithExpected()
        
    def testUpdateFromDictionary(self):
        self._expected['this'] = '2'
        self._expected['is'] = '3'
        self._expected['a'] = '5'
        self._expected['test'] = '7'
        self._db.update(self._expected)
        self._compareWithExpected()

    def testUpdateFromIterable(self):
        i = [('this', '2'), ('is', '3'), ('a', '5'), ('test', '7')]
        self._expected.update(i)
        self._db.update(i)
        self._compareWithExpected()

    def testUpdateFromKeywords(self):
        self._expected.update(this='2', was='3', a='5', test='7')
        self._db.update(this='2', was='3', a='5', test='7')
        self._compareWithExpected()

    def testUpdateFromDictionaryAndKeywords(self):
        d = { 'foo': 'bar' }
        self._expected.update(d, this='2', was='3', a='5', test='7')
        self._db.update(d, this='2', was='3', a='5', test='7')
        self._compareWithExpected()        

    def testBigUpdate(self):
        items = [(str(i),str(i)) for i in xrange(10000)]
        self._expected.update(items)
        self._db.update(items)
        self._compareWithExpected()
        
class CounterTests(unittest.TestCase):
    """Test the counter class"""

    def testInitSetsValueToNone(self):
        c = Counter()
        self.assertEqual(None, c.get())

    def testIncrementNoneValue(self):
        c = Counter()
        c.increment()
        self.assertEqual(None, c.get())

    def testDecrementNoneValue(self):
        c = Counter()
        c.decrement()
        self.assertEqual(None, c.get())

    def testSetValue(self):
        c = Counter()
        c.set(1)
        self.assertEqual(1, c.get())

    def testIncrementValue(self):
        c = Counter()
        c.set(1)
        c.increment()
        self.assertEqual(2, c.get())

    def testDecrementValue(self):
        c = Counter()
        c.set(1)
        c.decrement()
        self.assertEqual(0, c.get())

    def testMultiThreadedCounterUpdates(self):
        n = 250000 # perform this many increment/decrement/get operations per thread
        c = Counter()
        value = 1000000
        c.set(value)
        self.assertEqual(value, c.get())
        threads = []
        for i in range(8):
            ops = ['+' for x in range(n)] + ['-' for x in range(n)] + ['R' for x in range(n)]
            random.shuffle(ops)
            threads.append(threading.Thread(target = self._updateThread, args = (c,ops)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(value, c.get())

    def _updateThread(self, c, ops):
        for o in ops:
            if '+' == o:
                c.increment()
            elif '-' == o:
                c.decrement()
            else:
                _ = c.get()


class EsedbMultiThreadingFixture(unittest.TestCase):
    """Update a database with multiple threads."""

    def setUp(self):
        self._dataDirectory = 'unittest_data'
        self._deleteDataDirectory()
        self._database = self._makeDatabasePath('test.edb')
        self._db = esedb.open(self._database)

    def tearDown(self):
        self._db.close()
        self._deleteDataDirectory()

    def _makeDatabasePath(self, filename):
        return Path.Combine(self._dataDirectory, filename)

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def _insertRange(self, low, high):
        db = esedb.open(self._database)
        for i in xrange(low, high):
            db[i] = i
        db.close()

    def _setdefaultRange(self, low, high):
        db = esedb.open(self._database)
        for i in xrange(low, high):
            db.setdefault(i, i)
        db.close()
        
    def _deleteRange(self, low, high):
        db = esedb.open(self._database)
        for i in xrange(low, high):
            del db[i]
        db.close()

    def _popRange(self, low, high):
        db = esedb.open(self._database)
        for i in xrange(low, high):
            db.pop(i)
        db.close()

    def _popAllItems(self):
        db = esedb.open(self._database)
        try:
            while True:
                db.popitem()
        except KeyError:
            pass
        db.close()
        
    def _retrieveAllRecords(self, n):
        """Check that k=v for all records and there are n records"""
        db = esedb.open(self._database)
        self.assertEqual(n, len(db))
        for k,v in db.iteritems():
            self.assertEqual(k, v)
        db.close()

    def _randomOperations(self):
        keys = 'abcdefghijklmompqrstuvwzyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ-+'
        db = esedb.open(self._database)
        for i in xrange(10000):
            k = random.choice(keys) * random.randint(1,8)
            ignored = db.nextkey(k)
            if random.random() < 0.01: # 1% chance of close and reopen
                db.close()
                db = esedb.open(self._database)
            elif random.random() < 0.05: # 5% chance of deleting a key
                try:
                    del db[k]
                except KeyError:
                    # the record wasn't there. retrieve all records instead
                    _ = db.keys()
            elif random.random() < 0.05: # 5% chance of popping a key
                db.pop(k, 'default')
            elif random.random() < 0.05: # 5% chance of popping an item
                try:
                    db.popitem()
                except KeyError:
                    # dictionary is empty                
                    pass             
            else:
                v = '#' * random.randint(256,1024)
                if random.random() < 0.25:
                    db.setdefault(k, v)
                else:
                    db[k] = v
        db.close()

    def testMultiThreadedInserts(self):
        threads = [threading.Thread(target = self._insertRange, args = (x*1000, (x+1) * 1000)) for x in range(4)]
        for t in threads:
            t.start()
        d = {}
        for i in xrange(4000):
            d[i] = str(i)
        for t in threads:
            t.join()
        self.assertEqual(len(d), len(self._db))
        self.assertEqual(d.keys().sort(), self._db.keys().sort())
        for k in d.keys():
            self.assertEqual(d[k], self._db[k])

    def testMultiThreadedSetDefaults(self):
        threads = [threading.Thread(target = self._setdefaultRange, args = (x*1000, (x+1) * 1000)) for x in range(4)]
        for t in threads:
            t.start()
        d = {}
        for i in xrange(4000):
            d[i] = str(i)
        for t in threads:
            t.join()
        self.assertEqual(len(d), len(self._db))
        self.assertEqual(d.keys().sort(), self._db.keys().sort())
        for k in d.keys():
            self.assertEqual(d[k], self._db[k])
            
    def testMultiThreadedReplaces(self):
        for i in xrange(4000):
            self._db[i] = 'XXXX'
        threads = [threading.Thread(target = self._insertRange, args = (x*1000, (x+1) * 1000)) for x in range(4)]
        for t in threads:
            t.start()
        d = {}
        for i in xrange(4000):
            d[i] = str(i)
        for t in threads:
            t.join()
        self.assertEqual(len(d), len(self._db))
        self.assertEqual(d.keys().sort(), self._db.keys().sort())
        for k in d.keys():
            self.assertEqual(d[k], self._db[k])

    def testMultiThreadedRetrieves(self):
        n = 4000
        for i in xrange(n):
            self._db[i] = i
        threads = [threading.Thread(target = self._retrieveAllRecords, args = (n,))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def testMultiThreadedDeletes(self):
        for i in xrange(4000):
            self._db[i] = i
        threads = [threading.Thread(target = self._deleteRange, args = (x*1000, (x+1) * 1000)) for x in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(0, len(self._db))
        self.assertEqual([], self._db.keys())
        self.assertEqual([], self._db.values())

    def testMultiThreadedPops(self):
        for i in xrange(4000):
            self._db[i] = i
        threads = [threading.Thread(target = self._popRange, args = (x*1000, (x+1) * 1000)) for x in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(0, len(self._db))
        self.assertEqual([], self._db.keys())
        self.assertEqual([], self._db.values())

    def testMultiThreadedPopItems(self):
        for i in xrange(4000):
            self._db[i] = i
        threads = [threading.Thread(target = self._popAllItems) for x in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(0, len(self._db))
        self.assertEqual([], self._db.keys())
        self.assertEqual([], self._db.values())
        
    def testRandomMultiThreadedOperations(self):
        threads = [threading.Thread(target = self._randomOperations) for x in range(8)]
        for t in threads:
            t.start()
        self._db.clear() # try a concurrent clear
        for t in threads:
            t.join()
        self.assertEqual(len(self._db), len(self._db.keys()))


if __name__ == '__main__':
    unittest.main()
