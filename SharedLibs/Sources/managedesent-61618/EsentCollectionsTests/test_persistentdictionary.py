
import unittest
import random
import threading
import System

from System.IO import Directory
from System.IO import Path
from System.Collections.Generic import Dictionary
from System.Collections.Generic import SortedDictionary
from System.Collections.Generic import SortedList

import clr
clr.AddReferenceByPartialName('Esent.Collections')
from Microsoft.Isam.Esent.Collections.Generic import PersistentDictionary

def deleteDirectory(directory):
    if Directory.Exists(directory):
        Directory.Delete(directory, True)

class SingleDictionaryFixture(unittest.TestCase):

    def setUp(self):
        self._dataDirectory = 'SingleDictionaryFixture'
        self._deleteDataDirectory()
        self._dict = PersistentDictionary[System.String,System.String](self._dataDirectory)

    def tearDown(self):
        self._dict.Dispose()
        self._deleteDataDirectory()

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def testInsertAndRetrieveRecord(self):
        self._dict['key'] = 'value'
        self.assertEqual(self._dict['key'], 'value')

    def testLargeKey(self):
        # esent may truncate the key, but we should be able to set all this data
        key = 'K' * 1024*1024
        self._dict[key] = 'value'
        self.assertEqual(self._dict[key], 'value')

    def testLargeValue(self):
        value = 'V' * 1024*1024
        self._dict['bigstuff'] = value
        self.assertEqual(self._dict['bigstuff'], value)

    def testNullKey(self):
        self._dict[None] = 'value'
        self.assertEqual(self._dict[None], 'value')

    def testNullValue(self):
        self._dict['key'] = None
        self.assertEqual(self._dict['key'], None)

    def testOverwriteRecord(self):
        self._dict['key'] = 'value'
        self._dict['key'] = 'newvalue'
        self.assertEqual(self._dict['key'], 'newvalue')

    def testContainsKeyReturnsFalseWhenKeyNotPresent(self):
        self.assertEqual(False, self._dict.ContainsKey('key'))

    def testContainsKeyReturnsTrueWhenKeyIsPresent(self):
        self._dict['key'] = 'value'
        self.assertEqual(True, self._dict.ContainsKey('key'))

    def testRemoveRemovesKey(self):
        self._dict['key'] = 'value'
        self.assertEqual(True, self._dict.Remove('key'))
        self.assertEqual(False, self._dict.ContainsKey('key'))

    def testRemoveReturnsFalseWhenKeyNotPresent(self):
        self.assertEqual(False, self._dict.Remove('key'))

    def testCountIsZeroWhenDictionaryIsEmpty(self):
        self.assertEqual(0, self._dict.Count)

    def testCountIncreasesWithInsert(self):
        self._dict['a'] = 'a'
        self._dict['b'] = 'b'
        self.assertEqual(2, self._dict.Count)

    def testLenDecreasesWithDelete(self):
        self._dict['a'] = 'a'
        self._dict['b'] = 'b'
        self._dict['c'] = 'c'
        self._dict.Remove('b')
        self.assertEqual(2, self._dict.Count)

    def testClearOnEmptyDictionary(self):
        self._dict.Clear()
        self.assertEqual(0, self._dict.Count)

    def testClearRemovesRecords(self):
        self._dict['b'] = 'b'
        self._dict['a'] = 'a'
        self._dict.Clear()
        self.assertEqual(0, self._dict.Count)

class DictionaryFixture(unittest.TestCase):

    def setUp(self):
        self._dataDirectory = 'DictionaryFixture'
        self._deleteDataDirectory()

    def tearDown(self):
        self._deleteDataDirectory()

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def disposeCloseTwice(self):
        dict = PersistentDictionary[System.Guid,System.Int64](self._dataDirectory)
        dict.Dispose()
        dict.Dispose()

    def testMultipleDictionaries(self):
        dict1 = PersistentDictionary[System.Int32,System.String](self._dataDirectory + '\\a')
        dict2 = PersistentDictionary[System.String,System.Int32](self._dataDirectory + '\\b')

        dict1[0] = 'hello'
        dict2['world'] = 1

        self.assertEqual('hello', dict1[0])
        self.assertEqual(1, dict2['world'])

        dict1.Dispose()
        dict2.Dispose()

    def testCloseAndReopenEmptyDictionary(self):
        dict = PersistentDictionary[System.DateTime,System.UInt16](self._dataDirectory)
        dict.Dispose()

        dict = PersistentDictionary[System.DateTime,System.UInt16](self._dataDirectory)
        self.assertEqual(0, dict.Count)
        dict.Dispose()

class DictionaryComparisonFixture(unittest.TestCase):
    def setUp(self):
        self._dataDirectory = 'DictionaryComparisonFixture'
        self._deleteDataDirectory()
        self._createDictionary()
        self._expected = Dictionary[System.String,System.String]()

    def tearDown(self):
        self._closeDictionary()
        self._deleteDataDirectory()

    def _createDictionary(self):
        self._dict = PersistentDictionary[System.String,System.String](self._dataDirectory)

    def _closeDictionary(self):
        self._dict.Dispose()

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def _compareWithExpected(self):
        self.assertEqual(self._expected.Count, self._dict.Count)
        for k in self._expected.Keys:
            self.assertEqual(self._expected[k], self._dict[k])

    def _insert(self, k, v):
        self._expected[k] = v
        self._dict[k] = v

    def _delete(self, k):
        self._expected.Remove(k)
        self._dict.Remove(k)

    def _clear(self):
        self._expected.Clear()
        self._dict.Clear()

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

    def testCloseAndOpen(self):
        for i in xrange(16):
            self._insert(str(i), '?' * i)
        self._compareWithExpected()
        self._closeDictionary()
        self._createDictionary()
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
                self._closeDictionary()
                self._createDictionary()
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

class MultiThreadingFixture(unittest.TestCase):
    def setUp(self):
        self._dataDirectory = 'MultiThreadingFixture'
        self._deleteDataDirectory()
        self._dict = PersistentDictionary[System.String,System.String](self._dataDirectory)

    def tearDown(self):
        self._dict.Dispose()
        self._deleteDataDirectory()

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def _insertRange(self, low, high):
        for i in xrange(low, high):
            self._dict[str(i)] = str(i)

    def _deleteRange(self, low, high):
        for i in xrange(low, high):
            self._dict.Remove(str(i))

    def _retrieveAllRecords(self, n):
        """Check that key=value for all records and there are n records"""
        self.assertEqual(n, self._dict.Count)
        for i in self._dict:
            self.assertEqual(i.Key, i.Value)

    def _randomOperations(self):
        keys = 'abcdefghijklmompqrstuvwzyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ-+'
        for i in xrange(10000):
            k = random.choice(keys) * random.randint(1,8)
            if random.random() < 0.10:
                self._dict.Remove(k)
            else:
                v = '#' * random.randint(256,1024)
                self._dict[k] = v

    def testMultiThreadedInserts(self):
        threads = [threading.Thread(target = self._insertRange, args = (x*1000, (x+1) * 1000)) for x in range(4)]
        for t in threads:
            t.start()
        d = {}
        for i in xrange(4000):
            d[str(i)] = str(i)
        for t in threads:
            t.join()
        self.assertEqual(len(d), self._dict.Count)
        for k in d.keys():
            self.assertEqual(d[k], self._dict[k])

    def testMultiThreadedReplaces(self):
        for i in xrange(4000):
            self._dict[str(i)] = 'XXXX'
        threads = [threading.Thread(target = self._insertRange, args = (x*1000, (x+1) * 1000)) for x in range(4)]
        for t in threads:
            t.start()
        d = {}
        for i in xrange(4000):
            d[str(i)] = str(i)
        for t in threads:
            t.join()
        self.assertEqual(len(d), self._dict.Count)
        for k in d.keys():
            self.assertEqual(d[k], self._dict[k])

    def testMultiThreadedRetrieves(self):
        n = 4000
        for i in xrange(n):
            self._dict[str(i)] = str(i)
        threads = [threading.Thread(target = self._retrieveAllRecords, args = (n,))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def testMultiThreadedDeletes(self):
        for i in xrange(4000):
            self._dict[str(i)] = str(i)
        threads = [threading.Thread(target = self._deleteRange, args = (x*1000, (x+1) * 1000)) for x in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(0, self._dict.Count)

    def testRandomMultiThreadedOperations(self):
        threads = [threading.Thread(target = self._randomOperations) for x in range(8)]
        for t in threads:
            t.start()
        self._dict.Clear() # try a concurrent clear
        for t in threads:
            t.join()

class GenericDictionaryFixtureBase(unittest.TestCase):

    def _deleteDataDirectory(self):
        deleteDirectory(self._dataDirectory)

    def _add(self, expected, actual, k, v):
        """Add (k,v). This fails if k already exists."""
        actual.Add(k,v)
        expected.Add(k,v)

    def _set(self, expected, actual, k, v):
        """Set k = v."""
        actual[k] = v
        expected[k] = v

    def _remove(self, expected, actual, k):
        self.assertEqual(True, actual.Remove(k))
        self.assertEqual(True, expected.Remove(k))

    def _clear(self, expected, actual):
        actual.Clear()
        expected.Clear()

    def _checkKeyIsNotPresent(self, dict, k):
        self.assertEqual(False, dict.Keys.Contains(k))
        self.assertEqual(False, dict.ContainsKey(k))
        self.assertEqual(False, dict.TryGetValue(k)[0])
        self.assertEqual(False, dict.Remove(k))

    def _checkDuplicateKeyError(self, dict, k, v):
        self.assertRaises(System.ArgumentException, dict.Add, k, v)

    def _compareDictionaries(self, expected, actual):
        self.assertEqual(expected.Count, actual.Count)
        self.assertEqual(expected.Keys.Count, actual.Keys.Count)
        self.assertEqual(expected.Values.Count, actual.Values.Count)
        for i in expected:
            self.assertEqual(True, actual.Contains(i))
            self.assertEqual(True, actual.ContainsKey(i.Key))
            self.assertEqual(True, actual.ContainsValue(i.Value))
            self.assertEqual(True, actual.Keys.Contains(i.Key))
            self.assertEqual(True, actual.Values.Contains(i.Value))
            (f,v) = actual.TryGetValue(i.Key)
            self.assertEqual(True, f)
            self.assertEqual(i.Value, v)
            self.assertEqual(i.Value, actual[i.Key])
        for i in actual:
            self.assertEqual(True, expected.ContainsKey(i.Key))
        for k in actual.Keys:
            self.assertEqual(True, expected.ContainsKey(k))
        for v in actual.Values:
            self.assertEqual(True, expected.Values.Contains(v))

    def _doTest(self, expected, actual, keys, values):
        # Compare empty
        self._compareDictionaries(expected, actual)

        # Insert with Add()
        for k in keys:
            v = random.choice(values)
            self._add(expected, actual, k, v)
        self._compareDictionaries(expected, actual)

        # Replace with []
        # Make sure to try setting every value
        k = random.choice(keys)
        for v in values:
            self._set(expected, actual, k, v)
            self._compareDictionaries(expected, actual)

        # Delete key, reinsert with []
        k = random.choice(keys)
        v = random.choice(values)
        self._checkDuplicateKeyError(actual, k, v)
        self._remove(expected, actual, k)
        self._checkKeyIsNotPresent(actual, k)
        self._compareDictionaries(expected, actual)
        self._set(expected, actual, k, v)
        self._compareDictionaries(expected, actual)

        # for i in actual:
        #     print '%s => %.32s' % (i.Key, i.Value)

        # Clear
        self._clear(expected, actual)
        self._compareDictionaries(expected, actual)

    def createDictAndTest(self, tkey, tvalue):
        dict = PersistentDictionary[tkey,tvalue](self._dataDirectory)
        try:
            expected = Dictionary[tkey,tvalue]()
            self._doTest(expected, dict, data[tkey], data[tvalue])
        finally:
            dict.Dispose()

class GenericDictionaryFixture(GenericDictionaryFixtureBase):
    def setUp(self):
        self._dataDirectory = 'GenericDictionaryFixture'
        self._deleteDataDirectory()
        self._dict = None

    def tearDown(self):
        self._deleteDataDirectory()

    def createDictAndTest(self, tkey, tvalue):
        dict = PersistentDictionary[tkey,tvalue](self._dataDirectory)
        try:
            expected = Dictionary[tkey,tvalue]()
            self._doTest(expected, dict, data[tkey], data[tvalue])
        finally:
            dict.Dispose()

class SortedGenericDictionaryFixture(GenericDictionaryFixtureBase):
    def setUp(self):
        self._dataDirectory = 'SortedGenericDictionaryFixture'
        self._deleteDataDirectory()
        self._dict = None

    def tearDown(self):
        self._deleteDataDirectory()
        
    def _compareDictionaries(self, expected, actual):
        super(SortedGenericDictionaryFixture, self)._compareDictionaries(expected, actual)
        for x,y in zip(expected.Keys, actual.Keys):
            self.assertEqual(x, y)

    def createDictAndTest(self, tkey, tvalue):
        dict = PersistentDictionary[tkey,tvalue](self._dataDirectory)
        try:
            expected = SortedDictionary[tkey,tvalue]()
            self._doTest(expected, dict, data[tkey], data[tvalue])
        finally:
            dict.Dispose()

class SortedGenericListFixture(SortedGenericDictionaryFixture):
    def setUp(self):
        self._dataDirectory = 'SortedGenericListFixture'
        self._deleteDataDirectory()
        self._dict = None

    def tearDown(self):
        self._deleteDataDirectory()
        
    def createDictAndTest(self, tkey, tvalue):
        dict = PersistentDictionary[tkey,tvalue](self._dataDirectory)
        try:
            expected = SortedList[tkey,tvalue]()
            self._doTest(expected, dict, data[tkey], data[tvalue])
        finally:
            dict.Dispose()
            
keytypes = [
    System.Boolean,
    System.Byte,
    System.Int16,
    System.UInt16,
    System.Int32,
    System.UInt32,
    System.Int64,
    System.UInt64,
    System.Single,
    System.Double,
    System.DateTime,
    System.TimeSpan,
    System.Guid,
    System.String,
    ]

nullabletypes = [
    System.Boolean,
    System.Byte,
    System.Int16,
    System.UInt16,
    System.Int32,
    System.UInt32,
    System.Int64,
    System.UInt64,
    System.Single,
    System.Double,
    System.DateTime,
    System.TimeSpan,
    System.Guid,
    ]

valuetypes = [
    System.Boolean,
    System.Byte,
    System.Int16,
    System.UInt16,
    System.Int32,
    System.UInt32,
    System.Int64,
    System.UInt64,
    System.Single,
    System.Double,
    System.DateTime,
    System.TimeSpan,
    System.Guid,
    System.String,
    System.Decimal,
    ]

r = System.Random()

data = {}
data[System.Boolean] = [
    True,
    False]
data[System.Byte] = [
    1,
    2,
    System.Byte.MinValue,
    System.Byte.MaxValue,
    r.Next(System.Byte.MinValue, System.Byte.MaxValue)]
data[System.Int16] = [
    0,
    1,
    -1,
    System.Int16.MinValue,
    System.Int16.MaxValue,
    r.Next(System.Int16.MinValue, System.Int16.MaxValue)]
data[System.UInt16] = [
    1,
    2,
    System.UInt16.MinValue,
    System.UInt16.MaxValue,
    r.Next(System.UInt16.MinValue, System.UInt16.MaxValue)]
data[System.Int32] = [
    0,
    1,
    -1,
    System.Int32.MinValue,
    System.Int32.MaxValue,
    r.Next()]
data[System.UInt32] = [
    1,
    2,
    System.UInt32.MinValue,
    System.UInt32.MaxValue,
    r.Next(0, System.Int32.MaxValue)]
data[System.Int64] = [
    0,
    1,
    -1,
    System.Int64.MinValue,
    System.Int64.MaxValue,
    r.Next()]
data[System.UInt64] = [
    1,
    2,
    System.UInt64.MinValue,
    System.UInt64.MaxValue,
    r.Next(0, System.Int32.MaxValue)]
data[System.Single] = [
    0,
    1,
    -1,
    System.Single.MinValue,
    System.Single.MaxValue,
    r.Next()]
data[System.Double] = [
    0,
    1,
    -1,
    System.Math.PI,
    System.Double.MinValue,
    System.Double.MaxValue,
    r.NextDouble()]
data[System.Decimal] = [
    System.Decimal.MinValue,
    System.Decimal.MaxValue,
    System.Decimal.MinusOne,
    System.Decimal.Zero,
    System.Decimal.One,
    System.Decimal(r.Next()),
    System.Decimal(r.NextDouble())]
data[System.Guid] = [
    System.Guid.Empty,
    System.Guid.NewGuid()]
data[System.DateTime] = [
    System.DateTime.MinValue,
    System.DateTime.MaxValue,
    System.DateTime.Now,
    System.DateTime.UtcNow,
    System.DateTime.Today]
data[System.TimeSpan] = [
    System.TimeSpan.MinValue,
    System.TimeSpan.MaxValue,
    System.TimeSpan.FromDays(1),
    System.TimeSpan.FromHours(1),
    System.TimeSpan.FromMinutes(1),
    System.TimeSpan.FromSeconds(1),
    System.TimeSpan.FromMilliseconds(1),
    System.TimeSpan.FromTicks(1),
    System.TimeSpan(r.Next())]
data[System.String] = [
    System.String.Empty,
    '1',
    '`',
    'foo',
    'bar',
    'baz',
    'space',
    'space ',
    'case',
    'CASE',
    'punctuation',
    'punctuation!',
    r.Next().ToString(),
    r.NextDouble().ToString(),
    System.Guid.NewGuid.ToString(),
    System.DateTime.Now.ToString(),
    '#'*65000]

# Use this to create a unique closure for tkey and tvalue
def makef(tkey, tvalue):
    return lambda self : self.createDictAndTest(tkey, tvalue)

# Make nullable data, which is the non-nullable data + None
for t in nullabletypes:
    data[System.Nullable[t]] = list(data[t])
    data[System.Nullable[t]].append(None)
    valuetypes.append(System.Nullable[t])

# Create the test functions
for tkey in keytypes:
    for tvalue in valuetypes:
        name = 'test%s%s' % (tkey, tvalue)
        setattr(GenericDictionaryFixture, name, makef(tkey, tvalue))
        setattr(SortedGenericDictionaryFixture, name, makef(tkey, tvalue))
        setattr(SortedGenericListFixture, name, makef(tkey, tvalue))

if __name__ == '__main__':
    unittest.main()
