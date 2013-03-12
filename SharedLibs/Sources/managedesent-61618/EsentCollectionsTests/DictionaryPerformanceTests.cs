// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DictionaryPerformanceTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Test PersistentDictionary speed.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test PersistentDictionary speed.
    /// </summary>
    [TestClass]
    public class DictionaryPerformanceTests
    {
        /// <summary>
        /// Number of records to insert during testing.
        /// </summary>
        private const int N =
#if DEBUG
        100000;
#else
        1000000;
#endif

        /// <summary>
        /// The location of the dictionary we use for the tests.
        /// </summary>
        private const string DictionaryLocation = "PerformanceDictionary";

        /// <summary>
        /// Sequentially insert records into a Dictionary, measuring the performance.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void TestDictionarySequentialInsertAndLookupSpeed()
        {
            MeasureSequentialInsertAndLookupSpeed(new Dictionary<long, string>());
        }

        /// <summary>
        /// Sequentially insert records into a SortedDictionary, measuring the performance.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void TestSortedDictionarySequentialInsertAndLookupSpeed()
        {
            MeasureSequentialInsertAndLookupSpeed(new SortedDictionary<long, string>());
        }

        /// <summary>
        /// Sequentially insert records into a SortedList, measuring the performance.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void TestSortedListSequentialInsertAndLookupSpeed()
        {
            MeasureSequentialInsertAndLookupSpeed(new SortedList<long, string>());
        }

        /// <summary>
        /// Sequentially insert records into a PersistentDictionary, measuring the performance.
        /// </summary>
        [TestMethod]
        [Priority(4)]
        public void TestPersistentDictionarySequentialInsertAndLookupSpeed()
        {
            using (var dictionary = new PersistentDictionary<long, string>(DictionaryLocation))
            {
                MeasureSequentialInsertAndLookupSpeed(dictionary);
            }

            Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);
        }

        /// <summary>
        /// Randomly insert records into a Dictionary, measuring the performance.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void TestDictionaryRandomInsertAndLookupSpeed()
        {
            MeasureRandomInsertAndLookupSpeed(new Dictionary<long, string>());
        }

        /// <summary>
        /// Randomly insert records into a SortedDictionary, measuring the performance.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        public void TestSortedDictionaryRandomInsertAndLookupSpeed()
        {
            MeasureRandomInsertAndLookupSpeed(new SortedDictionary<long, string>());
        }

        /// <summary>
        /// Randomly insert records into a PersistentDictionary and measure the speed.
        /// </summary>
        [TestMethod]
        [Priority(4)]
        public void TestPersistentDictionaryBoxing()
        {
            using (var dictionary = new PersistentDictionary<long, long>(DictionaryLocation))
            {
                long[] keys = (from x in Enumerable.Range(0, N) select (long)x).ToArray();

                long data = 1;
                long newData = 2;

                GC.Collect();
                int collectionsAtStart = GC.CollectionCount(0);

                // Insert the records
                Insert(dictionary, keys, data);

                // Scan all entries to make sure they are in the cache
                ScanEntries(dictionary);

                // Retrieve one record
                RetrieveOneRecord(dictionary, 1000000, 3);

                // Now lookup entries
                keys.Shuffle();
                LookupEntries(dictionary, keys);

                // Now update the entries
                keys.Shuffle();
                UpdateAllEntries(dictionary, keys, newData);

                int collectionsAtEnd = GC.CollectionCount(0);
                Console.WriteLine("{0} generation 0 collections", collectionsAtEnd - collectionsAtStart);
            }

            Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);
        }

        /// <summary>
        /// Randomly insert records into a PersistentDictionary and measure the speed.
        /// </summary>
        [TestMethod]
        [Priority(4)]
        public void TestPersistentDictionaryRandomInsertAndLookupSpeed()
        {
            using (var dictionary = new PersistentDictionary<long, string>(DictionaryLocation))
            {
                MeasureRandomInsertAndLookupSpeed(dictionary);
            }

            Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);
        }

        /// <summary>
        /// Measure sequential insert and lookup speed in a generic IDictionary.
        /// </summary>
        /// <param name="dictionary">The dictionary to test.</param>
        private static void MeasureSequentialInsertAndLookupSpeed(IDictionary<long, string> dictionary)
        {
            SequentialInsertLookupAndUpdate(dictionary);
            SlowLinqQueries(dictionary, 20);
            LinqQueries(dictionary, 20);
            FastLinqQueries(dictionary, 20);
        }

        /// <summary>
        /// Measure sequential insert and lookup speed in a PersistentDictionary. This method
        /// takes a PersistentDictionary so that the PersistentDictionary Linq code will be used.
        /// </summary>
        /// <param name="dictionary">The dictionary to test.</param>
        private static void MeasureSequentialInsertAndLookupSpeed(PersistentDictionary<long, string> dictionary)
        {
            SequentialInsertLookupAndUpdate(dictionary);
            SlowLinqQueries(dictionary, 1000);
            LinqQueries(dictionary, 10000);
            FastLinqQueries(dictionary, 100000);
        }

        /// <summary>
        /// Measure random insert and lookup speed in a generic IDictionary.
        /// </summary>
        /// <param name="dictionary">The dictionary to test.</param>
        private static void MeasureRandomInsertAndLookupSpeed(IDictionary<long, string> dictionary)
        {
            RandomInsertLookupAndUpdate(dictionary);
            SlowLinqQueries(dictionary, 20);
            LinqQueries(dictionary, 20);
            FastLinqQueries(dictionary, 20);
        }

        /// <summary>
        /// Measure sequential insert and lookup speed in a PersistentDictionary. This method
        /// takes a PersistentDictionary so that the PersistentDictionary Linq code will be used.
        /// </summary>
        /// <param name="dictionary">The dictionary to test.</param>
        private static void MeasureRandomInsertAndLookupSpeed(PersistentDictionary<long, string> dictionary)
        {
            RandomInsertLookupAndUpdate(dictionary);
            SlowLinqQueries(dictionary, 1000);
            LinqQueries(dictionary, 10000);
            FastLinqQueries(dictionary, 100000);
        }

        /// <summary>
        /// Insert records in sequential order, retrieve them and update.
        /// </summary>
        /// <param name="dictionary">The dictionary to use.</param>
        private static void SequentialInsertLookupAndUpdate(IDictionary<long, string> dictionary)
        {
            const string Data = "01234567890ABCDEF01234567890ABCDEF";
            const string Newdata = "something completely different";

            long[] keys = (from x in Enumerable.Range(0, N) select (long)x).ToArray();

            // Insert the records
            Insert(dictionary, keys, Data);

            // Repeatedly read one record
            keys.Shuffle();
            long key = keys[0];
            Assert.AreEqual(Data, dictionary[key]);
            RetrieveOneRecord(dictionary, N, key);

            // Scan all entries to make sure they are in the cache
            ScanEntries(dictionary);

            // Now lookup entries
            keys.Shuffle();
            LookupEntries(dictionary, keys);

            // Now update the entries
            keys.Shuffle();
            UpdateAllEntries(dictionary, keys, Newdata);
        }

        /// <summary>
        /// Insert records in random order, retrieve them and update.
        /// </summary>
        /// <param name="dictionary">The dictionary to use.</param>
        private static void RandomInsertLookupAndUpdate(IDictionary<long, string> dictionary)
        {
            long[] keys = (from x in Enumerable.Range(0, N) select (long)x).ToArray();
            keys.Shuffle();

            const string Data = "01234567890ABCDEF01234567890ABCDEF";
            const string Newdata = "something completely different";

            // Insert the records
            Insert(dictionary, keys, Data);

            // Scan all entries to make sure they are in the cache
            ScanEntries(dictionary);

            // Now lookup entries
            keys.Shuffle();
            LookupEntries(dictionary, keys);

            // Now update the entries
            keys.Shuffle();
            UpdateAllEntries(dictionary, keys, Newdata);
        }

        /// <summary>
        /// Measure the speed of LINQ queries against the dictionary.
        /// </summary>
        /// <param name="dictionary">The dictionary to query.</param>
        /// <param name="numQueries">Number of queries to perform.</param>
        private static void SlowLinqQueries(IDictionary<long, string> dictionary, int numQueries)
        {
            var rand = new Random();
            Stopwatch stopwatch = Stopwatch.StartNew();
            int n = dictionary.Count;
            int total = 0;
            for (int i = 0; i < numQueries; ++i)
            {
                // Retrieve up to 10 records (average of 5)
                int min = rand.Next(0, n - 1);
                int max = rand.Next(min, Math.Min(min + 10, n)); // we'll add 1 to this below

                var query = from x in dictionary where min <= x.Key && x.Key < max + 1 && x.Value.Length > 0 select x.Value;
                Assert.AreEqual((max + 1) - min, query.Count());
                total += max - min;
            }

            stopwatch.Stop();
            Console.WriteLine(
                "Did {0:N0} LINQ queries in {1} ({2:N0} queries/second, {3:N0} records/second)",
                numQueries,
                stopwatch.Elapsed,
                numQueries * 1000 / stopwatch.ElapsedMilliseconds,
                total * 1000 / stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Measure the speed of slow LINQ queries against the PersistentDictionary.
        /// These queries are slow because they create the query inside of
        /// the loop and the query has to be compiled each time.
        /// </summary>
        /// <param name="dictionary">The dictionary to query.</param>
        /// <param name="numQueries">Number of queries to perform.</param>
        private static void SlowLinqQueries(PersistentDictionary<long, string> dictionary, int numQueries)
        {
            var rand = new Random();
            Stopwatch stopwatch = Stopwatch.StartNew();
            int n = dictionary.Count;
            int total = 0;
            for (int i = 0; i < numQueries; ++i)
            {
                // Retrieve up to 10 records (average of 5)
                int min = rand.Next(0, n - 1);
                int max = rand.Next(min, Math.Min(min + 10, n)); // we'll add 1 to this below

                var query = from x in dictionary where min <= x.Key && x.Key < max + 1 && x.Value.Length > 0 select x.Value;
                Assert.AreEqual((max + 1) - min, query.Count());
                total += max - min;
            }

            stopwatch.Stop();
            Console.WriteLine(
                "Did {0:N0} LINQ queries in {1} ({2:N0} queries/second, {3:N0} records/second)",
                numQueries,
                stopwatch.Elapsed,
                numQueries * 1000 / stopwatch.ElapsedMilliseconds,
                total * 1000 / stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Measure the speed of LINQ queries against the dictionary.
        /// </summary>
        /// <param name="dictionary">The dictionary to query.</param>
        /// <param name="numQueries">Number of queries to perform.</param>
        private static void LinqQueries(IDictionary<long, string> dictionary, int numQueries)
        {
            var rand = new Random();
            Stopwatch stopwatch = Stopwatch.StartNew();
            int n = dictionary.Count;
            int total = 0;
            for (int i = 0; i < numQueries; ++i)
            {
                // Retrieve up to 10 records (average of 5)
                int min = rand.Next(0, n - 1);
                int max = rand.Next(min + 1, Math.Min(min + 11, n));

                var query = from x in dictionary where min <= x.Key && x.Key < max select x.Value;
                Assert.AreEqual(max - min, query.Count());
                total += max - min;
            }

            stopwatch.Stop();
            Console.WriteLine(
                "Did {0:N0} LINQ queries in {1} ({2:N0} queries/second, {3:N0} records/second)",
                numQueries,
                stopwatch.Elapsed,
                numQueries * 1000 / stopwatch.ElapsedMilliseconds,
                total * 1000 / stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Measure the speed of LINQ queries against the PersistentDictionary.
        /// </summary>
        /// <param name="dictionary">The dictionary to query.</param>
        /// <param name="numQueries">Number of queries to perform.</param>
        private static void LinqQueries(PersistentDictionary<long, string> dictionary, int numQueries)
        {
            var rand = new Random();
            Stopwatch stopwatch = Stopwatch.StartNew();
            int n = dictionary.Count;
            int total = 0;
            for (int i = 0; i < numQueries; ++i)
            {
                // Retrieve up to 10 records (average of 5)
                int min = rand.Next(0, n - 1);
                int max = rand.Next(min + 1, Math.Min(min + 11, n));

                var query = from x in dictionary where min <= x.Key && x.Key < max select x.Value;
                Assert.AreEqual(max - min, query.Count());
                total += max - min;
            }

            stopwatch.Stop();
            Console.WriteLine(
                "Did {0:N0} PersistentDictionary LINQ queries in {1} ({2:N0} queries/second, {3:N0} records/second)",
                numQueries,
                stopwatch.Elapsed,
                numQueries * 1000 / stopwatch.ElapsedMilliseconds,
                total * 1000 / stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Measure the speed of fast LINQ queries against the dictionary.
        /// </summary>
        /// <param name="dictionary">The dictionary to query.</param>
        /// <param name="numQueries">Number of queries to perform.</param>
        private static void FastLinqQueries(IDictionary<long, string> dictionary, int numQueries)
        {
            var rand = new Random();
            Stopwatch stopwatch = Stopwatch.StartNew();
            int n = dictionary.Count;

            // Create the Enumerable outside of the loop.
            int key = 0;
            var query = from x in dictionary where x.Key == key && !String.IsNullOrEmpty(x.Value) select x;

            int total = 0;
            for (int i = 0; i < numQueries; ++i)
            {
                key = rand.Next(0, n);
                foreach (var x in query)
                {
                    Assert.AreEqual(key, x.Key);
                    total++;
                }
            }

            stopwatch.Stop();
            stopwatch.Stop();
            Console.WriteLine(
                "Did {0:N0} LINQ queries in {1} ({2:N0} queries/second, {3:N0} records/second)",
                numQueries,
                stopwatch.Elapsed,
                numQueries * 1000 / stopwatch.ElapsedMilliseconds,
                total * 1000 / stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Measure the speed of fast LINQ queries against the PersistentDictionary.
        /// </summary>
        /// <param name="dictionary">The dictionary to query.</param>
        /// <param name="numQueries">Number of queries to perform.</param>
        private static void FastLinqQueries(PersistentDictionary<long, string> dictionary, int numQueries)
        {
            var rand = new Random();
            Stopwatch stopwatch = Stopwatch.StartNew();
            int n = dictionary.Count;

            // Create the Enumerable outside of the loop. This means the expression
            // tree only has to be compiled once.
            int key = 0;
            var query = from x in dictionary where x.Key == key && !String.IsNullOrEmpty(x.Value) select x;

            int total = 0;
            for (int i = 0; i < numQueries; ++i)
            {
                key = rand.Next(0, n);
                foreach (var x in query)
                {
                    Assert.AreEqual(key, x.Key);
                    total++;
                }
            }

            stopwatch.Stop();
            stopwatch.Stop();
            Console.WriteLine(
                "Did {0:N0} PersistentDictionary LINQ queries in {1} ({2:N0} queries/second, {3:N0} records/second)",
                numQueries,
                stopwatch.Elapsed,
                numQueries * 1000 / stopwatch.ElapsedMilliseconds,
                total * 1000 / stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Repeatedly retrieve the same entry.
        /// </summary>
        /// <param name="dictionary">The dictionary containing the element.</param>
        /// <param name="numRetrieves">Number of times to retrieve the entry.</param>
        /// <param name="key">The key of the entry to retrieve.</param>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary value.</typeparam>
        private static void RetrieveOneRecord<TKey, TValue>(IDictionary<TKey, TValue> dictionary, int numRetrieves, TKey key)
            where TKey : IComparable<TKey>
            where TValue : IEquatable<TValue>
        {
            TValue expected = dictionary[key];
            Stopwatch stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < numRetrieves; ++i)
            {
                Assert.IsTrue(expected.Equals(dictionary[key]), "Got wrong entry");
            }

            stopwatch.Stop();
            Console.WriteLine(
                "Read one record {0:N0} times {1} ({2:N0} reads/second)",
                numRetrieves,
                stopwatch.Elapsed,
                numRetrieves * 1000 / stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Update the specified entries.
        /// </summary>
        /// <param name="dictionary">The dictionary to update.</param>
        /// <param name="keys">The keys of the entries to update.</param>
        /// <param name="newData">The data to set the entries to.</param>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary value.</typeparam>
        private static void UpdateAllEntries<TKey, TValue>(IDictionary<TKey, TValue> dictionary, ICollection<TKey> keys, TValue newData)
            where TKey : IComparable<TKey>
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            foreach (TKey key in keys)
            {
                dictionary[key] = newData;
            }

            stopwatch.Stop();
            Console.WriteLine(
                "Updated {0:N0} records in {1} ({2:N0} records/second)",
                keys.Count,
                stopwatch.Elapsed,
                keys.Count * 1000 / stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Retrieve all the entries specified by the keys.
        /// </summary>
        /// <param name="dictionary">The dictionary to lookup entries in.</param>
        /// <param name="keys">The keys to retrieve.</param>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary value.</typeparam>
        private static void LookupEntries<TKey, TValue>(IDictionary<TKey, TValue> dictionary, ICollection<TKey> keys)
            where TKey : IComparable<TKey>
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            foreach (TKey key in keys)
            {
                TValue s;
                if (!dictionary.TryGetValue(key, out s))
                {
                    Assert.Fail("Key wasn't found");
                }
            }

            stopwatch.Stop();
            Console.WriteLine(
                "Looked up {0:N0} records in {1} ({2:N0} records/second)",
                keys.Count,
                stopwatch.Elapsed,
                keys.Count * 1000 / stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Scan all the dictionary entries.
        /// </summary>
        /// <param name="dictionary">The dictionary to scan.</param>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary value.</typeparam>
        private static void ScanEntries<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> dictionary)
            where TKey : IComparable<TKey>
        {
            int i = 0;
            Stopwatch stopwatch = Stopwatch.StartNew();
            foreach (KeyValuePair<TKey, TValue> item in dictionary)
            {
                i++;
            }

            stopwatch.Stop();
            Console.WriteLine(
                "Scanned {0:N0} records in {1} ({2:N0} records/second)",
                i,
                stopwatch.Elapsed,
                i * 1000 / stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Insert the specified data using Add.
        /// </summary>
        /// <param name="dictionary">The dictionary to add elements to.</param>
        /// <param name="keys">The keys to insert.</param>
        /// <param name="data">The data for the keys.</param>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary value.</typeparam>
        private static void Insert<TKey, TValue>(IDictionary<TKey, TValue> dictionary, ICollection<TKey> keys, TValue data)
            where TKey : IComparable<TKey>
        {
            var stopwatch = Stopwatch.StartNew();
            foreach (TKey key in keys)
            {
                dictionary.Add(key, data);
            }

            stopwatch.Stop();
            Console.WriteLine(
                "Inserted {0:N0} records in {1} ({2:N0} records/second)",
                keys.Count,
                stopwatch.Elapsed,
                keys.Count * 1000 / stopwatch.ElapsedMilliseconds);
        }
    }
}

