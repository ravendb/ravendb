// --------------------------------------------------------------------------------------------------------------------
// <copyright file="GenericDictionaryTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Test PersistentDictionary with different types.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test PersistentDictionary with different types.
    /// </summary>
    [TestClass]
    public class GenericDictionaryTests
    {
        /// <summary>
        /// Path to put the dictionary in.
        /// </summary>
        private const string DictionaryPath = "GenericDictionary";

        /// <summary>
        /// Cleanup the GenericDictionaryTests fixture.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the GenericDictionaryTests fixture")]
        public void Teardown()
        {
            Cleanup.DeleteDirectoryWithRetry(DictionaryPath);
        }

        /// <summary>
        /// Test a PersistentDictionary{Boolean, Boolean}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a bool dictionary")]
        public void TestGenericBooleanDictionary()
        {
            TestNullableGenericDictionary<bool, bool>();
        } 

        /// <summary>
        /// Test a PersistentDictionary{Byte, Byte}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a byte dictionary")]
        public void TestGenericByteDictionary()
        {
            TestNullableGenericDictionary<byte, byte>();
        }

        /// <summary>
        /// Test a PersistentDictionary{Int16, Int16}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test an Int16 dictionary")]
        public void TestGenericInt16Dictionary()
        {
            TestNullableGenericDictionary<short, short>();
        }

        /// <summary>
        /// Test a PersistentDictionary{UInt16, UInt16}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test an UInt16 dictionary")]
        public void TestGenericUInt16Dictionary()
        {
            TestNullableGenericDictionary<ushort, ushort>();
        }

        /// <summary>
        /// Test a PersistentDictionary{Int32, Int32}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test an Int32 dictionary")]
        public void TestGenericInt32Dictionary()
        {
            TestNullableGenericDictionary<int, int>();
        }

        /// <summary>
        /// Test a PersistentDictionary{UInt32, UInt32}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test an UInt32 dictionary")]
        public void TestGenericUInt32Dictionary()
        {
            TestNullableGenericDictionary<uint, uint>();
        }

        /// <summary>
        /// Test a PersistentDictionary{Int64, Int64}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test an Int64 dictionary")]
        public void TestGenericInt64Dictionary()
        {
            TestNullableGenericDictionary<long, long>();
        }

        /// <summary>
        /// Test a PersistentDictionary{UInt64, UInt64}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test an UInt64 dictionary")]
        public void TestGenericUInt64Dictionary()
        {
            TestNullableGenericDictionary<ulong, ulong>();
        }

        /// <summary>
        /// Test a PersistentDictionary{float, float}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a float dictionary")]
        public void TestGenericFloatDictionary()
        {
            TestNullableGenericDictionary<float, float>();
        }

        /// <summary>
        /// Test a PersistentDictionary{double, double}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a double dictionary")]
        public void TestGenericDoubleDictionary()
        {
            TestNullableGenericDictionary<double, double>();
        }

        /// <summary>
        /// Test a PersistentDictionary{DateTime, DateTime}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a DateTime dictionary")]
        public void TestGenericDateTimeDictionary()
        {
            TestNullableGenericDictionary<DateTime, DateTime>();
        }

        /// <summary>
        /// Test a PersistentDictionary{TimeSpan, TimeSpan}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a TimeSpan dictionary")]
        public void TestGenericTimeSpanDictionary()
        {
            TestNullableGenericDictionary<TimeSpan, TimeSpan>();
        }

        /// <summary>
        /// Test a PersistentDictionary{Guid, Guid}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a Guid dictionary")]
        public void TestGenericGuidDictionary()
        {
            TestNullableGenericDictionary<Guid, Guid>();
        }

        /// <summary>
        /// Test a PersistentDictionary{String, String}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a String dictionary")]
        public void TestGenericStringDictionary()
        {
            using (var dictionary = new PersistentDictionary<string, string>(DictionaryPath))
            {
                RunDictionaryTests(dictionary, "foo", "bar");
                RunDictionaryTests(dictionary, String.Empty, String.Empty);
            }
        }

        /// <summary>
        /// Test a PersistentDictionary{String, Decimal}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a String => Decimal dictionary")]
        public void TestGenericStringDecimalDictionary()
        {
            using (var dictionary = new PersistentDictionary<string, decimal?>(DictionaryPath))
            {
                RunDictionaryTests(dictionary, "one", Decimal.One);
            }
        }

        /// <summary>
        /// Test a PersistentDictionary{String, IPAddress}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a String => IPAddress dictionary")]
        public void TestGenericStringIpAddressDictionary()
        {
            using (var dictionary = new PersistentDictionary<string, IPAddress>(DictionaryPath))
            {
                RunDictionaryTests(dictionary, "localhost", IPAddress.Loopback);
            }
        }

        /// <summary>
        /// Test a PersistentDictionary{String, Uri}
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test a String => Uri dictionary")]
        public void TestGenericStringUriDictionary()
        {
            using (var dictionary = new PersistentDictionary<string, Uri>(DictionaryPath))
            {
                RunDictionaryTests(dictionary, "http", new Uri("http://localhost"));
            }
        }

        /// <summary>
        /// Run a set of tests against a dictionary.
        /// </summary>
        /// <typeparam name="TKey">Key type of the dictionary.</typeparam>
        /// <typeparam name="TValue">Value type of the dictionary.</typeparam>
        /// <param name="dictionary">The dictionary to test.</param>
        /// <param name="key">Key value to use.</param>
        /// <param name="value">Data value to use.</param>
        private static void RunDictionaryTests<TKey, TValue>(PersistentDictionary<TKey, TValue> dictionary, TKey key, TValue value)
            where TKey : IComparable<TKey>
        {
            Assert.IsFalse(dictionary.IsReadOnly, "Dictionary is read-only");
            Assert.AreEqual(DictionaryPath, dictionary.Database);

            TestBasicOperations(dictionary, key, value);

            // Add the value
            var kvp = new KeyValuePair<TKey, TValue>(key, value);
            dictionary.Add(kvp);

            dictionary.Flush();

            TestDictionaryLinq(dictionary, key, value);
            TestDictionaryKeysLinq(dictionary, key);
            TestDictionaryEnumeration(dictionary, key, value);
            TestDictionaryCopyTo(dictionary, key, value);
        }

        /// <summary>
        /// Test dictionary insert/replace/delete.
        /// </summary>
        /// <typeparam name="TKey">Key type of the dictionary.</typeparam>
        /// <typeparam name="TValue">Value type of the dictionary.</typeparam>
        /// <param name="dictionary">The dictionary to test.</param>
        /// <param name="key">Key that is present in the dictionary.</param>
        /// <param name="value">Value associated with the key in the dictionary.</param>
        private static void TestBasicOperations<TKey, TValue>(PersistentDictionary<TKey, TValue> dictionary, TKey key, TValue value)
            where TKey : IComparable<TKey>
        {
            var kvp = new KeyValuePair<TKey, TValue>(key, value);

            // Add a record
            dictionary.Add(key, value);

            // Test PersistentDictionary.Add error handling
            try
            {
                dictionary.Add(key, value);
                Assert.Fail("Expected ArgumentException from Add");
            }
            catch (ArgumentException)
            {
                // Expected
            }

            // Overwrite a value
            dictionary[key] = value;

            // Retrieve a value
            Assert.AreEqual(value, dictionary[key], "Retrieve with [] failed");
            TValue t;
            Assert.IsTrue(dictionary.TryGetValue(key, out t), "TryGetValue({0}) failed", key);
            Assert.AreEqual(value, t, "TryGetValue({0}) returned the wrong value", key);

            // Clear and re-insert
            dictionary.Clear();
            Assert.AreEqual(0, dictionary.Count, "Dictionary is empty. Count is wrong");
            dictionary[key] = value;
            Assert.AreEqual(1, dictionary.Count, "Item was just inserted. Count is wrong");

            // Get the keys and values
            Assert.AreEqual(dictionary.Keys.First(), key, "Keys collection");
            Assert.AreEqual(dictionary.Values.First(), value, "Values collection");

            // Test PersistentDictionary.Contains (true)
            Assert.IsTrue(dictionary.ContainsKey(key), "Dictionary should have contained key {0}", key);
            Assert.IsTrue(dictionary.ContainsValue(value), "Dictionary should have contained value {0}", value);
            Assert.IsTrue(dictionary.Contains(kvp), "Dictionary should have contained <{0},{1}>", key, value);

            // Test PersistentDictionary.Remove
            Assert.IsTrue(dictionary.Remove(key), "Key {0} should exist, but removal failed", key);
            Assert.IsFalse(dictionary.Remove(key), "Key {0} doesn't exist, but removal succeeded", key);

            dictionary.Add(kvp);
            Assert.IsTrue(dictionary.Remove(kvp), "KeyValuePair <{0},{1}> should exist, but removal failed", kvp.Key, kvp.Value);
            Assert.IsFalse(dictionary.Remove(kvp), "KeyValuePair <{0},{1}> doesn't exist, but removal succeeded", kvp.Key, kvp.Value);

            // Test PersistentDictionary.Contains (false)
            Assert.IsFalse(dictionary.ContainsKey(key), "Dictionary should have contained key {0}", key);
            Assert.IsFalse(dictionary.ContainsValue(value), "Dictionary should have contained value {0}", value);
            Assert.IsFalse(dictionary.Contains(kvp), "Dictionary should have contained <{0},{1}>", key, value);
        }

        /// <summary>
        /// Test LINQ queries on the dictionary.
        /// </summary>
        /// <typeparam name="TKey">Key type of the dictionary.</typeparam>
        /// <typeparam name="TValue">Value type of the dictionary.</typeparam>
        /// <param name="dictionary">The dictionary to test.</param>
        /// <param name="key">Key that is present in the dictionary.</param>
        /// <param name="value">Value associated with the key in the dictionary.</param>
        private static void TestDictionaryLinq<TKey, TValue>(PersistentDictionary<TKey, TValue> dictionary, TKey key, TValue value)
            where TKey : IComparable<TKey>
        {
            var kvp = new KeyValuePair<TKey, TValue>(key, value);
            Assert.IsTrue(dictionary.Any(x => 0 == x.Key.CompareTo(key)), "Any == should have found {0}", key);
            Assert.IsTrue(dictionary.Any(x => 0 <= x.Key.CompareTo(key)), "Any <= should have found {0}", key);
            Assert.IsTrue(dictionary.Any(x => 0 >= x.Key.CompareTo(key)), "Any >= should have found {0}", key);
            Assert.IsTrue(dictionary.Any(x => !(0 != x.Key.CompareTo(key))), "Any !(!=) should have found {0}", key);

            var query = from x in dictionary where x.Key.CompareTo(key) == 0 select x.Value;
            Assert.AreEqual(value, query.Single(), "Where == failed");
            query = from x in dictionary where x.Key.CompareTo(key) == 0 select x.Value;
            Assert.AreEqual(value, query.Reverse().Single(), "Where == failed (reversed)");
            query = from x in dictionary where x.Key.CompareTo(key) <= 0 select x.Value;
            Assert.AreEqual(value, query.Single(), "Where <= failed");
            query = from x in dictionary where x.Key.CompareTo(key) >= 0 select x.Value;
            Assert.AreEqual(value, query.Single(), "Where >= failed");
            query = from x in dictionary where !(x.Key.CompareTo(key) != 0) select x.Value;
            Assert.AreEqual(value, query.Single(), "Where !(!=) failed");
            Assert.AreEqual(kvp, dictionary.Where(x => x.Key.CompareTo(key) >= 0).Reverse().Last(), "Where.Reverse.Last failed");

            Assert.AreEqual(kvp, dictionary.First(), "First");
            Assert.AreEqual(kvp, dictionary.First(x => x.Key.CompareTo(key) == 0), "First");
            Assert.AreEqual(kvp, dictionary.FirstOrDefault(), "FirstOrDefault");
            Assert.AreEqual(kvp, dictionary.FirstOrDefault(x => x.Key.CompareTo(key) == 0), "FirstOrDefault");
            Assert.AreEqual(kvp, dictionary.Last(), "Last");
            Assert.AreEqual(kvp, dictionary.Last(x => x.Key.CompareTo(key) == 0), "Last");
            Assert.AreEqual(kvp, dictionary.LastOrDefault(), "LastOrDefault");
            Assert.AreEqual(kvp, dictionary.LastOrDefault(x => x.Key.CompareTo(key) == 0), "LastOrDefault");
            Assert.AreEqual(kvp, dictionary.Single(), "Single");
            Assert.AreEqual(kvp, dictionary.Single(x => x.Key.CompareTo(key) == 0), "Single");
            Assert.AreEqual(kvp, dictionary.SingleOrDefault(), "SingleOrDefault");
            Assert.AreEqual(kvp, dictionary.SingleOrDefault(x => x.Key.CompareTo(key) == 0), "SingleOrDefault");

            Assert.AreEqual(1, dictionary.Count(x => x.Key.CompareTo(key) <= 0), "Count failed");
            Assert.AreEqual(0, dictionary.Count(x => x.Key.CompareTo(key) != 0), "Count failed");
        }

        /// <summary>
        /// Test LINQ queries on the dictionary's keys.
        /// </summary>
        /// <typeparam name="TKey">Key type of the dictionary.</typeparam>
        /// <typeparam name="TValue">Value type of the dictionary.</typeparam>
        /// <param name="dictionary">The dictionary to test.</param>
        /// <param name="key">Key that is present in the dictionary.</param>
        private static void TestDictionaryKeysLinq<TKey, TValue>(PersistentDictionary<TKey, TValue> dictionary, TKey key)
            where TKey : IComparable<TKey>
        {
            Assert.IsTrue(dictionary.Keys.Any(x => 0 == x.CompareTo(key)), "Any == should have found {0}", key);
            Assert.IsTrue(dictionary.Keys.Any(x => 0 <= x.CompareTo(key)), "Any <= should have found {0}", key);
            Assert.IsTrue(dictionary.Keys.Any(x => 0 >= x.CompareTo(key)), "Any >= should have found {0}", key);
            Assert.IsTrue(dictionary.Keys.Any(x => !(0 != x.CompareTo(key))), "Any !(!=) should have found {0}", key);

            var query = from x in dictionary.Keys where x.CompareTo(key) == 0 select x;
            Assert.AreEqual(key, query.Single(), "Where == failed");
            query = from x in dictionary.Keys where x.CompareTo(key) == 0 select x;
            Assert.AreEqual(key, query.Reverse().Single(), "Where == failed (reversed)");
            query = from x in dictionary.Keys where x.CompareTo(key) <= 0 select x;
            Assert.AreEqual(key, query.Single(), "Where <= failed");
            query = from x in dictionary.Keys where x.CompareTo(key) >= 0 select x;
            Assert.AreEqual(key, query.Single(), "Where >= failed");
            query = from x in dictionary.Keys where !(x.CompareTo(key) != 0) select x;
            Assert.AreEqual(key, query.Single(), "Where !(!=) failed");
            Assert.AreEqual(key, dictionary.Keys.Where(x => x.CompareTo(key) >= 0).Reverse().Last(), "Where.Reverse.Last failed");

            Assert.AreEqual(key, dictionary.Keys.First(), "First");
            Assert.AreEqual(key, dictionary.Keys.First(x => x.CompareTo(key) == 0), "First");
            Assert.AreEqual(key, dictionary.Keys.FirstOrDefault(), "FirstOrDefault");
            Assert.AreEqual(key, dictionary.Keys.FirstOrDefault(x => x.CompareTo(key) == 0), "FirstOrDefault");
            Assert.AreEqual(key, dictionary.Keys.Last(), "Last");
            Assert.AreEqual(key, dictionary.Keys.Last(x => x.CompareTo(key) == 0), "Last");
            Assert.AreEqual(key, dictionary.Keys.LastOrDefault(), "LastOrDefault");
            Assert.AreEqual(key, dictionary.Keys.LastOrDefault(x => x.CompareTo(key) == 0), "LastOrDefault");
            Assert.AreEqual(key, dictionary.Keys.Single(), "Single");
            Assert.AreEqual(key, dictionary.Keys.Single(x => x.CompareTo(key) == 0), "Single");
            Assert.AreEqual(key, dictionary.Keys.SingleOrDefault(), "SingleOrDefault");
            Assert.AreEqual(key, dictionary.Keys.SingleOrDefault(x => x.CompareTo(key) == 0), "SingleOrDefault");

            Assert.AreEqual(key, dictionary.Keys.Min(), "Min");
            Assert.AreEqual(key, dictionary.Keys.Max(), "Max");

            Assert.AreEqual(1, dictionary.Keys.Count(x => x.CompareTo(key) <= 0), "Count failed");
            Assert.AreEqual(0, dictionary.Keys.Count(x => x.CompareTo(key) != 0), "Count failed");
        }

        /// <summary>
        /// Test dictionary enumeration.
        /// </summary>
        /// <typeparam name="TKey">Key type of the dictionary.</typeparam>
        /// <typeparam name="TValue">Value type of the dictionary.</typeparam>
        /// <param name="dictionary">The dictionary to test.</param>
        /// <param name="key">Key that is present in the dictionary.</param>
        /// <param name="value">Value associated with the key in the dictionary.</param>
        private static void TestDictionaryEnumeration<TKey, TValue>(PersistentDictionary<TKey, TValue> dictionary, TKey key, TValue value)
            where TKey : IComparable<TKey>
        {
            var kvp = new KeyValuePair<TKey, TValue>(key, value);
            IEnumerable enumerator = dictionary.Where(x => true);
            foreach (object o in enumerator)
            {
                Assert.AreEqual(o, kvp, "Dictionary query enumeration");
            }

            enumerator = dictionary.Keys.Where(x => true);
            foreach (object o in enumerator)
            {
                Assert.AreEqual(o, kvp.Key, "Dictionary query enumeration");
            }

            foreach (KeyValuePair<TKey, TValue> o in dictionary.Where(x => true).Reverse())
            {
                Assert.AreEqual(o, kvp, "Dictionary query enumeration");
            }

            foreach (object o in (IEnumerable)dictionary)
            {
                Assert.AreEqual(o, kvp, "Dictionary enumeration");
            }

            foreach (KeyValuePair<TKey, TValue> a in dictionary.Reverse())
            {
                Assert.AreEqual(a, kvp, "Reverse dictionary enumeration");
            }

            foreach (object o in (IEnumerable)dictionary.Keys)
            {
                Assert.AreEqual(o, key, "Key enumeration");
            }

            foreach (TKey k in dictionary.Keys.Reverse())
            {
                Assert.AreEqual(k, key, "Reverse key enumeration");
            }

            foreach (TValue v in dictionary.Values.Reverse())
            {
                Assert.AreEqual(v, value, "Value enumeration");
            }

            foreach (TKey k in ((IDictionary<TKey, TValue>)dictionary).Keys)
            {
                Assert.AreEqual(k, key, "Key enumeration");
            }

            foreach (TValue v in ((IDictionary<TKey, TValue>)dictionary).Values)
            {
                Assert.AreEqual(v, value, "Value enumeration");
            }
        }

        /// <summary>
        /// Test CopyTo.
        /// </summary>
        /// <typeparam name="TKey">Key type of the dictionary.</typeparam>
        /// <typeparam name="TValue">Value type of the dictionary.</typeparam>
        /// <param name="dictionary">The dictionary to test.</param>
        /// <param name="key">Key that is present in the dictionary.</param>
        /// <param name="value">Value associated with the key in the dictionary.</param>
        private static void TestDictionaryCopyTo<TKey, TValue>(PersistentDictionary<TKey, TValue> dictionary, TKey key, TValue value)
            where TKey : IComparable<TKey>
        {
            var kvp = new KeyValuePair<TKey, TValue>(key, value);
            var elements = new KeyValuePair<TKey, TValue>[1];
            dictionary.CopyTo(elements, 0);
            Assert.AreEqual(kvp, elements[0], "CopyTo failed");

            var keys = new TKey[1];
            dictionary.Keys.CopyTo(keys, 0);
            Assert.AreEqual(key, keys[0], "Keys.CopyTo failed");

            var values = new TValue[1];
            dictionary.Values.CopyTo(values, 0);
            Assert.AreEqual(value, values[0], "values.CopyTo failed");
        }

        /// <summary>
        /// Create and modify a generic dictionary.
        /// </summary>
        /// <typeparam name="TKey">The key type for the dictionary.</typeparam>
        /// <typeparam name="TValue">The value type for the dictionary.</typeparam>
        private static void TestNullableGenericDictionary<TKey, TValue>() where TKey : IComparable<TKey> where TValue : struct
        {
            // Test the version with a nullable value. Use both null and non-null values.
            using (var dictionary = new PersistentDictionary<TKey, TValue?>(DictionaryPath))
            {
                RunDictionaryTests(dictionary, default(TKey), default(TValue));
                dictionary.Clear();
                RunDictionaryTests(dictionary, default(TKey), default(TValue?));
            }

            PersistentDictionaryFile.DeleteFiles(DictionaryPath);

            using (var dictionary = new PersistentDictionary<TKey, TValue>(DictionaryPath))
            {
                TKey key = default(TKey);
                TValue value = default(TValue);
                RunDictionaryTests(dictionary, key, value);
            }

            // Reopen the database
            Dictionary<TKey, TValue> temp;
            using (var dictionary = new PersistentDictionary<TKey, TValue>(DictionaryPath))
            {
                temp = new Dictionary<TKey, TValue>(dictionary);
            }

            // Delete the database
            Assert.IsTrue(PersistentDictionaryFile.Exists(DictionaryPath), "Dictionary should exist");
            PersistentDictionaryFile.DeleteFiles(DictionaryPath);
            Assert.IsFalse(PersistentDictionaryFile.Exists(DictionaryPath), "Dictionary should have been deleted");

            // Recreate the database
            using (var dictionary = new PersistentDictionary<TKey, TValue>(temp, DictionaryPath))
            {
                DictionaryAssert.AreEqual(temp, dictionary);
            }

            PersistentDictionaryFile.DeleteFiles(DictionaryPath);
        }
    }
}
