// --------------------------------------------------------------------------------------------------------------------
// <copyright file="SortedDictionaryComparisonTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Compare a PersistentDictionary against a generic SortedDictionary.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Compare a PersistentDictionary against a generic SortedDictionary.
    /// </summary>
    [TestClass]
    public class SortedDictionaryComparisonTests
    {
        /// <summary>
        /// Where the dictionary will be located.
        /// </summary>
        private const string DictionaryLocation = "SortedDictionaryComparisonFixture";

        /// <summary>
        /// A generic sorted dictionary that we will use as the oracle.
        /// </summary>
        private SortedDictionary<string, string> expected;

        /// <summary>
        /// The dictionary we are testing.
        /// </summary>
        private PersistentDictionary<string, string> actual;

        /// <summary>
        /// Test initialization.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.expected = new SortedDictionary<string, string>();
            this.actual = new PersistentDictionary<string, string>(DictionaryLocation);
        }

        /// <summary>
        /// Cleanup after the test.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            this.actual.Dispose();
            if (Directory.Exists(DictionaryLocation))
            {
                Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);
            }
        }

        /// <summary>
        /// Compare two empty dictionaries.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestEmptyDictionary()
        {
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Insert one item into the dictionary.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestInsert()
        {
            this.expected["foo"] = this.actual["foo"] = "1";
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Replace an item.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestReplace()
        {
            this.expected["foo"] = this.actual["foo"] = "1";
            this.expected["foo"] = this.actual["foo"] = "2";
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Delete an item.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestDelete()
        {
            this.expected["foo"] = this.actual["foo"] = "1";
            this.expected["bar"] = this.actual["bar"] = "2";
            this.expected.Remove("foo");
            Assert.IsTrue(this.actual.Remove("foo"));
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Insert an item into the dictionary.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestAddItem()
        {
            var item = new KeyValuePair<string, string>("thekey", "thevalue");
            ((ICollection<KeyValuePair<string, string>>)this.expected).Add(item);
            this.actual.Add(item);
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Insert an item into the dictionary and remove it.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestRemoveItem()
        {
            var item = new KeyValuePair<string, string>("thekey", "thevalue");
            this.expected.Add(item.Key, item.Value);
            this.actual.Add(item.Key, item.Value);
            ((ICollection<KeyValuePair<string, string>>)this.expected).Remove(item);
            Assert.IsTrue(this.actual.Remove(item));
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Insert several items into the dictionary.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestAdds()
        {
            for (int i = 0; i < 10; ++i)
            {
                this.expected.Add(i.ToString(), i.ToString());
                this.actual.Add(i.ToString(), i.ToString());
            }

            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Clear an empty dictionary.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestClearEmptyDictionary()
        {
            this.expected.Clear();
            this.actual.Clear();
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Clear the dictionary.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestClear()
        {
            for (int i = 7; i >= 0; --i)
            {
                this.expected.Add(i.ToString(), i.ToString());
                this.actual.Add(i.ToString(), i.ToString());
            }

            this.expected.Clear();
            this.actual.Clear();
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Clear the dictionary twice.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestClearTwice()
        {
            this.expected["foo"] = this.actual["foo"] = "!";

            this.expected.Clear();
            this.actual.Clear();
            this.expected.Clear();
            this.actual.Clear();
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Store a null value in the dictionary.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestNullValue()
        {
            this.expected["a"] = this.actual["a"] = null;
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Close and reopen the database.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestCloseAndReopen()
        {
            var rand = new Random();
            for (int i = 0; i < 100; ++i)
            {
                string k = rand.Next().ToString();
                string v = rand.NextDouble().ToString();
                this.expected.Add(k, v);
                this.actual.Add(k, v);
            }

            this.actual.Dispose();
            this.actual = new PersistentDictionary<string, string>(DictionaryLocation);
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }

        /// <summary>
        /// Close and delete the database.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestCloseAndDelete()
        {
            var rand = new Random();
            for (int i = 0; i < 64; ++i)
            {
                string k = rand.NextDouble().ToString();
                string v = rand.Next().ToString();
                this.expected.Add(k, v);
                this.actual.Add(k, v);
            }

            this.actual.Dispose();
            PersistentDictionaryFile.DeleteFiles(DictionaryLocation);

            // Deleting the files clears the dictionary
            this.expected.Clear();

            this.actual = new PersistentDictionary<string, string>(DictionaryLocation);
            DictionaryAssert.AreSortedAndEqual(this.expected, this.actual);
        }
    }
}
