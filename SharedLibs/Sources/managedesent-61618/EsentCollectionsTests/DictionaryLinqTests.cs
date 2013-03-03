// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DictionaryLinqTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Basic PersistentDictionary tests.
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
    /// Test the PersistentDictionary.
    /// </summary>
    [TestClass]
    public class DictionaryLinqTests
    {
        /// <summary>
        /// Where the dictionary will be located.
        /// </summary>
        private const string DictionaryLocation = "DictionaryLinqFixture";

        /// <summary>
        /// Test dictionary.
        /// </summary>
        private readonly IDictionary<int, string> testDictionary1 = new SortedDictionary<int, string>
        {
            { 0, "alpha" },
            { 1, "foo" },
            { 2, "bar" },
            { 3, "baz" },
            { 4, "qux" },
            { 5, "xyzzy" },
            { 6, "omega" },
        };

        /// <summary>
        /// Test dictionary.
        /// </summary>
        private readonly IDictionary<string, int> testDictionary3 = new SortedDictionary<string, int>
        {
            { "a", 1 },
            { "alpha", 2 },
            { "apple", 3 },
            { "b", 4 },
            { "bing", 6 },
            { "biing", 7 },
            { "biiing", 8 },
            { "bravo", 9 },
            { "c", 10 },
            { "c#", 11 },
            { "c++", 12 },
            { "d", 13 },
            { "delta", 14 },
            { "decimal", 15 },
            { "e", 16 },
            { "echo", 17 },
            { "f", 18 },
            { "g", 19 },
        };

        /// <summary>
        /// Test dictionary.
        /// </summary>
        private IDictionary<DateTime, Guid> testDictionary2;

        /// <summary>
        /// Trace listener. Added to capture PersistentDictionary traces.
        /// </summary>
        private TraceListener traceListener;

        /// <summary>
        /// Test initialization.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.testDictionary2 = new SortedDictionary<DateTime, Guid>();
            var entries = from x in Enumerable.Range(0, 100)
                          select
                              new KeyValuePair<DateTime, Guid>(
                              DateTime.UtcNow + TimeSpan.FromSeconds(x), Guid.NewGuid());
            foreach (KeyValuePair<DateTime, Guid> entry in entries)
            {
                this.testDictionary2.Add(entry);
            }

            this.traceListener = new ConsoleTraceListener();
            Trace.Listeners.Add(this.traceListener);
        }

        /// <summary>
        /// Cleanup after the test.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            Trace.Listeners.Remove(this.traceListener);
            Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);
        }

        #region Null Expression Tree tests

        /// <summary>
        /// Verify that PersistentDictionary.Where throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Where throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyWhereThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Where(null);
            }            
        }

        /// <summary>
        /// Verify that PersistentDictionary.Any throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Any throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyAnyThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Any(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.First throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.First throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyFirstThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.First(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.FirstOrDefault throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.FirstOrDefault throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyFirstOrDefaultThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.FirstOrDefault(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Last throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Last throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyLastThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Last(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.LastOrDefault throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.LastOrDefault throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyLastOrDefaultThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.LastOrDefault(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Count throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Count throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyCountThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Count(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Single throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Single throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifySingleThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Single(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.SingleOrDefault throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.SingleOrDefault throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifySingleOrDefaultThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.SingleOrDefault(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Keys.Where throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Keys.Where throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeysWhereThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Keys.Where(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Keys.Any throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Keys.Any throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeysAnyThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Keys.Any(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Keys.First throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Keys.First throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeysFirstThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Keys.First(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Keys.FirstOrDefault throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Keys.FirstOrDefault throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeysFirstOrDefaultThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Keys.FirstOrDefault(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Keys.Last throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Keys.Last throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeysLastThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Keys.Last(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Keys.LastOrDefault throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Keys.LastOrDefault throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeysLastOrDefaultThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Keys.LastOrDefault(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Keys.Count throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Keys.Count throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeysCountThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Keys.Count(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Keys.Single throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Keys.Single throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeysSingleThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Keys.Single(null);
            }
        }

        /// <summary>
        /// Verify that PersistentDictionary.Keys.SingleOrDefault throws an exception when the expression is null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that PersistentDictionary.Keys.SingleOrDefault throws an exception when the expression is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeysSingleOrDefaultThrowsExceptionWhenExpressionIsNull()
        {
            using (var persistentDictionary = new PersistentDictionary<Guid, Guid>(DictionaryLocation))
            {
                persistentDictionary.Keys.SingleOrDefault(null);
            }
        }

        #endregion

        #region Any

        /// <summary>
        /// Test the LINQ Any operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Any operator when records are found")]
        [Priority(2)]
        public void TestLinqAnyTrue()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                Assert.IsTrue(persistentDictionary.Any(x => x.Key < 5));
            }
        }

        /// <summary>
        /// Test the LINQ Any operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Any operator when records are not found")]
        [Priority(2)]
        public void TestLinqAnyNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                Assert.IsFalse(persistentDictionary.Any(x => x.Key < 0));
                Assert.IsFalse(persistentDictionary.Any(x => x.Key > 6));
                Assert.IsFalse(persistentDictionary.Any(x => false));
            }
        }

        /// <summary>
        /// Test the LINQ Any operator on the key collection when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Any operator when records are found")]
        [Priority(2)]
        public void TestLinqKeysAnyTrue()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                Assert.IsTrue(persistentDictionary.Keys.Any(x => x < 5));
            }
        }

        /// <summary>
        /// Test the LINQ Any operator on the key collection when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Any operator when records are not found")]
        [Priority(2)]
        public void TestLinqKeysAnyNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                Assert.IsFalse(persistentDictionary.Keys.Any(x => x < 0));
                Assert.IsFalse(persistentDictionary.Keys.Any(x => x > 6));
                Assert.IsFalse(persistentDictionary.Keys.Any(x => false));
            }
        }

        #endregion

        #region Min/Max

        /// <summary>
        /// Test the LINQ Min operator on the keys.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Min operator on the keys")]
        [Priority(2)]
        public void TestLinqMinKey()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.Min();
                var actual = persistentDictionary.Keys.Min();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ Max operator on the keys.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Max operator on the keys")]
        [Priority(2)]
        public void TestLinqMaxKey()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.Max();
                var actual = persistentDictionary.Keys.Max();
                Assert.AreEqual(expected, actual);
            }
        }

        #endregion

        #region First/FirstOrDefault

        /// <summary>
        /// Test the LINQ First operator.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ First operator")]
        [Priority(2)]
        public void TestLinqFirst()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.First();
                var actual = persistentDictionary.First();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ First operator on the keys.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ First operator on the keys")]
        [Priority(2)]
        public void TestLinqFirstKey()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.First();
                var actual = persistentDictionary.Keys.First();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ FirstOrDefault operator.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ FirstOrDefault operator")]
        [Priority(2)]
        public void TestLinqFirstOrDefault()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.FirstOrDefault();
                var actual = persistentDictionary.FirstOrDefault();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ FirstOrDefault operator on the keys.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ FirstOrDefault operator on the keys")]
        [Priority(2)]
        public void TestLinqFirstOrDefaultKey()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.FirstOrDefault();
                var actual = persistentDictionary.Keys.FirstOrDefault();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ First operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ First operator when records are found")]
        [Priority(2)]
        public void TestLinqFirstFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.First(x => x.Key >= 3);
                var actual = persistentDictionary.First(x => x.Key >= 3);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ First operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ First operator when records are not found")]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinqFirstNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = persistentDictionary.First(x => x.Key >= 99);
            }
        }

        /// <summary>
        /// Test the LINQ FirstOrDefault operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ FirstOrDefault operator when records are found")]
        [Priority(2)]
        public void TestLinqFirstOrDefaultFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.FirstOrDefault(x => x.Key >= 3);
                var actual = persistentDictionary.FirstOrDefault(x => x.Key >= 3);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ FirstOrDefault operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ FirstOrDefault operator when records are not found")]
        [Priority(2)]
        public void TestLinqFirstOrDefaultNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.FirstOrDefault(x => x.Key > 99);
                var actual = persistentDictionary.FirstOrDefault(x => x.Key > 99);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ First operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ First operator when records are found")]
        [Priority(2)]
        public void TestLinqKeysFirstFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.First(x => x >= 3);
                var actual = persistentDictionary.Keys.First(x => x >= 3);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ First operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ First operator when records are not found")]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinqKeysFirstNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = persistentDictionary.Keys.First(x => x >= 99);
            }
        }

        /// <summary>
        /// Test the LINQ FirstOrDefault operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ FirstOrDefault operator when records are found")]
        [Priority(2)]
        public void TestLinqKeysFirstOrDefaultFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.FirstOrDefault(x => x >= 3);
                var actual = persistentDictionary.Keys.FirstOrDefault(x => x >= 3);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ FirstOrDefault operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ FirstOrDefault operator when records are not found")]
        [Priority(2)]
        public void TestLinqKeysFirstOrDefaultNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.FirstOrDefault(x => x > 99);
                var actual = persistentDictionary.Keys.FirstOrDefault(x => x > 99);
                Assert.AreEqual(expected, actual);
            }
        }

        #endregion

        #region Single/SingleOrDefault

        /// <summary>
        /// Test the LINQ Single operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Single operator when records are found")]
        [Priority(2)]
        public void TestLinqSingleFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Single(x => x.Key == 3);
                var actual = persistentDictionary.Single(x => x.Key == 3);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ Single operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Single operator when records are not found")]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinqSingleNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = persistentDictionary.Single(x => x.Key >= 99);
            }
        }

        /// <summary>
        /// Test the LINQ Single operator when too many records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Single operator when too many records are found")]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinqSingleTooManyRecords()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = persistentDictionary.Single(x => x.Key >= 0);
            }
        }

        /// <summary>
        /// Test the LINQ SingleOrDefault operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ SingleOrDefault operator when records are found")]
        [Priority(2)]
        public void TestLinqSingleOrDefaultFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.SingleOrDefault(x => x.Key == 4);
                var actual = persistentDictionary.SingleOrDefault(x => x.Key == 4);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ SingleOrDefault operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ SingleOrDefault operator when records are not found")]
        [Priority(2)]
        public void TestLinqSingleOrDefaultNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.SingleOrDefault(x => x.Key > 99);
                var actual = persistentDictionary.SingleOrDefault(x => x.Key > 99);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ SingleOrDefault operator when too many records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ SingleOrDefault operator when too many records are found")]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinqSingleOrDefaultTooManyRecords()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = persistentDictionary.SingleOrDefault(x => x.Key >= 0);
            }
        }

        /// <summary>
        /// Test the LINQ Single operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Single operator when records are found")]
        [Priority(2)]
        public void TestLinqKeysSingleFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.Single(x => x == 3);
                var actual = persistentDictionary.Keys.Single(x => x == 3);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ Single operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Single operator when records are not found")]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinqKeysSingleNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = persistentDictionary.Keys.Single(x => x >= 99);
            }
        }

        /// <summary>
        /// Test the LINQ Single operator when too many records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Single operator when too many records are found")]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinqKeysSingleTooManyRecords()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = persistentDictionary.Keys.Single(x => x >= 0);
            }
        }

        /// <summary>
        /// Test the LINQ SingleOrDefault operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ SingleOrDefault operator when records are found")]
        [Priority(2)]
        public void TestLinqKeysSingleOrDefaultFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.SingleOrDefault(x => x == 4);
                var actual = persistentDictionary.Keys.SingleOrDefault(x => x == 4);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ SingleOrDefault operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ SingleOrDefault operator when records are not found")]
        [Priority(2)]
        public void TestLinqKeysSingleOrDefaultNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.SingleOrDefault(x => x > 99);
                var actual = persistentDictionary.Keys.SingleOrDefault(x => x > 99);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ SingleOrDefault operator when too many records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ SingleOrDefault operator when too many records are found")]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinqKeysSingleOrDefaultTooManyRecords()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = persistentDictionary.Keys.SingleOrDefault(x => x >= 0);
            }
        }

        #endregion

        #region Last/LastOrDefault

        /// <summary>
        /// Test the LINQ Last operator.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Last operator")]
        [Priority(2)]
        public void TestLinqLast()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Last();
                var actual = persistentDictionary.Last();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ Last operator on the keys.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Last operator on the keys")]
        [Priority(2)]
        public void TestLinqLastKey()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.Last();
                var actual = persistentDictionary.Keys.Last();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ LastOrDefault operator.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ LastOrDefault operator")]
        [Priority(2)]
        public void TestLinqLastOrDefault()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.LastOrDefault();
                var actual = persistentDictionary.LastOrDefault();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ LastOrDefault operator on the keys.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ LastOrDefault operator on the keys")]
        [Priority(2)]
        public void TestLinqLastOrDefaultKey()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.LastOrDefault();
                var actual = persistentDictionary.Keys.LastOrDefault();
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ Last operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Last operator when records are found")]
        [Priority(2)]
        public void TestLinqLastFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Last(x => x.Key < 4);
                var actual = persistentDictionary.Last(x => x.Key < 4);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ Last operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Last operator when records are not found")]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinqLastNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = persistentDictionary.Last(x => x.Key <= -99);
            }
        }

        /// <summary>
        /// Test the LINQ LastOrDefault operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ LastOrDefault operator when records are found")]
        [Priority(2)]
        public void TestLinqLastOrDefaultFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.LastOrDefault(x => x.Key < 4);
                var actual = persistentDictionary.LastOrDefault(x => x.Key < 4);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ LastOrDefault operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ LastOrDefault operator when records are not found")]
        [Priority(2)]
        public void TestLinqLastOrDefaultNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.LastOrDefault(x => x.Key < -99);
                var actual = persistentDictionary.LastOrDefault(x => x.Key < -99);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ Last operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Last operator when records are found")]
        [Priority(2)]
        public void TestLinqKeysLastFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.Last(x => x < 4);
                var actual = persistentDictionary.Keys.Last(x => x < 4);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ Last operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Last operator when records are not found")]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestLinqKeysLastNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = persistentDictionary.Keys.Last(x => x <= -99);
            }
        }

        /// <summary>
        /// Test the LINQ LastOrDefault operator when records are found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ LastOrDefault operator when records are found")]
        [Priority(2)]
        public void TestLinqKeysLastOrDefaultFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.LastOrDefault(x => x < 4);
                var actual = persistentDictionary.Keys.LastOrDefault(x => x < 4);
                Assert.AreEqual(expected, actual);
            }
        }

        /// <summary>
        /// Test the LINQ LastOrDefault operator when records are not found.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ LastOrDefault operator when records are not found")]
        [Priority(2)]
        public void TestLinqKeysLastOrDefaultNotFound()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.LastOrDefault(x => x < -99);
                var actual = persistentDictionary.Keys.LastOrDefault(x => x < -99);
                Assert.AreEqual(expected, actual);
            }
        }

        #endregion

        #region Count

        /// <summary>
        /// Test the LINQ Count operator
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Count operator")]
        [Priority(2)]
        public void TestLinqCount()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                Assert.AreEqual(this.testDictionary1.Count(), persistentDictionary.Count());
            }
        }

        /// <summary>
        /// Test the LINQ Count operator with a query
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Count operator with a query")]
        [Priority(2)]
        public void TestLinqCountQuery()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                Assert.AreEqual(
                    this.testDictionary1.Count(x => x.Key > 4),
                    persistentDictionary.Count(x => x.Key > 4));
            }
        }

        /// <summary>
        /// Test the LINQ Count operator with a query that returns 0 records
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Count operator with a query that return 0 records")]
        [Priority(2)]
        public void TestLinqCountQueryZeroRecords()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                Assert.AreEqual(0, persistentDictionary.Count(x => x.Key > 9999));
            }
        }

        /// <summary>
        /// Test the LINQ Count operator
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Count operator")]
        [Priority(2)]
        public void TestLinqKeysCount()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                Assert.AreEqual(this.testDictionary1.Keys.Count(), persistentDictionary.Keys.Count());
            }
        }

        /// <summary>
        /// Test the LINQ Count operator with a query
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Count operator with a query")]
        [Priority(2)]
        public void TestLinqKeysCountQuery()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                Assert.AreEqual(
                    this.testDictionary1.Keys.Count(x => x > 4),
                    persistentDictionary.Keys.Count(x => x > 4));
            }
        }

        /// <summary>
        /// Test the LINQ Count operator with a query that returns 0 records
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Count operator with a query that return 0 records")]
        [Priority(2)]
        public void TestLinqKeysCountQueryZeroRecords()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                Assert.AreEqual(0, persistentDictionary.Keys.Count(x => x > 9999));
            }
        }

        #endregion

        #region Reverse

        /// <summary>
        /// Test the LINQ Reverse operator.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Reverse operator")]
        [Priority(2)]
        public void TestLinqReverse()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Reverse();
                var actual = persistentDictionary.Reverse();
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Test the LINQ Reverse operator.
        /// </summary>
        [TestMethod]
        [Description("Test reversing a LINQ query")]
        [Priority(2)]
        public void TestLinqReverseQuery()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Where(x => x.Key < 5 && x.Key > 2 && x.Key != 4).Reverse();
                var actual = persistentDictionary.Where(x => x.Key < 5 && x.Key > 2 && x.Key != 4).Reverse();
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Test using the LINQ Reverse operator twice.
        /// </summary>
        [TestMethod]
        [Description("Test double reversing a LINQ query")]
        [Priority(2)]
        public void TestLinqDoubleReverseQuery()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Where(x => x.Key < 5 && x.Key > -1 && x.Key != 4).Reverse();
                var actual = persistentDictionary.Where(x => x.Key < 5 && x.Key > -1 && x.Key != 4).Reverse();
                EnumerableAssert.AreEqual(expected.Reverse(), actual.Reverse(), null);
            }
        }

        /// <summary>
        /// Test the LINQ Reverse operator.
        /// </summary>
        [TestMethod]
        [Description("Test the LINQ Reverse operator")]
        [Priority(2)]
        public void TestLinqKeysReverse()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.Reverse();
                var actual = persistentDictionary.Keys.Reverse();
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Test the LINQ Reverse operator.
        /// </summary>
        [TestMethod]
        [Description("Test reversing a LINQ query")]
        [Priority(2)]
        public void TestLinqKeysReverseQuery()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.Where(x => x < 5 && x > 2 && x != 4).Reverse();
                var actual = persistentDictionary.Keys.Where(x => x < 5 && x > 2 && x != 4).Reverse();
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Test using the LINQ Reverse operator twice.
        /// </summary>
        [TestMethod]
        [Description("Test double reversing a LINQ query")]
        [Priority(2)]
        public void TestLinqKeysDoubleReverseQuery()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = this.testDictionary1.Keys.Where(x => x < 5 && x > -1 && x != 4).Reverse();
                var actual = persistentDictionary.Keys.Where(x => x < 5 && x > -1 && x != 4).Reverse();
                EnumerableAssert.AreEqual(expected.Reverse(), actual.Reverse(), null);
            }
        }

        #endregion

        /// <summary>
        /// Linq test 1.
        /// </summary>
        [TestMethod]
        [Description("Linq test 1")]
        [Priority(2)]
        public void LinqTest1()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = from x in this.testDictionary1 where x.Key > 3 select x.Value;
                var actual = from x in persistentDictionary where x.Key > 3 select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 2.
        /// </summary>
        [TestMethod]
        [Description("Linq test 2")]
        [Priority(2)]
        public void LinqTest2()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = from x in this.testDictionary1 where x.Key >= 1 && x.Key <= 5 && x.Value.StartsWith("b") select x.Value;
                var actual = from x in persistentDictionary where x.Key >= 1 && x.Key <= 5 && x.Value.StartsWith("b") select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 3.
        /// </summary>
        [TestMethod]
        [Description("Linq test 3")]
        [Priority(2)]
        public void LinqTest3()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = from x in this.testDictionary1 where x.Value.Length == 3 select x.Value;
                var actual = from x in persistentDictionary where x.Value.Length == 3 select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 4.
        /// </summary>
        [TestMethod]
        [Description("Linq test 4")]
        [Priority(2)]
        public void LinqTest4()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary2))
            {
                DateTime time = DateTime.UtcNow + TimeSpan.FromSeconds(2);
                var expected = from x in this.testDictionary2 where x.Key > time select x;
                var actual = from x in persistentDictionary where x.Key > time select x;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 5.
        /// </summary>
        [TestMethod]
        [Description("Linq test 5")]
        [Priority(2)]
        public void LinqTest5()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = from x in this.testDictionary1 where !(x.Key < 1 || x.Key > 5) && (0 == x.Key % 2) select x.Value;
                var actual = from x in persistentDictionary where !(x.Key < 1 || x.Key > 5) && (0 == x.Key % 2) select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 6.
        /// </summary>
        [TestMethod]
        [Description("Linq test 6")]
        [Priority(2)]
        public void LinqTest6()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = from x in this.testDictionary1
                               where !(x.Key < 1 || x.Key > 5) && (x.Key > 3 || x.Key > 2)
                               select x.Value;
                var actual = from x in persistentDictionary
                             where !(x.Key < 1 || x.Key > 5) && (x.Key > 3 || x.Key > 2)
                             select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 7.
        /// </summary>
        [TestMethod]
        [Description("Linq test 7")]
        [Priority(2)]
        public void LinqTest7()
        {
            var rand = new Random();
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                for (int i = 0; i < 128; ++i)
                {
                    int min = rand.Next(-1, 8);
                    int max = rand.Next(-1, 8);

                    var expected = from x in this.testDictionary1 where x.Key >= min && x.Key <= max select x.Value;
                    var actual = from x in persistentDictionary where x.Key >= min && x.Key <= max select x.Value;
                    EnumerableAssert.AreEqual(expected, actual, null);
                }
            }
        }

        /// <summary>
        /// Linq test 8.
        /// </summary>
        [TestMethod]
        [Description("Linq test 8")]
        [Priority(2)]
        public void LinqTest8()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary3))
            {
                var expected = from x in this.testDictionary3 where x.Key.StartsWith("b") select x.Value;
                var actual = from x in persistentDictionary where x.Key.StartsWith("b") select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 9.
        /// </summary>
        [TestMethod]
        [Description("Linq test 9")]
        [Priority(2)]
        public void LinqTest9()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary3))
            {
                var expected = from x in this.testDictionary3 where x.Key.StartsWith("de") || x.Key.StartsWith("bi") select x.Value;
                var actual = from x in persistentDictionary where x.Key.StartsWith("de") || x.Key.StartsWith("bi") select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 10.
        /// </summary>
        [TestMethod]
        [Description("Linq test 10")]
        [Priority(2)]
        public void LinqTest10()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary3))
            {
                var expected = from x in this.testDictionary3 where (x.Key.CompareTo("a") > 0 && x.Key.CompareTo("c") <= 0) || x.Key.StartsWith("c") select x.Value;
                var actual = from x in persistentDictionary where (x.Key.CompareTo("a") > 0 && x.Key.CompareTo("c") <= 0) || x.Key.StartsWith("c") select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 11. Make sure the Enumerable is live -- the key range shouldn't be
        /// evaluated until the query is performed.
        /// </summary>
        [TestMethod]
        [Description("Linq test 11")]
        [Priority(2)]
        public void LinqTest11()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                int? min = 3;
                int? max = 6;

                var expected = from x in this.testDictionary1 where x.Key >= min && x.Key <= max select x;
                var actual = from x in persistentDictionary where x.Key >= min && x.Key <= max select x;
                EnumerableAssert.AreEqual(expected, actual, null);

                min = 1;
                max = 7;
                EnumerableAssert.AreEqual(expected, actual, null);

                min = 4;
                max = 5;
                EnumerableAssert.AreEqual(expected, actual, null);

                expected = expected.Reverse();
                actual = actual.Reverse();

                min = -1;
                max = 99;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 12.
        /// </summary>
        [TestMethod]
        [Description("Linq test 12")]
        [Priority(2)]
        public void LinqTest12()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary3))
            {
                var expected = from x in this.testDictionary3 where String.Equals(x.Key, "b") || String.Equals("c", x.Key) select x.Value;
                var actual = from x in persistentDictionary where String.Equals(x.Key, "b") || String.Equals("c", x.Key) select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 13.
        /// </summary>
        [TestMethod]
        [Description("Linq test 13")]
        [Priority(2)]
        public void LinqTest13()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary3))
            {
                var expected = from x in this.testDictionary3 where x.Key.Equals("b") || x.Key.Equals("c") select x.Value;
                var actual = from x in persistentDictionary where x.Key.Equals("b") || x.Key.Equals("c") select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 14.
        /// </summary>
        [TestMethod]
        [Description("Linq test 14")]
        [Priority(2)]
        public void LinqTest14()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary3))
            {
                var expected = from x in this.testDictionary3 where String.Compare(x.Key, "d") < 0 && String.Compare("b", x.Key) <= 0 select x.Value;
                var actual = from x in persistentDictionary where String.Compare(x.Key, "d") < 0 && String.Compare("b", x.Key) <= 0 select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 15.
        /// </summary>
        [TestMethod]
        [Description("Linq test 15")]
        [Priority(2)]
        public void LinqTest15()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = from x in this.testDictionary1 where x.Key.Equals(6 / 2) select x.Value;
                var actual = from x in persistentDictionary where x.Key.Equals(6 / 2) select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 16.
        /// </summary>
        [TestMethod]
        [Description("Linq test 16")]
        [Priority(2)]
        public void LinqTest16()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary2))
            {
                Assert.AreEqual(this.testDictionary2.Reverse().Last(), persistentDictionary.Reverse().Last(), null);
            }
        }

        /// <summary>
        /// Linq test 17.
        /// </summary>
        [TestMethod]
        [Description("Linq test 17")]
        [Priority(2)]
        public void LinqTest17()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = from x in this.testDictionary1.Keys where x.Equals(5 - 3) || x == 6 / 2 || 2 * 2 == x select x;
                var actual = from x in persistentDictionary.Keys where x.Equals(5 - 3) || x == 6 / 2 || 2 * 2 == x select x;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 18.
        /// </summary>
        [TestMethod]
        [Description("Linq test 18")]
        [Priority(2)]
        public void LinqTest18()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary1))
            {
                var expected = from x in this.testDictionary1 where x.Key.Equals("not a number") select x.Value;
                var actual = from x in persistentDictionary where x.Key.Equals("not a number") select x.Value;
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Linq test 19.
        /// </summary>
        [TestMethod]
        [Description("Linq test 19")]
        [Priority(2)]
        public void LinqTest19()
        {
            using (var persistentDictionary = CloneDictionary(this.testDictionary2))
            {
                Guid g = Guid.NewGuid();
                var expected = this.testDictionary2.Where(x => x.Value == g).Reverse();
                var actual = persistentDictionary.Where(x => x.Value == g).Reverse();
                EnumerableAssert.AreEqual(expected, actual, null);
            }
        }

        /// <summary>
        /// Create a PersistentDictionary that is a copy of another dictionary.
        /// </summary>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary value.</typeparam>
        /// <param name="source">The dictionary to clone.</param>
        /// <returns>A persistent dictionary cloned from the input.</returns>
        private static PersistentDictionary<TKey, TValue> CloneDictionary<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> source) where TKey : IComparable<TKey>
        {
            var dict = new PersistentDictionary<TKey, TValue>(source, DictionaryLocation);
            dict.TraceSwitch.Level = TraceLevel.Verbose;
            return dict;
        }
    }
}