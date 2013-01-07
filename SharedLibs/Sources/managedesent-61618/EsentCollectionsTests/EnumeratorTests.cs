// --------------------------------------------------------------------------------------------------------------------
// <copyright file="EnumeratorTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System.Collections;
    using System.Collections.Generic;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Contains methods to test an enumerator.
    /// </summary>
    [TestClass]
    public class EnumeratorTests
    {
        /// <summary>
        /// Directory to create the database in.
        /// </summary>
        private const string Directory = "EnumeratorTests";
 
        /// <summary>
        /// Dictionary we are testing.
        /// </summary>
        private PersistentDictionary<int, int> dictionary;

        /// <summary>
        /// Setup the EnumeratorTests fixture.
        /// </summary>
        [TestInitialize]
        [Description("Setup the EnumeratorTests fixture")]
        public void Setup()
        {
            Cleanup.DeleteDirectoryWithRetry(Directory);
            this.dictionary = new PersistentDictionary<int, int>(Directory);
            this.dictionary[1] = 1;
        }

        /// <summary>
        /// Cleanup the EnumeratorTests fixture.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the EnumeratorTests fixture")]
        public void Teardown()
        {
            this.dictionary.Dispose();
            Cleanup.DeleteDirectoryWithRetry(Directory);
        }

        /// <summary>
        /// Test the dictionary enumerator.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the dictionary enumerator")]
        public void TestDictionaryEnumerator()
        {
            ValidateEnumerator(this.dictionary.GetEnumerator());
        }

        /// <summary>
        /// Test the reversed dictionary enumerator.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the reversed dictionary enumerator")]
        public void TestDictionaryReverseEnumerator()
        {
            ValidateEnumerator(this.dictionary.Reverse().GetEnumerator());
        }

        /// <summary>
        /// Test the dictionary keys enumerator.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the dictionary keys enumerator")]
        public void TestDictionaryKeysEnumerator()
        {
            ValidateEnumerator(this.dictionary.Keys.GetEnumerator());
        }

        /// <summary>
        /// Test the reversed dictionary keys enumerator.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the reversed dictionary keys enumerator")]
        public void TestDictionaryKeysReverseEnumerator()
        {
            ValidateEnumerator(this.dictionary.Keys.Reverse().GetEnumerator());
        }

        /// <summary>
        /// Test the dictionary value enumerator.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test the dictionary value enumerator")]
        public void TestDictionaryValuesEnumerator()
        {
            ValidateEnumerator(this.dictionary.Values.GetEnumerator());
        }

        /// <summary>
        /// Test an enumerator.
        /// </summary>
        /// <typeparam name="T">The type returned by the enumerator.</typeparam>
        /// <param name="enumerator">The enumerator to test this must be non-empty.</param>
        internal static void ValidateEnumerator<T>(IEnumerator<T> enumerator)
        {
            Assert.IsTrue(enumerator.MoveNext(), "First move next failed");
            T first = enumerator.Current;
            object obj = ((IEnumerator)enumerator).Current;
            Assert.AreEqual(obj, first, "Current and IEnumerator.Current returned different results");
            while (enumerator.MoveNext())
            {
            }

            Assert.IsFalse(enumerator.MoveNext(), "MoveNext when at end should return false");
            enumerator.Reset();
            Assert.IsTrue(enumerator.MoveNext(), "MoveNext after Reset fails");
            Assert.AreEqual(first, enumerator.Current, "Didn't move back to first element");
            enumerator.Dispose();
        }
    }
}
