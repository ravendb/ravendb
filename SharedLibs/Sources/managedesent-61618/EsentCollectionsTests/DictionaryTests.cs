// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DictionaryTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Basic PersistentDictionary tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the PersistentDictionary.
    /// </summary>
    [TestClass]
    public class DictionaryTests
    {
        /// <summary>
        /// Where the dictionary will be located.
        /// </summary>
        private const string DictionaryLocation = "DictionaryFixture";

        /// <summary>
        /// The dictionary we are testing.
        /// </summary>
        private PersistentDictionary<DateTime, Guid?> dictionary;

        /// <summary>
        /// Test initialization.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.dictionary = new PersistentDictionary<DateTime, Guid?>(DictionaryLocation);
        }

        /// <summary>
        /// Cleanup after the test.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            this.dictionary.Dispose();
            Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);
        }

        /// <summary>
        /// A PersistentDictionary is read-write.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyDatabasePropertyReturnsDatabaseDirectory()
        {
            Assert.AreEqual(DictionaryLocation, this.dictionary.Database);
        }

        /// <summary>
        /// A PersistentDictionary is read-write.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyDictionaryIsNotReadOnly()
        {
            Assert.IsFalse(this.dictionary.IsReadOnly);
        }

        /// <summary>
        /// A PersistentDictionary's Key collection is read-only.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyDictionaryKeysAreReadOnly()
        {
            CheckCollectionIsReadOnly(this.dictionary.Keys);
        }

        /// <summary>
        /// A PersistentDictionary's Value collection is read-only.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyDictionaryValuesAreReadOnly()
        {
            CheckCollectionIsReadOnly(this.dictionary.Values);
        }

        /// <summary>
        /// Contains should return false for items that don't exist.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyContainsReturnsFalseWhenItemIsNotPresent()
        {
            var item = new KeyValuePair<DateTime, Guid?>(DateTime.Now, Guid.NewGuid());
            Assert.IsFalse(this.dictionary.Contains(item));
        }

        /// <summary>
        /// ContainsKey should return false for keys that don't exist.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyTryGetValueReturnsFalseWhenKeyIsNotPresent()
        {
            Guid? v;
            Assert.IsFalse(this.dictionary.TryGetValue(DateTime.Now, out v));
        }

        /// <summary>
        /// ContainsKey should return false for keys that don't exist.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyContainsKeyReturnsFalseWhenKeyIsNotPresent()
        {
            Assert.IsFalse(this.dictionary.ContainsKey(DateTime.Now));
        }

        /// <summary>
        /// Keys.Contains should return false for keys that don't exist.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyKeysContainsReturnsFalseWhenKeyIsNotPresent()
        {
            Assert.IsFalse(this.dictionary.Keys.Contains(DateTime.Now));
        }

        /// <summary>
        /// ContainsValue should return false for values that don't exist.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyContainsValueReturnsFalseWhenValueIsNotPresent()
        {
            Assert.IsFalse(this.dictionary.ContainsValue(Guid.Empty));
        }

        /// <summary>
        /// ContainsValue should return false for values that don't exist.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyValuesContainsReturnsFalseWhenValueIsNotPresent()
        {
            Assert.IsFalse(this.dictionary.Values.Contains(Guid.Empty));
        }

        /// <summary>
        /// Contains should return false when the value doesn't match.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyContainsItemReturnsFalseWhenValueDoesNotMatch()
        {
            var item = new KeyValuePair<DateTime, Guid?>(DateTime.Now, Guid.NewGuid());
            this.dictionary.Add(item);
            var otherItem = new KeyValuePair<DateTime, Guid?>(item.Key, Guid.NewGuid());
            Assert.IsFalse(this.dictionary.Contains(otherItem));
        }

        /// <summary>
        /// Remove should return false for keys that don't exist.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyRemoveReturnsFalseWhenKeyIsNotPresent()
        {
            Assert.IsFalse(this.dictionary.Remove(DateTime.Now));
        }

        /// <summary>
        /// Remove should return false for items that don't exist.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyRemoveItemReturnsFalseWhenItemIsNotPresent()
        {
            var item = new KeyValuePair<DateTime, Guid?>(DateTime.Now, Guid.NewGuid());
            Assert.IsFalse(this.dictionary.Remove(item));
        }

        /// <summary>
        /// Remove should return false (and not remove anything) when
        /// the value doesn't match.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyRemoveItemReturnsFalseWhenValueDoesNotMatch()
        {
            var item = new KeyValuePair<DateTime, Guid?>(DateTime.Now, Guid.NewGuid());
            this.dictionary.Add(item);
            var itemToRemove = new KeyValuePair<DateTime, Guid?>(item.Key, Guid.NewGuid());
            Assert.IsFalse(this.dictionary.Remove(itemToRemove));
            Assert.AreEqual(item.Value, this.dictionary[item.Key]);
        }

        /// <summary>
        /// Remove should return true (and remove the item) when
        /// the value does match.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyRemoveItemReturnsTrueWhenValueDoesMatch()
        {
            var item = new KeyValuePair<DateTime, Guid?>(DateTime.Now, Guid.NewGuid());
            this.dictionary.Add(item);
            Assert.IsTrue(this.dictionary.Remove(item));
            Assert.IsFalse(this.dictionary.Contains(item));
        }

        /// <summary>
        /// Calling Add() with a duplicate key throws an exception.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(ArgumentException))]
        public void VerifyAddThrowsExceptionForDuplicateKey()
        {
            var k = DateTime.UtcNow;
            var v = Guid.NewGuid();
            this.dictionary.Add(k, v);
            this.dictionary.Add(k, v);
        }

        /// <summary>
        /// Calling Add() with a duplicate item throws an exception.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(ArgumentException))]
        public void VerifyAddItemThrowsExceptionForDuplicateKey()
        {
            var k = DateTime.UtcNow;
            var v = Guid.NewGuid();
            this.dictionary.Add(k, v);
            this.dictionary.Add(new KeyValuePair<DateTime, Guid?>(k, Guid.NewGuid()));
        }

        /// <summary>
        /// Getting a value throws an exception if the key isn't found.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(KeyNotFoundException))]
        public void VerifyGettingValueThrowsExceptionIfKeyIsNotPresent()
        {
            var ignored = this.dictionary[DateTime.MaxValue];
        }

        /// <summary>
        /// Getting the first item throws an exception if the dictionary is empty.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyFirstThrowsExceptionIfDictionaryIsEmpty()
        {
            this.dictionary.First();
        }

        /// <summary>
        /// Getting the first item throws an exception if the dictionary is empty.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyFirstOrDefaultReturnsDefaultIfDictionaryIsEmpty()
        {
            var expected = new KeyValuePair<DateTime, Guid?>(default(DateTime), default(Guid?));
            Assert.AreEqual(expected, this.dictionary.FirstOrDefault());
        }

        /// <summary>
        /// Getting the first item returns the first item.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyFirstReturnsFirstItem()
        {
            var key = DateTime.Now;
            var value = Guid.NewGuid();
            this.dictionary[key] = value;
            var expected = new KeyValuePair<DateTime, Guid?>(key, value);
            Assert.AreEqual(expected, this.dictionary.First());
        }

        /// <summary>
        /// Getting the last item throws an exception if the dictionary is empty.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyLastThrowsExceptionIfDictionaryIsEmpty()
        {
            this.dictionary.Last();
        }

        /// <summary>
        /// Getting the last item throws an exception if the dictionary is empty.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyLastOrDefaultReturnsDefaultIfDictionaryIsEmpty()
        {
            var expected = new KeyValuePair<DateTime, Guid?>(default(DateTime), default(Guid?));
            Assert.AreEqual(expected, this.dictionary.LastOrDefault());
        }

        /// <summary>
        /// Getting the last item returns the last item.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyLastReturnsLastItem()
        {
            var key = DateTime.Now;
            var value = Guid.NewGuid();
            this.dictionary[key] = value;
            var expected = new KeyValuePair<DateTime, Guid?>(key, value);
            Assert.AreEqual(expected, this.dictionary.Last());
        }

        /// <summary>
        /// Exercise the Flush code path.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void FlushDatabase()
        {
            this.dictionary.Add(DateTime.Now, Guid.NewGuid());
            this.dictionary.Flush();
        }

        /// <summary>
        /// This dictionary has a nullable value. Set a value to null.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyValueCanBeNull()
        {
            var key = DateTime.Now;
            this.dictionary[key] = null;
            Assert.IsNull(this.dictionary[key]);
        }

        /// <summary>
        /// PersistentDatabaseFile.Exists should return true for this database.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void ExistsReturnsTrue()
        {
            Assert.IsTrue(PersistentDictionaryFile.Exists(DictionaryLocation));
        }

        /// <summary>
        /// Retrieve the first element from an empty dictionary. An exception
        /// should be thrown.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyFirstThrowsExceptionWhenDictionaryIsEmpty()
        {
            var ignored = this.dictionary.First();
        }

        /// <summary>
        /// Retrieve the first element from the keys of an empty dictionary. An exception
        /// should be thrown.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyFirstKeyThrowsExceptionWhenDictionaryIsEmpty()
        {
            var ignored = this.dictionary.Keys.First();
        }

        /// <summary>
        /// Retrieve the last element from an empty dictionary. An exception
        /// should be thrown.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyLastThrowsExceptionWhenDictionaryIsEmpty()
        {
            var ignored = this.dictionary.Last();
        }

        /// <summary>
        /// Retrieve the last element from the keys of an empty dictionary. An exception
        /// should be thrown.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(InvalidOperationException))]
        public void VerifyLastKeyThrowsExceptionWhenDictionaryIsEmpty()
        {
            var ignored = this.dictionary.Keys.Last();
        }

        /// <summary>
        /// Create a lot of enumerators.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Create lots of enumerators, using up cursors and sessions")]
        public void CreateLotsOfEnumerators()
        {
            this.dictionary[DateTime.Now] = Guid.NewGuid();
            var enumerators = new IEnumerator[200];
            
            for (int i = 0; i < enumerators.Length; ++i)
            {
                IEnumerable enumerable = this.dictionary;
                enumerators[i] = enumerable.GetEnumerator();
                Assert.IsTrue(enumerators[i].MoveNext());
            }

            foreach (IEnumerator enumerator in enumerators)
            {
                ((IDisposable)enumerator).Dispose();                
            }
        }

        #region CopyTo Tests

        /// <summary>
        /// Copy into a null array. An exception should be thrown.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Copy into a null array. An exception should be thrown")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyCopyToThrowsExceptionWhenArryIsEmpty()
        {
            this.dictionary.CopyTo(null, 0);
        }

        /// <summary>
        /// Copy into a negative array index. An exception should be thrown.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Copy into a negative array index. An exception should be thrown")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCopyToThrowsExceptionWhenArryIndexIsNegative()
        {
            var data = new KeyValuePair<DateTime, Guid?>[10];
            this.dictionary.CopyTo(data, -1);
        }

        /// <summary>
        /// Copy into a too-big array index. An exception should be thrown.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Copy into a too-big array index. An exception should be thrown")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCopyToThrowsExceptionWhenArryIndexIsTooBig()
        {
            var data = new KeyValuePair<DateTime, Guid?>[10];
            this.dictionary.CopyTo(data, data.Length);
        }

        /// <summary>
        /// Copy into an array that is too small. An exception should be thrown.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Copy into an array that is too small. An exception should be thrown")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCopyToThrowsExceptionWhenArryIsTooSmall()
        {
            var data = new KeyValuePair<DateTime, Guid?>[1];
            this.dictionary[DateTime.Now] = Guid.NewGuid();
            this.dictionary[DateTime.UtcNow] = Guid.NewGuid();
            this.dictionary.CopyTo(data, 0);
        }

        /// <summary>
        /// Copy into an array.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Copy into an array")]
        public void VerifyCopyCopiesElements()
        {
            var data = new KeyValuePair<DateTime, Guid?>[2];
            this.dictionary[DateTime.Now] = Guid.NewGuid();
            this.dictionary[DateTime.UtcNow] = Guid.NewGuid();
            this.dictionary.CopyTo(data, 0);
            CollectionAssert.AreEquivalent(data, this.dictionary.ToArray());
        }

        /// <summary>
        /// Copy into an array.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Copy keys into an array")]
        public void VerifyCopyKeysCopiesElements()
        {
            var data = new DateTime[2];
            this.dictionary[DateTime.Now] = Guid.NewGuid();
            this.dictionary[DateTime.UtcNow] = Guid.NewGuid();
            this.dictionary.Keys.CopyTo(data, 0);
            CollectionAssert.AreEquivalent(data, this.dictionary.Keys.ToArray());
        }

        #endregion

        /// <summary>
        /// Make sure the given collection is read-only.
        /// </summary>
        /// <typeparam name="T">The type of the collection.</typeparam>
        /// <param name="collection">The collection to check.</param>
        private static void CheckCollectionIsReadOnly<T>(ICollection<T> collection)
        {
            // IsReadOnly is true
            Assert.IsTrue(collection.IsReadOnly);

            // Add() throws an exception
            try
            {
                collection.Add(default(T));
                Assert.Fail("Should have thrown a NotSupportedException");
            }
            catch (NotSupportedException)
            {
            }

            // Remove() throws an exception
            try
            {
                collection.Remove(default(T));
                Assert.Fail("Should have thrown a NotSupportedException");
            }
            catch (NotSupportedException)
            {
            }

            // Clear() throws an exception
            try
            {
                collection.Clear();
                Assert.Fail("Should have thrown a NotSupportedException");
            }
            catch (NotSupportedException)
            {
            }
        }
    }
}