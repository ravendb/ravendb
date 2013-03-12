// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DictionaryCreationTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Test creating a PersistentDictionary.
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
    /// Test creating a PersistentDictionary.
    /// </summary>
    [TestClass]
    public class DictionaryCreationTests
    {
        /// <summary>
        /// A PersistentDictionary is read-write.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyKeyColumnConverterThrowsExceptionOnUnsupportedType()
        {
            var k = new KeyColumnConverter<decimal>();
        }

        /// <summary>
        /// Creating a dictionary without a directory fails.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyConstructorThrowsExceptionWhenDirectoryIsNull()
        {
            var dictionary = new PersistentDictionary<int, int>(null);
        }

        /// <summary>
        /// Creating a dictionary with an invalid key type fails.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyConstructorThrowsExceptionWhenKeyTypeIsInvalid()
        {
            var dictionary = new PersistentDictionary<NonSerializableComparable, int>("foo");
        }

        /// <summary>
        /// Creating a dictionary with an invalid value type fails.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyConstructorThrowsExceptionWhenValueTypeIsInvalid()
        {
            var dictionary = new PersistentDictionary<int, NonSerializableStruct>("foo");
        }

        /// <summary>
        /// Creating a dictionary with an invalid value type fails.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyConstructorThrowsExceptionWhenValueTypeIsInvalid2()
        {
            var dictionary = new PersistentDictionary<int, NonSerializable>("foo");
        }

        /// <summary>
        /// PersistentDictionaryFile.Exists fails when the directory is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyExistsThrowsExceptionWhenDirectoryIsNull()
        {
            PersistentDictionaryFile.Exists(null);
        }

        /// <summary>
        /// Checking for a database returns false when the directory 
        /// doesn't even exists.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void VerifyExistsReturnsFalseWhenDirectoryDoesNotExist()
        {
            const string NonExistentDirectory = "doesnotexist";
            Assert.IsFalse(Directory.Exists(NonExistentDirectory));
            Assert.IsFalse(PersistentDictionaryFile.Exists(NonExistentDirectory));
        }

        /// <summary>
        /// Checking for a database returns false when the specified
        /// directory is actually a file.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void VerifyExistsReturnsFalseWhenDirectoryIsAFile()
        {
            string file = Path.GetTempFileName();
            Assert.IsFalse(PersistentDictionaryFile.Exists(file));
            File.Delete(file);
        }

        /// <summary>
        /// Checking for a database returns false when the specified
        /// directory is actually a file.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void VerifyExistsReturnsFalseWhenDatabaseFileDoesNotExist()
        {
            const string TestDirectory = "testdirectory";
            Directory.CreateDirectory(TestDirectory);
            Assert.IsFalse(PersistentDictionaryFile.Exists(TestDirectory));
            Directory.Delete(TestDirectory);
        }

        /// <summary>
        /// PersistentDictionaryFile.DeleteFiles fails when the directory is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyDeleteThrowsExceptionWhenDirectoryIsNull()
        {
            PersistentDictionaryFile.DeleteFiles(null);
        }

        /// <summary>
        /// PersistentDictionaryFile.DeleteFiles works on a non-existent directory.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        public void VerifyDeleteSucceedsWhenDirectoryDoesNotExist()
        {
            const string NonExistentDirectory = "doesnotexist";
            Assert.IsFalse(Directory.Exists(NonExistentDirectory));
            PersistentDictionaryFile.DeleteFiles(NonExistentDirectory);
        }

        /// <summary>
        /// PersistentDictionaryFile.DeleteFiles works on an empty directory.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void VerifyDeleteSucceedsWhenDirectoryIsEmpty()
        {
            const string TestDirectory = "testdirectory";
            if (Directory.Exists(TestDirectory))
            {
                Directory.Delete(TestDirectory);
            }

            Directory.CreateDirectory(TestDirectory);
            PersistentDictionaryFile.DeleteFiles(TestDirectory);
            Directory.Delete(TestDirectory);
        }

        /// <summary>
        /// PersistentDictionaryFile.DeleteFiles does not delete
        /// unrelated files.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void VerifyDeleteLeavesUnrelatedFiles()
        {
            const string TestDirectory = "testdirectory";
            string testFile = Path.Combine(TestDirectory, "myfile.log");
            if (Directory.Exists(TestDirectory))
            {
                Directory.Delete(TestDirectory);
            }

            Directory.CreateDirectory(TestDirectory);
            File.WriteAllText(testFile, "hello world");
            PersistentDictionaryFile.DeleteFiles(TestDirectory);
            Assert.IsTrue(File.Exists(testFile));
            Cleanup.DeleteDirectoryWithRetry(TestDirectory);
        }

        /// <summary>
        /// PersistentDictionaryFile.DeleteFiles removes all database files.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyDeleteRemovesDatabaseFiles()
        {
            const string DictionaryLocation = "DictionaryToDelete";
            var dict = new PersistentDictionary<ulong, bool>(DictionaryLocation);
            dict.Dispose();
            Assert.IsTrue(PersistentDictionaryFile.Exists(DictionaryLocation));
            PersistentDictionaryFile.DeleteFiles(DictionaryLocation);
            Assert.IsFalse(PersistentDictionaryFile.Exists(DictionaryLocation));
            Directory.Delete(DictionaryLocation, false);
        }

        /// <summary>
        /// PersistentDictionaryFile.DeleteFiles removes reserved logs (created 
        /// on older versions of Windows).
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyDeleteRemovesReservedLogs()
        {
            const string DictionaryLocation = "DictionaryToDelete";
            const string ReservedLog = @"DictionaryToDelete\res1.log";
            var dict = new PersistentDictionary<ulong, bool>(DictionaryLocation);
            dict.Dispose();
            if (!File.Exists(ReservedLog))
            {
                File.WriteAllText(ReservedLog, "VerifyDeleteRemovesDatabaseFiles");
            }

            Assert.IsTrue(PersistentDictionaryFile.Exists(DictionaryLocation));
            PersistentDictionaryFile.DeleteFiles(DictionaryLocation);
            Assert.IsFalse(PersistentDictionaryFile.Exists(DictionaryLocation));
            Directory.Delete(DictionaryLocation, false);
        }

        /// <summary>
        /// Opening a dictionary fails if the types of the keys don't match.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(ArgumentException))]
        public void VerifyOpenFailsOnMismatchedKeyTypes()
        {
            const string DictionaryLocation = "IntIntDictionary";
            var dict = new PersistentDictionary<int, int>(DictionaryLocation);
            dict.Dispose();
            var wrongDict = new PersistentDictionary<long, int>(DictionaryLocation);
            Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);
        }

        /// <summary>
        /// Opening a dictionary fails if the types of the values don't match.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(ArgumentException))]
        public void VerifyOpenFailsOnMismatchedValueTypes()
        {
            const string DictionaryLocation = "IntIntDictionary";
            var dict = new PersistentDictionary<int, int>(DictionaryLocation);
            dict.Dispose();
            var wrongDict = new PersistentDictionary<int, string>(DictionaryLocation);
            Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);
        }

        /// <summary>
        /// Trying to copy a null dictionary generates an exception.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyConstructorThrowsExceptionWhenDictionaryIsNull()
        {
            const string DictionaryLocation = "CopiedDictionaryFail";
            var dict = new PersistentDictionary<int, int>(null, DictionaryLocation);
        }

        /// <summary>
        /// Create a copy of a dictionary using the constructor.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyConstructorCanCopyDictionary()
        {
            const string DictionaryLocation = "CopiedDictionary";
            var expected = new Dictionary<int, Guid?>();
            for (int i = 0; i < 256; ++i)
            {
                expected[i] = Guid.NewGuid();
            }

            var actual = new PersistentDictionary<int, Guid?>(expected, DictionaryLocation);
            DictionaryAssert.AreEqual(expected, actual);
            actual.Dispose();
            Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);            
        }

        /// <summary>
        /// A dummy class used to test invalid key types.
        /// </summary>
        [Serializable]
        private struct NonSerializableComparable : IComparable<NonSerializableComparable>
        {
            /// <summary>
            /// This dictionary stops the structure from being serializable.
            /// </summary>
            /// <remarks>
            /// This isn't actually used because this class is never instantiated. The presence
            /// of this member in the class means it can't be used as the value of a 
            /// dictionary and that is what we want to test.
            /// </remarks>
            private Dictionary<int, int> dict;

            /// <summary>
            /// Dummy CompareTo method.
            /// </summary>
            /// <param name="other">Parameter is ignored.</param>
            /// <returns>Always returns 0.</returns>
            public int CompareTo(NonSerializableComparable other)
            {
                return this.dict.Count.CompareTo(other.dict.Count);
            }
        }

        /// <summary>
        /// A dummy struct used to test invalid value types.
        /// </summary>
        [Serializable]
        private struct NonSerializableStruct 
        {
            /// <summary>
            /// Dummy object.
            /// </summary>
            public object Obj;
        }

        /// <summary>
        /// A dummy class used to test invalid value types.
        /// </summary>
        [Serializable]
        private class NonSerializable
        {
        }
    }
}