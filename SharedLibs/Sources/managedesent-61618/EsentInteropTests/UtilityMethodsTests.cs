//-----------------------------------------------------------------------
// <copyright file="UtilityMethodsTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System.IO;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Testing the utility methods used in this test framework.
    /// </summary>
    [TestClass]
    public class UtilityMethodsTests
    {
        /// <summary>
        /// Check that Any.Bytes returns an array of at least 1 byte.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that Any.Bytes returns an array of at least 1 byte")]
        public void TestAnyBytesIsAtLeastOneByte()
        {
            byte[] bytes = Any.Bytes;
            Assert.IsTrue(bytes.Length >= 1);
        }

        /// <summary>
        /// Check that Any.Bytes returns an array of no more than 255 bytes.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that Any.Bytes returns an array of no more than 255 bytes")]
        public void TestAnyBytesIsNoMoreThan255Bytes()
        {
            byte[] bytes = Any.Bytes;
            Assert.IsTrue(bytes.Length <= 255);
        }

        /// <summary>
        /// Check that Any.BytesOfLength returns a string of the correct length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that Any.BytesOfLength returns a string of the correct length")]
        public void TestAnyBytesOfLengthIsCorrectLength()
        {
            byte[] s = Any.BytesOfLength(20);
            Assert.AreEqual(20, s.Length);
        }

        /// <summary>
        /// Check that Any.String returns a string of at least 1 character.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that Any.String returns a string of at least 1 character")]
        public void TestAnyStringIsAtLeastOneCharacter()
        {
            string s = Any.String;
            Assert.IsTrue(s.Length >= 1);
        }

        /// <summary>
        /// Check that Any.String returns a string of no more than 120 characters.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that Any.String returns a string of no more than 120 characters")]
        public void TestAnyStringIsNoMoreThan120Characters()
        {
            string s = Any.String;
            Assert.IsTrue(s.Length <= 120);
        }

        /// <summary>
        /// Check that Any.String returns a string of ASCII characters.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that Any.String returns a string of ASCII characters")]
        public void TestAnyStringIsAsciiCharacters()
        {
            string s = Any.String;
            foreach (char c in s)
            {
                Assert.IsTrue(c <= '~');    // last ASCII character (127)
                Assert.IsTrue(c >= ' ');    // first ASCII character (32);
            }
        }

        /// <summary>
        /// Check that Any.StringOfLength returns a string of the correct length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that Any.StringOfLength returns a string of the correct length")]
        public void TestAnyStringOfLengthIsCorrectLength()
        {
            string s = Any.StringOfLength(10);
            Assert.AreEqual(10, s.Length);
        }

        /// <summary>
        /// Verify that Cleanup.DeleteDirectoryWithRetry can be called on
        /// a directory that doesn't exist.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that DeleteDirectoryWithRetry can be called on a directory that doesn't exist")]
        public void TestDeleteDirectoryWithRetryWhenDirectoryDoesNotExist()
        {
            string directory = Path.GetRandomFileName();
            Assert.IsFalse(Directory.Exists(directory));
            Cleanup.DeleteDirectoryWithRetry(directory);
        }

        /// <summary>
        /// Verify that Cleanup.DeleteDirectoryWithRetry removes a directory.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that DeleteDirectoryWithRetry removes a directory")]
        public void VerifyDeleteDirectoryWithRetryRemovesDirectory()
        {
            // Create a random directory with a file in it
            string directory = Path.GetRandomFileName();
            Directory.CreateDirectory(directory);
            File.WriteAllText(Path.Combine(directory, "foo.txt"), "hello");
            Assert.IsTrue(Directory.Exists(directory));

            // Delete the directory
            Cleanup.DeleteDirectoryWithRetry(directory);

            // The directory should no longer exist
            Assert.IsFalse(Directory.Exists(directory));
        }

        /// <summary>
        /// Verify that Cleanup.DeleteFileWithRetry can be called with
        /// a filename that doesn't exist.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that DeleteFileWithRetry removes a file")]
        public void TestDeleteFileWithRetryWhenFileDoesNotExist()
        {
            string file = Path.GetRandomFileName();
            Assert.IsFalse(File.Exists(file));
            Cleanup.DeleteFileWithRetry(file);
        }

        /// <summary>
        /// Verify that Cleanup.DeleteFileWithRetry removes a file.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that DeleteFileWithRetry removes a file")]
        public void VerifyDeleteFileWithRetryRemovesFile()
        {
            // Create a random file
            string file = Path.GetRandomFileName();
            File.WriteAllText(file, "hello");
            Assert.IsTrue(File.Exists(file));

            // Delete the file
            Cleanup.DeleteFileWithRetry(file);

            // The file should no longer exist
            Assert.IsFalse(File.Exists(file));
        }
    }
}