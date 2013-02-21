//-----------------------------------------------------------------------
// <copyright file="StringCacheTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the StringCache class.
    /// </summary>
    [TestClass]
    public class StringCacheTests
    {
        /// <summary>
        /// Try to intern a random string (should not be interned).
        /// </summary>
        [TestMethod]
        [Description("Try to intern a random string (should not be interned)")]
        [Priority(0)]
        public void TryToInternRandomString()
        {
            string s = StringCache.TryToIntern(Any.String);
            Assert.IsNull(String.IsInterned(s), "Should not have been interned");
        }

        /// <summary>
        /// Try to intern an interned string.
        /// </summary>
        [TestMethod]
        [Description("Try to intern an interned string")]
        [Priority(0)]
        public void TryToInternInternedString()
        {
            string s = String.Intern(StringCache.TryToIntern(Any.String));
            Assert.IsNotNull(String.IsInterned(s), "Should not have been interned");
        }

        /// <summary>
        /// Get a string with a null buffer.
        /// </summary>
        [TestMethod]
        [Description("Test StringCache.GetString with a null buffer")]
        [Priority(0)]
        public void GetStringWithNull()
        {
            byte[] buffer = null;
            Assert.AreEqual(String.Empty, StringCache.GetString(buffer, 0, 0));
        }

        /// <summary>
        /// Get a string with a null buffer.
        /// </summary>
        [TestMethod]
        [Description("Test StringCache.GetString")]
        [Priority(0)]
        public void GetString()
        {
            byte[] buffer = Encoding.Unicode.GetBytes("Hello");
            Assert.AreEqual("Hello", StringCache.GetString(buffer, 0, buffer.Length));
        }

        /// <summary>
        /// Get a string twice to make sure it is cached.
        /// </summary>
        [TestMethod]
        [Description("Test StringCache.GetString caching")]
        [Priority(0)]
        public void GetStringCaching()
        {
            byte[] buffer = Encoding.Unicode.GetBytes("Hello");
            string s1 = StringCache.GetString(buffer, 0, buffer.Length);
            string s2 = StringCache.GetString(buffer, 0, buffer.Length);
            Assert.AreSame(s1, s2);
        }

        /// <summary>
        /// Make sure a long string isn't cached.
        /// </summary>
        [TestMethod]
        [Description("Test StringCache.GetString caching with a long string")]
        [Priority(0)]
        public void VerifyLongStringIsNotCached()
        {
            string expected = Any.StringOfLength(8192);
            byte[] buffer = Encoding.Unicode.GetBytes(expected);
            string actual = StringCache.GetString(buffer, 0, buffer.Length);
            Assert.AreEqual(expected, actual);
            Assert.AreNotSame(actual, StringCache.GetString(buffer, 0, buffer.Length));
        }

        /// <summary>
        /// Get severals strings twice to make sure they are cached.
        /// </summary>
        [TestMethod]
        [Description("Test StringCache.GetString caching with multiple strings")]
        [Priority(0)]
        public void GetStringCachingMultipleStrings()
        {
            string[] expected = new[]
            {
                "Hello",
                "hello",
                "World",
                "World ",
                "foo",
                "bar",
                "baz",
                "qux",
                "0",
                "1",
                " ",
                String.Empty,
            };
            string[] actual = new string[expected.Length];

            for (int i = 0; i < expected.Length; ++i)
            {
                byte[] buffer = Encoding.Unicode.GetBytes(expected[i]);
                actual[i] = StringCache.GetString(buffer, 0, buffer.Length);
                Assert.AreEqual(expected[i], actual[i], "First conversion is incorrect");
            }

            for (int i = 0; i < expected.Length; ++i)
            {
                byte[] buffer = Encoding.Unicode.GetBytes(expected[i]);
                string cached = StringCache.GetString(buffer, 0, buffer.Length);
                Assert.AreEqual(expected[i], cached, "Cached value is incorrect");
                Assert.AreSame(actual[i], cached, "Value {0} was not cached", expected[i]);
            }
        }

        /// <summary>
        /// Convert multiple random strings.
        /// </summary>
        [TestMethod]
        [Description("Test StringCache.GetString caching random strings")]
        [Priority(2)]
        public void GetStringCachingRandomStrings()
        {
            for (int i = 0; i < 250000; ++i)
            {
                string expected = Any.String;
                byte[] buffer = Encoding.Unicode.GetBytes(expected);
                string actual = StringCache.GetString(buffer, 0, buffer.Length);
                Assert.AreEqual(expected, actual);
                Assert.AreSame(actual, StringCache.GetString(buffer, 0, buffer.Length));
            }
        }
    }
}