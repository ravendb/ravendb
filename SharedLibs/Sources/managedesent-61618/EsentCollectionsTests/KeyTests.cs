// --------------------------------------------------------------------------------------------------------------------
// <copyright file="KeyTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Test the Key class.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the Key class.
    /// </summary>
    [TestClass]
    public class KeyTests
    {
        /// <summary>
        /// Verify the key constructor sets the members.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the key constructor sets the members")]
        public void VerifyKeyConstructorSetsMembers()
        {
            var key = Key<int>.CreateKey(7, true);
            Assert.AreEqual(7, key.Value);
            Assert.IsTrue(key.IsInclusive);
        }

        /// <summary>
        /// Verify Equals(null) returns false.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify Key.Equals(null) returns false")]
        public void VerifyKeyEqualsNullIsFalse()
        {
            var key = Key<int>.CreateKey(0, false);
            Assert.IsFalse(key.Equals(null));
        }

        /// <summary>
        /// Verify a key equals itself.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a key equals itself")]
        public void VerifyKeyEqualsSelf()
        {
            var key = Key<int>.CreateKey(0, false);
            EqualityAsserts.TestEqualsAndHashCode(key, key, true);
        }

        /// <summary>
        /// Verify a key equals a key with the same value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a key equals a key with the same value")]
        public void VerifyKeyEqualsSameValue()
        {
            var key1 = Key<int>.CreateKey(1, true);
            var key2 = Key<int>.CreateKey(1, true);
            EqualityAsserts.TestEqualsAndHashCode(key1, key2, true);
        }

        /// <summary>
        /// Verify a key does not equal a key with different inclusive setting.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a key does not equal a key with different inclusive setting")]
        public void VerifyKeyNotEqualDifferentInclusive()
        {
            var key1 = Key<int>.CreateKey(2, true);
            var key2 = Key<int>.CreateKey(2, false);
            EqualityAsserts.TestEqualsAndHashCode(key1, key2, false);
        }

        /// <summary>
        /// Verify a key does not equal a prefix key.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a key does not equal a prefix key")]
        public void VerifyKeyNotEqualPrefixKey()
        {
            var key1 = Key<string>.CreateKey("foo", true);
            var key2 = Key<string>.CreatePrefixKey("foo");
            EqualityAsserts.TestEqualsAndHashCode(key1, key2, false);
        }

        /// <summary>
        /// Verify a key does not equal a key with different value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a key does not equal a key with different value")]
        public void VerifyKeyNotEqualDifferentValue()
        {
            var key1 = Key<int>.CreateKey(3, false);
            var key2 = Key<int>.CreateKey(4, false);
            EqualityAsserts.TestEqualsAndHashCode(key1, key2, false);
        }

        /// <summary>
        /// Verify the Key.Equals returns false for null object.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the Key.Equals returns false for null object")]
        public void VerifyKeyEqualsNullOnjectIsFalse()
        {
            object obj = null;
            Assert.IsFalse(Key<double>.CreateKey(0, false).Equals(obj));
        }

        /// <summary>
        /// Verify the Key.Equals returns false for an object of a different type
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the Key.Equals returns false for object of a different type")]
        public void VerifyKeyEqualsDifferentTypeIsFalse()
        {
            object obj = new object();
            Assert.IsFalse(Key<double>.CreateKey(0, false).Equals(obj));
        }
    }
}