// --------------------------------------------------------------------------------------------------------------------
// <copyright file="KeyRangeTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Test the Key class.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the KeyRange class.
    /// </summary>
    [TestClass]
    public class KeyRangeTests
    {
        /// <summary>
        /// Verify the KeyRange constructor sets the members.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the KeyRange constructor sets the members")]
        public void VerifyKeyRangeConstructorSetsMembers()
        {
            var keyrange = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, false));
            Assert.AreEqual(keyrange.Min, Key<int>.CreateKey(1, true));
            Assert.AreEqual(keyrange.Max, Key<int>.CreateKey(2, false));
        }

        /// <summary>
        /// Call KeyRange.ToString() with an empty key range.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Call KeyRange.ToString with an empty range")]
        public void TestNullKeyRangeToString()
        {
            var keyrange = KeyRange<int>.OpenRange;
            string s = keyrange.ToString();
            Assert.IsNotNull(s);
            Assert.AreNotEqual(s, String.Empty);
        }

        /// <summary>
        /// Call KeyRange.ToString() with an exclusive range.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Call KeyRange.ToString with an exclusive range")]
        public void TestKeyRangeToStringExclusive()
        {
            var keyrange = new KeyRange<int>(Key<int>.CreateKey(1, false), Key<int>.CreateKey(2, false));
            string s = keyrange.ToString();
            Assert.IsNotNull(s);
            Assert.AreNotEqual(s, String.Empty);
            StringAssert.Contains(s, "1");
            StringAssert.Contains(s, "2");
            StringAssert.Contains(s, "exclusive");
        }

        /// <summary>
        /// Call KeyRange.ToString() with an inclusive range.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Call KeyRange.ToString with an inclusive range")]
        public void TestKeyRangeToStringInclusive()
        {
            var keyrange = new KeyRange<int>(Key<int>.CreateKey(3, true), Key<int>.CreateKey(4, true));
            string s = keyrange.ToString();
            Assert.IsNotNull(s);
            Assert.AreNotEqual(s, String.Empty);
            StringAssert.Contains(s, "3");
            StringAssert.Contains(s, "4");
            StringAssert.Contains(s, "inclusive");
        }

        /// <summary>
        /// Call KeyRange.ToString() with a prefix range.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Call KeyRange.ToString with a prefix range")]
        public void TestKeyRangeToStringPrefix()
        {
            var keyrange = new KeyRange<string>(Key<string>.CreateKey("3", true), Key<string>.CreatePrefixKey("4"));
            string s = keyrange.ToString();
            Assert.IsNotNull(s);
            Assert.AreNotEqual(s, String.Empty);
            StringAssert.Contains(s, "3");
            StringAssert.Contains(s, "4");
            StringAssert.Contains(s, "prefix");
        }

        /// <summary>
        /// Call KeyRange.ToString() with an empty range.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Call KeyRange.ToString with an empty range")]
        public void TestKeyRangeToStringEmpt()
        {
            string s = KeyRange<Guid>.EmptyRange.ToString();
            Assert.IsNotNull(s);
            Assert.AreNotEqual(s, String.Empty);
            StringAssert.Contains(s, "empty");
        }

        /// <summary>
        /// KeyRange.Empty test 1
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 1 (open range)")]
        public void VerifyKeyRangeEmpty1()
        {
            Assert.IsFalse(KeyRange<Guid>.OpenRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 2
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 2 (empty range)")]
        public void VerifyKeyRangeEmpty2()
        {
            Assert.IsTrue(KeyRange<Guid>.EmptyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 3
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 3 (null min)")]
        public void VerifyKeyRangeEmpty3()
        {
            var keyRange = new KeyRange<long>(null, Key<long>.CreateKey(1, false));
            Assert.IsFalse(keyRange.IsEmpty);                
        }

        /// <summary>
        /// KeyRange.Empty test 4
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 4 (null max)")]
        public void VerifyKeyRangeEmpty4()
        {
            var keyRange = new KeyRange<long>(Key<long>.CreateKey(1, false), null);
            Assert.IsFalse(keyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 5
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 5 (min and max)")]
        public void VerifyKeyRangeEmpty5()
        {
            var keyRange = new KeyRange<long>(Key<long>.CreateKey(1, false), Key<long>.CreateKey(2, false));
            Assert.IsFalse(keyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 6
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 6 (min > max)")]
        public void VerifyKeyRangeEmpty6()
        {
            var keyRange = new KeyRange<long>(Key<long>.CreateKey(2, false), Key<long>.CreateKey(1, false));
            Assert.IsTrue(keyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 7
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 7 (min == max, min is exclusive)")]
        public void VerifyKeyRangeEmpty7()
        {
            var keyRange = new KeyRange<long>(Key<long>.CreateKey(2, false), Key<long>.CreateKey(2, true));
            Assert.IsTrue(keyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 8
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 8 (min == max, max is exclusive)")]
        public void VerifyKeyRangeEmpty8()
        {
            var keyRange = new KeyRange<long>(Key<long>.CreateKey(2, true), Key<long>.CreateKey(2, false));
            Assert.IsTrue(keyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 9
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 9 (min == max, both are exclusive)")]
        public void VerifyKeyRangeEmpty9()
        {
            var keyRange = new KeyRange<long>(Key<long>.CreateKey(2, false), Key<long>.CreateKey(2, false));
            Assert.IsTrue(keyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 10
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 10 (min == max, both are inclusive)")]
        public void VerifyKeyRangeEmpty10()
        {
            var keyRange = new KeyRange<long>(Key<long>.CreateKey(2, true), Key<long>.CreateKey(2, true));
            Assert.IsFalse(keyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 11
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 11 (min == max, max is prefix)")]
        public void VerifyKeyRangeEmpty11()
        {
            var keyRange = new KeyRange<string>(Key<string>.CreateKey("b", true), Key<string>.CreatePrefixKey("b"));
            Assert.IsFalse(keyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 12
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 12 (min == max, max is prefix, min is exclusive)")]
        public void VerifyKeyRangeEmpty12()
        {
            // The record "ba" would match this
            var keyRange = new KeyRange<string>(Key<string>.CreateKey("b", false), Key<string>.CreatePrefixKey("b"));
            Assert.IsFalse(keyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 13
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 13 (empty string range)")]
        public void VerifyKeyRangeEmpty13()
        {
            Assert.IsTrue(KeyRange<string>.EmptyRange.IsEmpty);
        }

        /// <summary>
        /// KeyRange.Empty test 14
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Empty test 14 (longer prefix)")]
        public void VerifyKeyRangeEmpty14()
        {
            KeyRange<string> range = new KeyRange<string>(
                Key<string>.CreateKey("ggi", true),
                Key<string>.CreatePrefixKey("g"));
            Assert.IsFalse(range.IsEmpty);
        }

        /// <summary>
        /// Verify the KeyRange.Equals returns false for null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the KeyRange.Equals returns false for null")]
        public void VerifyKeyRangeEqualsNullIsFalse()
        {
            var keyrange = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, false));
            Assert.IsFalse(keyrange.Equals(null));
        }

        /// <summary>
        /// Verify the KeyRange.Equals returns false for null object.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the KeyRange.Equals returns false for null object")]
        public void VerifyKeyRangeEqualsNullOnjectIsFalse()
        {
            object obj = null;
            Assert.IsFalse(KeyRange<double>.OpenRange.Equals(obj));
        }

        /// <summary>
        /// Verify the KeyRange.Equals returns false for an object of a different type
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the KeyRange.Equals returns false for object of a different type")]
        public void VerifyKeyRangeEqualsDifferentTypeIsFalse()
        {
            object obj = new object();
            Assert.IsFalse(KeyRange<double>.OpenRange.Equals(obj));
        }

        /// <summary>
        /// Verify a KeyRange equals itself.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a KeyRange equals itself")]
        public void VerifyKeyRangeEqualsSelf()
        {
            var keyrange = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, false));
            EqualityAsserts.TestEqualsAndHashCode(keyrange, keyrange, true);
        }

        /// <summary>
        /// Verify open key ranges are equal.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify open key ranges are equal")]
        public void VerifyOpenKeyRangesAreEqual()
        {
            var keyrange1 = new KeyRange<int>(null, null);
            var keyrange2 = new KeyRange<int>(null, null);
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, true);
        }

        /// <summary>
        /// Verify empty string key ranges are equal.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify Empty key ranges are equal")]
        public void VerifyEmptyStringKeyRangesAreEqual()
        {
            EqualityAsserts.TestEqualsAndHashCode(KeyRange<string>.EmptyRange, KeyRange<string>.EmptyRange, true);
        }

        /// <summary>
        /// Verify different empty string key ranges are equal.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify different empty key ranges are equal")]
        public void VerifyDifferentEmptyStringKeyRangesAreEqual()
        {
            var keyRange = new KeyRange<long>(Key<long>.CreateKey(5, false), Key<long>.CreateKey(5, false));
            EqualityAsserts.TestEqualsAndHashCode(KeyRange<long>.EmptyRange, keyRange, true);
        }

        /// <summary>
        /// Verify a KeyRange equals a range with the same min values.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a KeyRange equals a range with the same mi nvalues")]
        public void VerifyKeyRangeEqualsSameMinValues()
        {
            var keyrange1 = new KeyRange<int>(Key<int>.CreateKey(1, true), null);
            var keyrange2 = new KeyRange<int>(Key<int>.CreateKey(1, true), null);
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, true);
        }

        /// <summary>
        /// Verify a KeyRange equals a range with the same max values.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a KeyRange equals a range with the same max values")]
        public void VerifyKeyRangeEqualsSameMaxValues()
        {
            var keyrange1 = new KeyRange<int>(null, Key<int>.CreateKey(2, false));
            var keyrange2 = new KeyRange<int>(null, Key<int>.CreateKey(2, false));
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, true);
        }

        /// <summary>
        /// Verify a KeyRange equals a range with the same values.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a KeyRange equals a range with the same values")]
        public void VerifyKeyRangeEqualsSameValues()
        {
            var keyrange1 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, false));
            var keyrange2 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, false));
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, true);
        }

        /// <summary>
        /// Verify KeyRange inequality 1.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a KeyRange inequality 1")]
        public void VerifyKeyRangeInequality1()
        {
            var keyrange1 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, true));
            var keyrange2 = new KeyRange<int>(Key<int>.CreateKey(1, true), null);
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, false);
        }

        /// <summary>
        /// Verify KeyRange inequality 2.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a KeyRange inequality 2")]
        public void VerifyKeyRangeInequality2()
        {
            var keyrange1 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, true));
            var keyrange2 = new KeyRange<int>(null, Key<int>.CreateKey(2, true));
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, false);
        }

        /// <summary>
        /// Verify KeyRange inequality 3.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a KeyRange inequality 3")]
        public void VerifyKeyRangeInequality3()
        {
            var keyrange1 = new KeyRange<int>(null, null);
            var keyrange2 = new KeyRange<int>(null, Key<int>.CreateKey(2, true));
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, false);
        }

        /// <summary>
        /// Verify KeyRange inequality 4.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a KeyRange inequality 4")]
        public void VerifyKeyRangeInequality4()
        {
            var keyrange1 = new KeyRange<int>(Key<int>.CreateKey(1, true), null);
            var keyrange2 = new KeyRange<int>(null, null);
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, false);
        }

        /// <summary>
        /// Verify a KeyRange does not equal a range with different inclusiveness.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a KeyRange does not equal a range with different inclusiveness")]
        public void VerifyKeyRangeInequalityInclusiveness()
        {
            var keyrange1 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, true));
            var keyrange2 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, false));
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, false);
        }

        /// <summary>
        /// Verify a KeyRange does not equal a range with reversed values.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a KeyRange equals a range with reversed values")]
        public void VerifyKeyRangeEqualsReversedValues()
        {
            var keyrange1 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, true));
            var keyrange2 = new KeyRange<int>(Key<int>.CreateKey(2, true), Key<int>.CreateKey(1, true));
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, false);
        }

        /// <summary>
        /// KeyRange.Equals test 1
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Equals test 1 (null min vs non-null min)")]
        public void VerifyKeyRangeNotEquals1()
        {
            var keyrange1 = new KeyRange<int>(null, Key<int>.CreateKey(2, false));
            var keyrange2 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, false));
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, false);
        }

        /// <summary>
        /// KeyRange.Equals test 2
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Equals test 2 (null max vs non-null max)")]
        public void VerifyKeyRangeNotEquals2()
        {
            var keyrange1 = new KeyRange<int>(Key<int>.CreateKey(1, true), null);
            var keyrange2 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, false));
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, false);
        }

        /// <summary>
        /// KeyRange.Equals test 3
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Equals test 3 (different mins)")]
        public void VerifyKeyRangeNotEquals3()
        {
            var keyrange1 = new KeyRange<int>(Key<int>.CreateKey(1, false), Key<int>.CreateKey(2, false));
            var keyrange2 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, false));
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, false);
        }

        /// <summary>
        /// KeyRange.Equals test 4
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Equals test 4 (different maxs)")]
        public void VerifyKeyRangeNotEquals4()
        {
            var keyrange1 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, true));
            var keyrange2 = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(2, false));
            EqualityAsserts.TestEqualsAndHashCode(keyrange1, keyrange2, false);
        }

        #region Invert tests

        /// <summary>
        /// KeyRange.Invert test 1
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Invert test 1 (empty range)")]
        public void TestKeyRangeInvert1()
        {
            var keyrange = KeyRange<int>.OpenRange;
            Assert.AreEqual(keyrange, keyrange.Invert());
        }

        /// <summary>
        /// KeyRange.Invert test 2
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Invert test 2 (range with min and max)")]
        public void TestKeyRangeInvert2()
        {
            var keyrange = new KeyRange<int>(Key<int>.CreateKey(1, false), Key<int>.CreateKey(2, true));
            Assert.AreEqual(keyrange.Invert(), KeyRange<int>.OpenRange);
        }

        /// <summary>
        /// KeyRange.Invert test 3
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Invert test 3 (range with min)")]
        public void TestKeyRangeInvert3()
        {
            var keyrange = new KeyRange<int>(Key<int>.CreateKey(1, false), null);
            Assert.AreEqual(keyrange.Invert(), new KeyRange<int>(null, Key<int>.CreateKey(1, true)));
        }

        /// <summary>
        /// KeyRange.Invert test 4
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Invert test 4 (range with max)")]
        public void TestKeyRangeInvert4()
        {
            var keyrange = new KeyRange<int>(null, Key<int>.CreateKey(2, true));
            Assert.AreEqual(keyrange.Invert(), new KeyRange<int>(Key<int>.CreateKey(2, false), null));
        }

        /// <summary>
        /// KeyRange.Invert test 5
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Invert test 5 (range with prefix)")]
        public void TestKeyRangeInvert5()
        {
            var keyrange = new KeyRange<string>(null, Key<string>.CreatePrefixKey("z"));
            Assert.AreEqual(KeyRange<string>.OpenRange, keyrange.Invert());
        }

        /// <summary>
        /// KeyRange.Invert test 6
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Invert test 6 (range with prefix)")]
        public void TestKeyRangeInvert6()
        {
            var keyrange = new KeyRange<string>(Key<string>.CreatePrefixKey("a"), null);
            Assert.AreEqual(KeyRange<string>.OpenRange, keyrange.Invert());
        }

        /// <summary>
        /// KeyRange.Invert test 7
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Invert test 7 (empty range)")]
        public void TestKeyRangeInvert7()
        {
            Assert.AreEqual(KeyRange<string>.EmptyRange, KeyRange<string>.EmptyRange.Invert());
        }

        #endregion

        #region Intersect tests

        /// <summary>
        /// KeyRange intersect test 1
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 1 (empty ranges)")]
        public void TestKeyRangeIntersect1()
        {
            var nullRange = KeyRange<int>.OpenRange;
            KeyRangeIntersectionHelper(nullRange, nullRange, nullRange);
        }

        /// <summary>
        /// KeyRange intersect test 2
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 2 (range with min and empty range)")]
        public void TestKeyRangeIntersect2()
        {
            var range1 = KeyRange<int>.OpenRange;
            var range2 = new KeyRange<int>(Key<int>.CreateKey(1, false), null);
            KeyRangeIntersectionHelper(range1, range2, range2);
        }

        /// <summary>
        /// KeyRange intersect test 3
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 3 (range with max and empty range)")]
        public void TestKeyRangeIntersect3()
        {
            var range1 = KeyRange<int>.OpenRange;
            var range2 = new KeyRange<int>(null, Key<int>.CreateKey(7, true));
            KeyRangeIntersectionHelper(range1, range2, range2);
        }

        /// <summary>
        /// KeyRange intersect test 4
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 4 (range with min+max and empty range)")]
        public void TestKeyRangeIntersect4()
        {
            var range1 = KeyRange<int>.OpenRange;
            var range2 = new KeyRange<int>(Key<int>.CreateKey(3, false), Key<int>.CreateKey(7, true));
            KeyRangeIntersectionHelper(range1, range2, range2);
        }

        /// <summary>
        /// KeyRange intersect test 5
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 5 (range with min and range with max)")]
        public void TestKeyRangeIntersect5()
        {
            var range1 = new KeyRange<int>(null, Key<int>.CreateKey(7, true));
            var range2 = new KeyRange<int>(Key<int>.CreateKey(3, false), null);
            var expected = new KeyRange<int>(Key<int>.CreateKey(3, false), Key<int>.CreateKey(7, true));
            KeyRangeIntersectionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange intersect test 6
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 6 (equal ranges)")]
        public void TestKeyRangeIntersect6()
        {
            var range = new KeyRange<int>(Key<int>.CreateKey(6, true), Key<int>.CreateKey(7, true));
            KeyRangeIntersectionHelper(range, range, range);
        }

        /// <summary>
        /// KeyRange intersect test 7
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 7 (ranges with same values)")]
        public void TestKeyRangeIntersect7()
        {
            var range1 = new KeyRange<int>(Key<int>.CreateKey(3, true), Key<int>.CreateKey(7, false));
            var range2 = new KeyRange<int>(Key<int>.CreateKey(3, false), Key<int>.CreateKey(7, true));
            var expected = new KeyRange<int>(Key<int>.CreateKey(3, false), Key<int>.CreateKey(7, false));
            KeyRangeIntersectionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange intersect test 8
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 8 (different ranges)")]
        public void TestKeyRangeIntersect8()
        {
            var range1 = new KeyRange<int>(Key<int>.CreateKey(2, true), Key<int>.CreateKey(7, true));
            var range2 = new KeyRange<int>(Key<int>.CreateKey(3, true), Key<int>.CreateKey(8, true));
            var expected = new KeyRange<int>(Key<int>.CreateKey(3, true), Key<int>.CreateKey(7, true));
            KeyRangeIntersectionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange intersect test 9
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 9 (prefix vs inclusive)")]
        public void TestKeyRangeIntersect9()
        {
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreateKey("b", true));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreatePrefixKey("b"));
            var expected = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreateKey("b", true));
            KeyRangeIntersectionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange intersect test 10
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 10 (prefix vs exclusive)")]
        public void TestKeyRangeIntersect10()
        {
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreateKey("b", false));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreatePrefixKey("b"));
            var expected = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreateKey("b", false));
            KeyRangeIntersectionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange intersect test 11
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 11 (separate ranges)")]
        public void TestKeyRangeIntersect11()
        {
            var range1 = new KeyRange<int>(Key<int>.CreateKey(8, true), Key<int>.CreateKey(10, true));
            var range2 = new KeyRange<int>(Key<int>.CreateKey(12, true), Key<int>.CreateKey(14, true));
            KeyRangeIntersectionHelper(range1, range2, KeyRange<int>.EmptyRange);
        }

        /// <summary>
        /// KeyRange intersect test 12
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 12 (separate ranges with inclusive/exclusive)")]
        public void TestKeyRangeIntersect12()
        {
            var range1 = new KeyRange<int>(Key<int>.CreateKey(8, true), Key<int>.CreateKey(10, true));
            var range2 = new KeyRange<int>(Key<int>.CreateKey(10, false), Key<int>.CreateKey(14, true));
            KeyRangeIntersectionHelper(range1, range2, KeyRange<int>.EmptyRange);
        }

        /// <summary>
        /// KeyRange intersect test 13
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 13 (overlapping ranges with inclusive/exclusive)")]
        public void TestKeyRangeIntersect13()
        {
            var range1 = new KeyRange<int>(Key<int>.CreateKey(8, true), Key<int>.CreateKey(10, true));
            var range2 = new KeyRange<int>(Key<int>.CreateKey(10, true), Key<int>.CreateKey(14, true));
            var expected = new KeyRange<int>(Key<int>.CreateKey(10, true), Key<int>.CreateKey(10, true));
            Assert.IsFalse(expected.IsEmpty);
            KeyRangeIntersectionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange intersect test 14
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 14 (overlapping ranges with prefix)")]
        public void TestKeyRangeIntersect14()
        {
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreatePrefixKey("x"));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("x", true), null);
            var expected = new KeyRange<string>(Key<string>.CreateKey("x", true), Key<string>.CreatePrefixKey("x"));
            Assert.IsFalse(expected.IsEmpty);
            KeyRangeIntersectionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange intersect test 15
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 15 (string range with min+max and empty range)")]
        public void TestKeyRangeIntersect15()
        {
            var range1 = KeyRange<string>.OpenRange;
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("z"));
            KeyRangeIntersectionHelper(range1, range2, range2);
        }

        /// <summary>
        /// KeyRange intersect test 16
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 16 (string range with prefix/non-prefix)")]
        public void TestKeyRangeIntersect16()
        {
            // The longer prefix is more restrictive, so we want it
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("z"));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("zz"));
            KeyRangeIntersectionHelper(range1, range2, range2);
        }

        /// <summary>
        /// KeyRange intersect test 17
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 17 (string range with prefix/non-prefix)")]
        public void TestKeyRangeIntersect17()
        {
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("z"));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreateKey("z", true));
            KeyRangeIntersectionHelper(range1, range2, range2);
        }

        /// <summary>
        /// KeyRange intersect test 18
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 18 (string range with prefix/prefix)")]
        public void TestKeyRangeIntersect18()
        {
            // The longer prefix is more restrictive, so we want it
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("ba"));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("bb"));
            KeyRangeIntersectionHelper(range1, range2, range1);
        }

        /// <summary>
        /// KeyRange intersect test 19
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 19 (string range with prefix/non-prefix)")]
        public void TestKeyRangeIntersect19()
        {
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("y"));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreateKey("x", true));
            KeyRangeIntersectionHelper(range1, range2, range2);
        }

        /// <summary>
        /// KeyRange intersect test 20
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 20 (string range with prefix/prefix)")]
        public void TestKeyRangeIntersect20()
        {
            // The longer prefix is more restrictive, so we want it
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("abc"));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("wxyz"));
            KeyRangeIntersectionHelper(range1, range2, range1);
        }

        /// <summary>
        /// KeyRange intersect test 21
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 21 (empty range and non-empty range)")]
        public void TestKeyRangeIntersect21()
        {
            var range1 = KeyRange<int>.EmptyRange;
            var range2 = new KeyRange<int>(null, Key<int>.CreateKey(7, true));
            KeyRangeIntersectionHelper(range1, range2, KeyRange<int>.EmptyRange);
        }

        /// <summary>
        /// KeyRange intersect test 22
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Intersect test 22 (string range with prefix/non-prefix)")]
        public void TestKeyRangeIntersect22()
        {
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("b"));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreateKey("x", true));
            KeyRangeIntersectionHelper(range1, range2, range1);
        }

        #endregion

        #region Union tests

        /// <summary>
        /// KeyRange union test 1
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 1 (open ranges)")]
        public void TestKeyRangeUnion1()
        {
            KeyRangeUnionHelper(KeyRange<int>.OpenRange, KeyRange<int>.OpenRange, KeyRange<int>.OpenRange);
        }

        /// <summary>
        /// KeyRange union test 2
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 2 (range with min and open range)")]
        public void TestKeyRangeUnion2()
        {
            var range2 = new KeyRange<int>(Key<int>.CreateKey(1, false), null);
            KeyRangeUnionHelper(KeyRange<int>.OpenRange, range2, KeyRange<int>.OpenRange);
        }

        /// <summary>
        /// KeyRange union test 3
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 3 (range with max and open range)")]
        public void TestKeyRangeUnion3()
        {
            var range2 = new KeyRange<int>(null, Key<int>.CreateKey(7, true));
            KeyRangeUnionHelper(KeyRange<int>.OpenRange, range2, KeyRange<int>.OpenRange);
        }

        /// <summary>
        /// KeyRange union test 4
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 4 (range with min+max and empty range)")]
        public void TestKeyRangeUnion4()
        {
            var range2 = new KeyRange<int>(Key<int>.CreateKey(3, false), Key<int>.CreateKey(7, true));
            KeyRangeUnionHelper(KeyRange<int>.OpenRange, range2, KeyRange<int>.OpenRange);
        }

        /// <summary>
        /// KeyRange union test 5
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 5 (range with min and range with max)")]
        public void TestKeyRangeUnion5()
        {
            var range1 = new KeyRange<int>(null, Key<int>.CreateKey(7, true));
            var range2 = new KeyRange<int>(Key<int>.CreateKey(3, false), null);
            KeyRangeUnionHelper(range1, range2, KeyRange<int>.OpenRange);
        }

        /// <summary>
        /// KeyRange union test 6
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 6 (equal ranges)")]
        public void TestKeyRangeUnion6()
        {
            var range = new KeyRange<int>(Key<int>.CreateKey(6, true), Key<int>.CreateKey(7, true));
            KeyRangeUnionHelper(range, range, range);
        }

        /// <summary>
        /// KeyRange union test 7
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 7 (ranges with same values)")]
        public void TestKeyRangeUnion7()
        {
            var range1 = new KeyRange<int>(Key<int>.CreateKey(3, true), Key<int>.CreateKey(7, false));
            var range2 = new KeyRange<int>(Key<int>.CreateKey(3, false), Key<int>.CreateKey(7, true));
            var expected = new KeyRange<int>(Key<int>.CreateKey(3, true), Key<int>.CreateKey(7, true));
            KeyRangeUnionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange union test 8
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 8 (different ranges)")]
        public void TestKeyRangeUnion8()
        {
            var range1 = new KeyRange<int>(Key<int>.CreateKey(2, true), Key<int>.CreateKey(7, true));
            var range2 = new KeyRange<int>(Key<int>.CreateKey(3, true), Key<int>.CreateKey(8, true));
            var expected = new KeyRange<int>(Key<int>.CreateKey(2, true), Key<int>.CreateKey(8, true));
            KeyRangeUnionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange union test 9
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 9 (prefix vs inclusive)")]
        public void TestKeyRangeUnion9()
        {
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreateKey("b", true));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreatePrefixKey("b"));
            var expected = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreatePrefixKey("b"));
            KeyRangeUnionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange union test 10
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 10 (prefix vs exclusive)")]
        public void TestKeyRangeUnion10()
        {
            var range1 = new KeyRange<string>(Key<string>.CreateKey("b", false), Key<string>.CreateKey("c", false));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreatePrefixKey("c"));
            var expected = new KeyRange<string>(Key<string>.CreateKey("a", true), Key<string>.CreatePrefixKey("c"));
            KeyRangeUnionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange union test 11
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 11 (string range with min+max and empty range)")]
        public void TestKeyRangeUnion11()
        {
            var range = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreateKey("b", true));
            KeyRangeUnionHelper(KeyRange<string>.EmptyRange, range, range);
        }

        /// <summary>
        /// KeyRange union test 12
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 12 (two prefixes)")]
        public void TestKeyRangeUnion12()
        {
            // The shorter prefix matches more records so we want to use it
            var range1 = new KeyRange<string>(Key<string>.CreateKey("b", false), Key<string>.CreatePrefixKey("c"));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("cc"));
            var expected = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("c"));
            KeyRangeUnionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange union test 13
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 13 (prefix/non-prefix)")]
        public void TestKeyRangeUnion13()
        {
            var range1 = new KeyRange<string>(Key<string>.CreateKey("b", false), Key<string>.CreateKey("c", true));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("cc"));
            var expected = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("cc"));
            KeyRangeUnionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange union test 14
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 14 (non-prefix/prefix)")]
        public void TestKeyRangeUnion14()
        {
            // The prefix matches more records so we want to use it
            var range1 = new KeyRange<string>(Key<string>.CreateKey("b", false), Key<string>.CreateKey("cc", true));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("c"));
            var expected = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("c"));
            KeyRangeUnionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange union test 15
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 15 (two prefixes)")]
        public void TestKeyRangeUnion15()
        {
            // The shorter prefix matches more records so we want to use it
            var range1 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("abcd"));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("xyz"));
            var expected = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("xyz"));
            KeyRangeUnionHelper(range1, range2, expected);
        }

        /// <summary>
        /// KeyRange union test 16
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("KeyRange.Union test 16 (non-prefix/prefix)")]
        public void TestKeyRangeUnion16()
        {
            // The prefix matches more records so we want to use it
            var range1 = new KeyRange<string>(Key<string>.CreateKey("m", false), Key<string>.CreateKey("z", false));
            var range2 = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("n"));
            var expected = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreateKey("z", false));
            KeyRangeUnionHelper(range1, range2, expected);
        }

        #endregion

        /// <summary>
        /// Helper function to check KeyRange intersection.
        /// </summary>
        /// <typeparam name="T">The type of the KeyRange</typeparam>
        /// <param name="range1">The first range.</param>
        /// <param name="range2">The second range.</param>
        /// <param name="expected">The result of the intersection.</param>
        private static void KeyRangeIntersectionHelper<T>(KeyRange<T> range1, KeyRange<T> range2, KeyRange<T> expected) where T : IComparable<T>
        {
            Assert.AreEqual(expected, range1 & range2);
            Assert.AreEqual(expected, range2 & range1);
        }

        /// <summary>
        /// Helper function to check KeyRange union.
        /// </summary>
        /// <typeparam name="T">The type of the KeyRange</typeparam>
        /// <param name="range1">The first range.</param>
        /// <param name="range2">The second range.</param>
        /// <param name="expected">The result of the union.</param>
        private static void KeyRangeUnionHelper<T>(KeyRange<T> range1, KeyRange<T> range2, KeyRange<T> expected) where T : IComparable<T>
        {
            Assert.AreEqual(expected, range1 | range2);
            Assert.AreEqual(expected, range2 | range1);
        }
    }
}