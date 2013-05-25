// --------------------------------------------------------------------------------------------------------------------
// <copyright file="KeyValueExpressionEvaluatorTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test evaluation of KeyValue expressions.
    /// </summary>
    [TestClass]
    public class KeyValueExpressionEvaluatorTests
    {
        /// <summary>
        /// Const member used for tests.
        /// </summary>
        private const uint ConstMember = 18U;

        /// <summary>
        /// Static member used for tests.
        /// </summary>
        private static ulong staticMember;

        /// <summary>
        /// Member used for tests.
        /// </summary>
        private int member;

        #region KeyRangeIsExact

        /// <summary>
        /// Verify that KeyExpressionEvaluator.KeyRangeIsExact throws an exception when its argument is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that KeyExpressionEvaluator.KeyRangeIsExact throws an exception when its argument is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeyRangeIsExactThrowsExceptionWhenArgumentIsNull()
        {
            bool ignored = KeyExpressionEvaluator<short>.KeyRangeIsExact(null);
        }

        /// <summary>
        /// Verify that KeyValueExpressionEvaluator.KeyRangeIsExact throws an exception when its argument is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that .KeyValueExpressionEvaluatorKeyRangeIsExact throws an exception when its argument is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeyValueRangeIsExactThrowsExceptionWhenArgumentIsNull()
        {
            bool ignored = KeyValueExpressionEvaluator<short, decimal>.KeyRangeIsExact(null);
        }

        /// <summary>
        /// Verify that a key comparison is exact.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a key comparison gives an exact range")]
        public void VerifyKeyComparisonIsExact()
        {
            Assert.IsTrue(KeyValueExpressionEvaluator<int, string>.KeyRangeIsExact(x => x.Key > 0));
        }

        /// <summary>
        /// Verify that a value comparison is not exact.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a value comparison does not give an exact range")]
        public void VerifyValueComparisonIsNotExact()
        {
            Assert.IsFalse(KeyValueExpressionEvaluator<string, int>.KeyRangeIsExact(x => x.Value > 0));
        }

        /// <summary>
        /// Verify that a call to Equals is exact.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a call to Equals is exact")]
        public void VerifyEqualsWithIsExact()
        {
            Assert.IsTrue(KeyValueExpressionEvaluator<short, string>.KeyRangeIsExact(x => x.Key.Equals(7)));
        }

        /// <summary>
        /// Verify that a call to String.StartsWith is exact.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a call to String.StartsWith is exact")]
        public void VerifyStringStartsWithIsExact()
        {
            Assert.IsTrue(KeyValueExpressionEvaluator<string, int>.KeyRangeIsExact(x => x.Key.StartsWith("foo")));
        }

        /// <summary>
        /// Verify that a call to the static String.Equals is exact.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a call to the static String.Equals is exact")]
        public void VerifyStringEqualsWithIsExact()
        {
            Assert.IsTrue(KeyValueExpressionEvaluator<string, int>.KeyRangeIsExact(x => String.Equals(x.Key, "foo")));
        }

        /// <summary>
        /// Verify that a call to String.Contains is not exact.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a call to String.Contains is not exact")]
        public void VerifyStringContainsIsNotExact()
        {
            Assert.IsFalse(KeyValueExpressionEvaluator<string, int>.KeyRangeIsExact(x => x.Key.Contains("foo")));
        }

        /// <summary>
        /// Verify that the AND of two exact index ranges is exact.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the AND of two exact index ranges is exact")]
        public void VerifyAndOfRangesIsExact()
        {
            Assert.IsTrue(KeyValueExpressionEvaluator<int, int>.KeyRangeIsExact(x => x.Key > 3 && x.Key < 4));
        }

        /// <summary>
        /// Verify that the OR of two exact index ranges is not exact.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the OR of two exact index ranges is not exact")]
        public void VerifyAndOfRangesIsNotExact()
        {
            Assert.IsFalse(KeyValueExpressionEvaluator<int, int>.KeyRangeIsExact(x => x.Key > 3 || x.Key < 4));
        }

        /// <summary>
        /// Verify that the AND of and exact range and a non-exact range is not exact.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the AND of and exact range and a non-exact range is not exact")]
        public void VerifyAndOfExactAndNonExactRangesIsNotExact()
        {
            Assert.IsFalse(KeyValueExpressionEvaluator<int, int>.KeyRangeIsExact(x => x.Key > 3 && x.Value < 4));
            Assert.IsFalse(KeyValueExpressionEvaluator<int, int>.KeyRangeIsExact(x => x.Value > 3 && x.Key < 4));
        }

        #endregion

        /// <summary>
        /// Verify that KeyExpressionEvaluator.GetKeyRange throws an exception when its argument is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that KeyExpressionEvaluator.GetKeyRange throws an exception when its argument is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeyExpressionEvaluatorThrowsExceptionWhenArgumentIsNull()
        {
            KeyRange<DateTime> keyRange = KeyExpressionEvaluator<DateTime>.GetKeyRange(null);
        }

        /// <summary>
        /// Verify that KeyValueExpressionEvaluator.GetKeyRange throws an exception when its argument is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that KeyValueExpressionEvaluator.GetKeyRange throws an exception when its argument is null")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyKeyValueExpressionEvaluatorThrowsExceptionWhenArgumentIsNull()
        {
            KeyRange<short> keyRange = KeyValueExpressionEvaluator<short, decimal>.GetKeyRange(null);
        }

        /// <summary>
        /// Verify that a true expression gives an open range.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a true expression gives an open range")]
        public void VerifyTrueExpressionGivesOpenRange()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => true);
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify that an expression without ranges gives an open range.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an expression without ranges gives an open range")]
        public void VerifyExpressionWithoutRangeGivesOpenRange()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => (0 == (x.Key % 2)));
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify that an expression on the value gives an open range.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an expression without ranges gives an open range")]
        public void VerifyValueExpressionGivesNoLimits()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, int>.GetKeyRange(x => x.Value < 100);
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify that a LT comparison gives a max limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a < comparison gives a max limit")]
        public void VerifyLtComparisonGivesMaxLimit()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key < 10);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(10, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify that a LE comparison gives a max limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a <= comparison gives a max limit")]
        public void VerifyLeComparisonGivesMaxLimit()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key <= 11);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(11, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify that a LT comparison gives a max limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a reversed < comparison gives a max limit")]
        public void VerifyLtComparisonReversedGivesMaxLimit()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => 10 > x.Key);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(10, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify that a LE comparison gives a max limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a reversed <= comparison gives a max limit")]
        public void VerifyLeComparisonReversedGivesMaxLimit()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => 11 >= x.Key);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(11, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify that multiple LT comparisons give a max limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that multiple < comparisons give a max limit")]
        public void VerifyLtCollapse()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key < 3 && x.Key < 2 && x.Key < 5);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(2, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify that multiple LE comparisons give a max limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that multiple <= comparisons give a max limit")]
        public void VerifyLeCollapse()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key <= 1 && 2 >= x.Key && x.Key <= 3);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(1, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify that LT and LE comparisons give a max limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that < and <= comparisons give a max limit")]
        public void VerifyLtAndLeCollapse()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key <= 11 && x.Key < 11);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(11, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify that a GT comparison gives a min limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a > comparison gives a min limit")]
        public void VerifyGtComparisonGivesMinLimit()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key > 10);
            Assert.AreEqual(10, keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Verify that a GE comparison gives a min limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a >= comparison gives a min limit")]
        public void VerifyGeComparisonGivesMinLimit()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key >= 11);
            Assert.AreEqual(11, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Verify that a GT comparison gives a min limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a reversed > comparison gives a min limit")]
        public void VerifyGtComparisonReversedGivesMinLimit()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => 10 < x.Key);
            Assert.AreEqual(10, keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Verify that a GE comparison gives a min limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a reversed >= comparison gives a min limit")]
        public void VerifyGeComparisonReversedGivesMinLimit()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => 11 <= x.Key);
            Assert.AreEqual(11, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Verify that multiple GT comparisons give a min limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that multiple > comparisons give a min limit")]
        public void VerifyGtCollapse()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key > 3 && x.Key > 2 && x.Key > 5);
            Assert.AreEqual(5, keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Verify that multiple GE comparisons give a min limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that multiple >= comparisons give a min limit")]
        public void VerifyGeCollapse()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key >= 1 && 2 <= x.Key && x.Key >= 3);
            Assert.AreEqual(3, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Verify that GT and GE comparisons give a min limit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that > and >= comparisons give a min limit")]
        public void VerifyGtAndGeCollapse()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key >= 11 && x.Key > 11);
            Assert.AreEqual(11, keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Verify that an == comparison gives upper and lower limits.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an == comparison gives upper and lower limits")]
        public void VerifyEqGivesMinAndMaxLimits()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key == 7);
            Assert.AreEqual(7, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(7, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify that a reversed == comparison gives upper and lower limits.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that a reversed == comparison gives upper and lower limits")]
        public void VerifyEqReversedGivesMinAndMaxLimits()
        {
            KeyRange<long> keyRange = KeyValueExpressionEvaluator<long, string>.GetKeyRange(x => 8 == x.Key);
            Assert.AreEqual(8L, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(8L, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify that LT and GT comparisons give min and max limits.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify < and > comparisons give min and max limits")]
        public void VerifyLtAndGtComparisonsGiveMinAndMaxLimits()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => 19 < x.Key && x.Key < 101);
            Assert.AreEqual(19, keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.AreEqual(101, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify an AND clause still produces limits.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify an AND clause still produces limits")]
        public void VerifyAndStillGivesLimits()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => 19 <= x.Key && x.Key <= 101 && x.Value.StartsWith("foo"));
            Assert.AreEqual(19, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(101, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify an AND clause intersects limits.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify an AND clause intersects limits")]
        public void VerifyAndIntersectsLimits()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => 19 <= x.Key && x.Key <= 101 && 21 < x.Key && x.Key < 99);
            Assert.AreEqual(21, keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.AreEqual(99, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify an OR clause unions limits.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify an OR clause unions limits")]
        public void VerifyOrUnionsLimits()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => (19 <= x.Key && x.Key <= 101) || x.Key > 200);
            Assert.AreEqual(19, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Verify an OR clause removes limits.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify an OR clause removes limits")]
        public void VerifyOrRemovesLimits()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => (19 <= x.Key && x.Key <= 101) || x.Value.StartsWith("foo"));
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify local variable evaluation.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify local variable evaluation")]
        public void VerifyLocalVariableEvaluation()
        {
            int i = 29;
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, long>.GetKeyRange(x => x.Key <= i);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(i, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify member variable evaluation.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify member variable evaluation")]
        public void VerifyMemberVariableEvaluation()
        {
            this.member = 18;
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, long>.GetKeyRange(x => x.Key <= this.member);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(this.member, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify that key access only works for the parameter.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify key access only works for the parameter")]
        public void VerifyKeyAccessIsForParameterOnly()
        {
            var k = new KeyValuePair<int, int>(1, 2);
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, int>.GetKeyRange(x => k.Key == x.Key);
            Assert.AreEqual(1, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(1, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test key access against the key.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test key access against the key")]
        public void TestKeyAccessAgainstSelf()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, int>.GetKeyRange(x => x.Key == x.Key);
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify conditional access is optimized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify conditional access is optimized")]
        public void VerifyConditionalParameterAccessIsOptimized()
        {
            KeyValuePair<int, string> kvp1 = new KeyValuePair<int, string>(0, "hello");
            KeyValuePair<int, string> kvp2 = new KeyValuePair<int, string>(1, "hello");
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key < (0 == DateTime.Now.Ticks ? kvp1 : kvp2).Key);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(1, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify conditional parameter access is recognized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify conditional parameter access is recognized")]
        public void VerifyConditionalParameterAccessIsRecognized()
        {
            KeyValuePair<int, string> kvp = new KeyValuePair<int, string>(0, "hello");
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key < (0 == DateTime.Now.Ticks ? x : kvp).Key);
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify array access is optimized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify array access is optimized")]
        public void VerifyArrayAccessIsOptimized()
        {
            KeyValuePair<int, string> kvp = new KeyValuePair<int, string>(1, "hello");
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key < (new[] { kvp })[0].Key);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(1, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify array parameter access is recognized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify array parameter access is recognized")]
        public void VerifyArrayParameterAccessIsRecognized()
        {
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key < (new[] { x })[0].Key);
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify delegate access is optimized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify delegate access is optimized")]
        public void VerifyDelegateAccessIsOptimized()
        {
            Func<int, int> f = x => x * 2;
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key <= f(1));
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(2, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify delegate parameter access is recognized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify delegate parameter access is recognized")]
        public void VerifyDelegateParameterAccessIsRecognized()
        {
            Func<KeyValuePair<int, string>, int> f = x => x.Key;
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key < f(x));
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify method call parameter access is optimized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify method call access is optimized")]
        public void VerifyMethodCallAccessIsOptimized()
        {
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key < (String.IsNullOrEmpty("foo") ? 0 : 1));
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(1, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify static method call parameter access is optimized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify static method call access is optimized")]
        public void VerifyStaticMethodCallAccessIsOptimized()
        {
            var expected = new KeyRange<int>(null, Key<int>.CreateKey(8, false));
            var actual =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key < Math.Min(8, 9));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Verify method call parameter access is recognized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify method call parameter access is recognized")]
        public void VerifyMethodCallParameterAccessIsRecognized()
        {
            Func<KeyValuePair<int, string>, int> f = x => x.Key;
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key < (String.IsNullOrEmpty(x.Value) ? 0 : 1));
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify method call parameter access is recognized (2).
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify method call parameter access is recognized (2)")]
        public void VerifyMethodCallParameterAccessIsRecognized2()
        {
            Func<KeyValuePair<int, string>, int> f = x => x.Key;
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key < Math.Max(0, "foo".StartsWith("f") ? 10 : x.Key));
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify method call object access is recognized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify method call object access is recognized")]
        public void VerifyMethodCallObjectAccessIsRecognized()
        {
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(
                    x => x.Key < x.Key.GetHashCode());
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        #region Nullable Types

        /// <summary>
        /// Verify comparing the key with a nullable type is recognized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify comparing the key with a nullable type is recognized")]
        public void VerifyComparisonWithNullableTypeIsRecognized()
        {
            int? min = -99;
            int? max = 99;
            KeyRange<int> actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => min < x.Key && x.Key < max);
            KeyRange<int> expected = new KeyRange<int>(Key<int>.CreateKey(-99, false), Key<int>.CreateKey(99, false));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Verify comparing the key with a null nullable type works.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify comparing the key with a null nullable type works")]
        public void VerifyComparisonWithNullWorks()
        {
            int? min = -99;
            int? max = null;
            KeyRange<int> actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => min < x.Key && x.Key < max);
            KeyRange<int> expected = new KeyRange<int>(Key<int>.CreateKey(-99, false), null);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Verify comparing the key with a complex nullable constant works.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify comparing the key with a complex nullable constant works")]
        public void VerifyComparisonWithComplexNullable()
        {
            int? min = -99;
            int? max = 1;
            KeyRange<int> actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => min < x.Key && x.Key < max + 8);
            KeyRange<int> expected = new KeyRange<int>(Key<int>.CreateKey(-99, false), Key<int>.CreateKey(9, false));
            Assert.AreEqual(expected, actual);
        }

        #endregion

        #region NOT handling

        /// <summary>
        /// Verify a NOT of an EQ removes limits.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a NOT of == removes limits")]
        public void VerifyNotOfEqRemovesLimits()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => !(x.Key == 3));
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Verify a NOT of an NE doesn't work.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a NOT of != works")]
        public void VerifyNotOfNe()
        {
            KeyRange<int> actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => !(x.Key != 3));
            KeyRange<int> expected = new KeyRange<int>(Key<int>.CreateKey(3, true), Key<int>.CreateKey(3, true));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Verify a NOT of an LT gives GE.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a NOT of < gives >=")]
        public void VerifyNotOfLtGivesGe()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => !(x.Key < 4));
            Assert.AreEqual(4, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Verify a NOT of an LE gives GT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a NOT of <= gives >")]
        public void VerifyNotOfLeGivesGt()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => !(x.Key <= 4));
            Assert.AreEqual(4, keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Verify a NOT of an GT gives LE.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a NOT of > gives <=")]
        public void VerifyNotOfGtGivesLe()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => !(x.Key > 4));
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(4, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify a NOT of an GE gives LT.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify a NOT of >= gives <")]
        public void VerifyNotOfGeGivesLt()
        {
            KeyRange<int> actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => !(x.Key >= 4));
            KeyRange<int> expected = new KeyRange<int>(null, Key<int>.CreateKey(4, false));
        }

        /// <summary>
        /// Verify DeMorgans law works for OR.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify DeMorgans law works for ||")]
        public void VerifyDeMorgansForOr()
        {
            // This should be the same as (x.Key >= 11 && x.Key < 22)
            KeyRange<int> actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => !(x.Key < 11 || x.Key >= 22));
            KeyRange<int> expected = new KeyRange<int>(Key<int>.CreateKey(11, true), Key<int>.CreateKey(22, false));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Verify DeMorgans law works for AND.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify DeMorgans law works for &&")]
        public void VerifyDeMorgansForAnd()
        {
            // This should be the same as (x.Key > 11 || x.Key > 22)
            KeyRange<int> actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => !(x.Key <= 11 && x.Key <= 22));
            KeyRange<int> expected = new KeyRange<int>(Key<int>.CreateKey(11, false), null);
            Assert.AreEqual(expected, actual);
        }

        #endregion

        #region Constant Folding

        /// <summary>
        /// Verify constant folding with functions.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding with functions")]
        public void VerifyConstantFoldingWithFunctions()
        {
            var rnd = new Random();
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, long>.GetKeyRange(x => x.Key <= rnd.Next());
            Assert.IsNull(keyRange.Min);
            Assert.IsNotNull(keyRange.Max);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Verify constant folding with add.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (add)")]
        public void VerifyConstantFoldingAdd()
        {
            short i = 22;
            ConstantFoldingHelper(x => x.Key <= i + 10);
        }

        /// <summary>
        /// Verify constant folding with checked add.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (checked add)")]
        public void VerifyConstantFoldingCheckedAdd()
        {
            short i = 22;
            ConstantFoldingHelper(x => x.Key <= checked(i + 10));
        }

        /// <summary>
        /// Verify constant folding with minus.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (minus)")]
        public void VerifyConstantFoldingMinus()
        {
            int i = 42;
            ConstantFoldingHelper(x => x.Key <= i - 10);
        }

        /// <summary>
        /// Verify constant folding with checked minus.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (checked minus)")]
        public void VerifyConstantFoldingCheckedMinus()
        {
            int i = 42;
            ConstantFoldingHelper(x => x.Key <= checked(i - 10));
        }

        /// <summary>
        /// Verify constant folding with multiply.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (multiply)")]
        public void VerifyConstantFoldingMultiply()
        {
            int i = 16;
            ConstantFoldingHelper(x => x.Key <= i * 2);
        }

        /// <summary>
        /// Verify constant folding with checked multiply.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (checked multiply)")]
        public void VerifyConstantFoldingCheckedMultiply()
        {
            int i = 16;
            ConstantFoldingHelper(x => x.Key <= checked(i * 2));
        }

        /// <summary>
        /// Verify constant folding with divide.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (divide)")]
        public void VerifyConstantFoldingDivide()
        {
            int i = 320;
            ConstantFoldingHelper(x => x.Key <= i / 10);
        }

        /// <summary>
        /// Verify constant folding with checked divide.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (checked divide)")]
        public void VerifyConstantFoldingCheckedDivide()
        {
            int i = 320;
            ConstantFoldingHelper(x => x.Key <= checked(i / 10));
        }

        /// <summary>
        /// Verify constant folding with negate.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (negate)")]
        public void VerifyConstantFoldingNegate()
        {
            int i = -32;
            ConstantFoldingHelper(x => x.Key <= -i);
        }

        /// <summary>
        /// Verify constant folding with checked negate.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (checked negate)")]
        public void VerifyConstantFoldingCheckedNegate()
        {
            int i = -32;
            ConstantFoldingHelper(x => x.Key <= checked(-i));
        }

        /// <summary>
        /// Verify constant folding with mod.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (mod)")]
        public void VerifyConstantFoldingMod()
        {
            int i = 128;
            ConstantFoldingHelper(x => x.Key <= 32 % i);
        }

        /// <summary>
        /// Verify constant folding with right shift.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (right shift)")]
        public void VerifyConstantFoldingRightShift()
        {
            int i = 128;
            int j = 2;
            ConstantFoldingHelper(x => x.Key <= i >> j);
        }

        /// <summary>
        /// Verify constant folding with left shift.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (left shift)")]
        public void VerifyConstantFoldingLeftShift()
        {
            int i = 16;
            const int One = 1;
            ConstantFoldingHelper(x => x.Key <= i << One);
        }

        /// <summary>
        /// Verify constant folding with and.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (and)")]
        public void VerifyConstantFoldingAnd()
        {
            int i = 33;
            ConstantFoldingHelper(x => x.Key <= (i & 32));
        }

        /// <summary>
        /// Verify constant folding with or.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (or)")]
        public void VerifyConstantFoldingOr()
        {
            int i = 0;
            ConstantFoldingHelper(x => x.Key <= (i | 32));
        }

        /// <summary>
        /// Verify constant folding with xor.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (xor)")]
        public void VerifyConstantFoldingXor()
        {
            int i = 33;
            int j = 1;
            ConstantFoldingHelper(x => x.Key <= (i ^ j));
        }

        /// <summary>
        /// Verify constant folding with not.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding (not)")]
        public void VerifyConstantFoldingNot()
        {
            int i = ~32;
            ConstantFoldingHelper(x => x.Key <= ~i);
        }

        /// <summary>
        /// Verify constant folding with invalid types.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding with an invalid type")]
        public void VerifyConstantFoldingInvalidType()
        {
            KeyRange<double> keyRange = KeyExpressionEvaluator<double>.GetKeyRange(x => x.Equals("foo"));
            Assert.AreEqual(keyRange, KeyRange<double>.OpenRange);
        }

        /// <summary>
        /// Verify constant folding with invalid calculation.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding with an invalid calculation")]
        public void VerifyConstantFoldingInvalidCalculation()
        {
            long l = 7;
            KeyRange<string> keyRange = KeyExpressionEvaluator<string>.GetKeyRange(x => x.Equals(l + 5.6));
            Assert.AreEqual(keyRange, KeyRange<string>.OpenRange);
        }

        /// <summary>
        /// Verify constant folding with a function returning an invalid type returned from a method.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding with an invalid type returned from a method")]
        public void VerifyConstantFoldingInvalidReturnType()
        {
            KeyRange<double> keyRange = KeyExpressionEvaluator<double>.GetKeyRange(x => x.Equals(int.MaxValue.ToString()));
            Assert.AreEqual(keyRange, KeyRange<double>.OpenRange);
        }

        /// <summary>
        /// Verify constant folding with a cast.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding with a cast")]
        public void VerifyConstantFoldingCast()
        {
            object o = Math.PI;
            KeyRange<double> keyRange = KeyExpressionEvaluator<double>.GetKeyRange(x => x == (double)o);
            var key = Key<double>.CreateKey(Math.PI, true);
            Assert.AreEqual(keyRange, new KeyRange<double>(key, key));
        }

        /// <summary>
        /// Verify constant folding with a cast of a null value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding with a cast of a null value")]
        public void VerifyConstantFoldingNullCast()
        {
            Func<object> f = () => null;
            KeyRange<double> keyRange = KeyExpressionEvaluator<double>.GetKeyRange(x => x == (double)f());
            Assert.AreEqual(keyRange, KeyRange<double>.OpenRange);
        }

        /// <summary>
        /// Verify constant folding with an invalid cast.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify constant folding with an invalid cast")]
        public void VerifyConstantFoldingInvalidCast()
        {
            object o = Guid.NewGuid();
            KeyRange<double> keyRange = KeyExpressionEvaluator<double>.GetKeyRange(x => x == (double)o);
            Assert.AreEqual(keyRange, KeyRange<double>.OpenRange);
        }

        #endregion

        #region Testing Different Types

        /// <summary>
        /// Test Boolean expression (true).
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Bool expression (true)")]
        public void TestTrueBoolExpression()
        {
            KeyRange<bool> keyRange = KeyValueExpressionEvaluator<bool, string>.GetKeyRange(x => x.Key == true);
            Assert.AreEqual(true, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(true, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
        }

        /// <summary>
        /// Test Boolean expression (false).
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Bool expression (false)")]
        public void TestFalseBoolExpression()
        {
            KeyRange<bool> keyRange = KeyValueExpressionEvaluator<bool, string>.GetKeyRange(x => x.Key == false);
            Assert.AreEqual(false, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(false, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
        }

        /// <summary>
        /// Test Byte expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Byte expression")]
        public void TestByteExpression()
        {
            KeyRange<byte> keyRange = KeyValueExpressionEvaluator<byte, string>.GetKeyRange(x => x.Key < 2);

            // This fails because the Key is promoted and the KeyValueExpressionEvaluator doesn't handle that.
            // Arguably this isn't currently critical because we can only have 2^8 records with this type of key.
            Assert.AreEqual(
                new KeyRange<byte>(null, Key<byte>.CreateKey(2, false)),
                keyRange,
                "Byte promotion not handled");
        }

        /// <summary>
        /// Test Int16 expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Int16 expression")]
        public void TestInt16Expression()
        {
            KeyRange<short> keyRange = KeyValueExpressionEvaluator<short, string>.GetKeyRange(x => x.Key < 2);
            Assert.AreEqual(
                new KeyRange<short>(null, Key<short>.CreateKey(2, false)),
                keyRange,
                "Int16 promotion not handled");
        }

        /// <summary>
        /// Test UInt16 expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test UInt16 expression")]
        public void TestUInt16Expression()
        {
            KeyRange<ushort> keyRange = KeyValueExpressionEvaluator<ushort, string>.GetKeyRange(x => x.Key < 2);
            Assert.AreEqual(
                new KeyRange<ushort>(null, Key<ushort>.CreateKey(2, false)),
                keyRange,
                "UInt16 promotion not handled");
        }

        /// <summary>
        /// Test Int32 expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Int32 expression")]
        public void TestInt32Expression()
        {
            KeyRange<int> keyRange = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key < 2);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(2, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test UInt32 expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test UInt32 expression")]
        public void TestUInt32Expression()
        {
            KeyRange<uint> keyRange = KeyValueExpressionEvaluator<uint, string>.GetKeyRange(x => x.Key < 2);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(2U, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test Int64 expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Int64 expression")]
        public void TestInt64Expression()
        {
            KeyRange<long> keyRange = KeyValueExpressionEvaluator<long, string>.GetKeyRange(x => x.Key < 2);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(2L, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test UInt64 expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test UInt64 expression")]
        public void TestUInt64Expression()
        {
            KeyRange<ulong> keyRange = KeyValueExpressionEvaluator<ulong, string>.GetKeyRange(x => x.Key < 2);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(2UL, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test float expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test float expression")]
        public void TestFloatExpression()
        {
            KeyRange<float> keyRange = KeyValueExpressionEvaluator<float, string>.GetKeyRange(x => x.Key < 2.0f);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(2.0f, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test double expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test double expression")]
        public void TestDoubleExpression()
        {
            KeyRange<double> keyRange = KeyValueExpressionEvaluator<double, string>.GetKeyRange(x => x.Key < 2.0);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(2.0, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test TimeSpan expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test TimeSpan expression")]
        public void TestTimeSpanExpression()
        {
            KeyRange<TimeSpan> keyRange = KeyValueExpressionEvaluator<TimeSpan, string>.GetKeyRange(x => x.Key < TimeSpan.FromSeconds(2));
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(TimeSpan.FromSeconds(2), keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test DateTime expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DateTime expression")]
        public void TestDateTimeExpression()
        {
            DateTime dateTime = DateTime.UtcNow;
            KeyRange<DateTime> keyRange = KeyValueExpressionEvaluator<DateTime, string>.GetKeyRange(x => x.Key < dateTime);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(dateTime, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test Guid expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Guid expression")]
        public void TestGuidExpression()
        {
            Guid guid = Guid.NewGuid();
            KeyRange<Guid> keyRange = KeyValueExpressionEvaluator<Guid, string>.GetKeyRange(x => x.Key == guid);
            Assert.AreEqual(guid, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
            Assert.AreEqual(guid, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test nullable bool.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable bool expression")]
        public void TestNullableBoolExpression()
        {
            Func<bool?> f = () => false;
            var expected = new KeyRange<bool>(Key<bool>.CreateKey(false, true), Key<bool>.CreateKey(false, true));
            var actual = KeyValueExpressionEvaluator<bool, string>.GetKeyRange(x => x.Key == f());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable byte.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable byte expression")]
        public void TestNullableByteExpression()
        {
            byte? min = 1;
            Func<byte?> fmax = () => 8;
            var expected = new KeyRange<byte>(Key<byte>.CreateKey(1, true), Key<byte>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<byte, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual, "byte to int? promotion not supported");
        }

        /// <summary>
        /// Test nullable short.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable short expression")]
        public void TestNullableShortExpression()
        {
            short? min = 1;
            Func<short?> fmax = () => 8;
            var expected = new KeyRange<short>(Key<short>.CreateKey(1, true), Key<short>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<short, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual, "short to int? promotion not supported");
        }

        /// <summary>
        /// Test nullable ushort.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable ushort expression")]
        public void TestNullableUShortExpression()
        {
            ushort? min = 1;
            Func<ushort?> fmax = () => 8;
            var expected = new KeyRange<ushort>(Key<ushort>.CreateKey(1, true), Key<ushort>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<ushort, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual, "ushort to int? promotion not supported");
        }

        /// <summary>
        /// Test an expression that compares an ushort key with a ulong? value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test an expression that compares an ushort key with a ulong? value")]
        public void TestUShortNullableULongExpression()
        {
            ulong? min = 1;
            Func<ulong?> fmax = () => 8;
            var expected = new KeyRange<ushort>(Key<ushort>.CreateKey(1, true), Key<ushort>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<ushort, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable int.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable int expression")]
        public void TestNullableIntExpression()
        {
            int? min = 1;
            Func<int?> fmax = () => 8;
            var expected = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable int with a null value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable int expression with a null value")]
        public void TestNullIntExpression()
        {
            int? min = 1;
            Func<int?> fmax = () => null;
            var expected = new KeyRange<int>(Key<int>.CreateKey(1, true), null);
            var actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test an expression that compares an int key with a long? value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test an expression that compares an int key with a long? value")]
        public void TestIntNullableLongExpression()
        {
            long? min = 1;
            Func<long?> fmax = () => 8;
            var expected = new KeyRange<int>(Key<int>.CreateKey(1, true), Key<int>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test an expression that compares an int key with a long? value that is too big.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test an expression that compares an int key with a long? value that is too big")]
        [ExpectedException(typeof(OverflowException))]
        public void TestIntNullableLongExpressionOverflow()
        {
            long? min = 1;
            Func<long?> fmax = () => Int64.MaxValue - 3;
            var actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
        }

        /// <summary>
        /// Test an expression that compares an int key with a long? value that is too small.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test an expression that compares an int key with a long? value that is too small")]
        [ExpectedException(typeof(OverflowException))]
        public void TestIntNullableLongExpressionUnderflow()
        {
            long? min = Int64.MinValue + 2;
            Func<long?> fmax = () => 1;
            var actual = KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
        }

        /// <summary>
        /// Test nullable uint.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable uint expression")]
        public void TestNullableUIntExpression()
        {
            uint? min = 1;
            Func<uint?> fmax = () => 8;
            var expected = new KeyRange<uint>(Key<uint>.CreateKey(1, true), Key<uint>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<uint, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable long expression")]
        public void TestNullableLongExpression()
        {
            long? min = 1;
            Func<long?> fmax = () => 8;
            var expected = new KeyRange<long>(Key<long>.CreateKey(1, true), Key<long>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<long, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable ulong.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable ulong expression")]
        public void TestNullableULongExpression()
        {
            ulong? min = 1;
            Func<ulong?> fmax = () => 8;
            var expected = new KeyRange<ulong>(Key<ulong>.CreateKey(1, true), Key<ulong>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<ulong, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable float.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable float expression")]
        public void TestNullableFloatExpression()
        {
            float? min = 1;
            Func<float?> fmax = () => 8;
            var expected = new KeyRange<float>(Key<float>.CreateKey(1, true), Key<float>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<float, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable double.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable double expression")]
        public void TestNullableDoubleExpression()
        {
            double? min = 1;
            Func<double?> fmax = () => 8;
            var expected = new KeyRange<double>(Key<double>.CreateKey(1, true), Key<double>.CreateKey(8, false));
            var actual = KeyValueExpressionEvaluator<double, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable DateTime.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable DateTime expression")]
        public void TestNullableDateTimeExpression()
        {
            DateTime? min = DateTime.MinValue;
            Func<DateTime?> fmax = () => DateTime.MaxValue;
            var expected = new KeyRange<DateTime>(Key<DateTime>.CreateKey(DateTime.MinValue, true), Key<DateTime>.CreateKey(DateTime.MaxValue, false));
            var actual = KeyValueExpressionEvaluator<DateTime, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable DateTime with a null value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable DateTime expression with a null value")]
        public void TestNullDateTimeExpression()
        {
            DateTime? min = null;
            Func<DateTime?> fmax = () => DateTime.MaxValue;
            var expected = new KeyRange<DateTime>(null, Key<DateTime>.CreateKey(DateTime.MaxValue, false));
            var actual = KeyValueExpressionEvaluator<DateTime, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable TimeSpan.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable TimeSpan expression")]
        public void TestNullableTimeSpanExpression()
        {
            TimeSpan? min = TimeSpan.MinValue;
            Func<TimeSpan?> fmax = () => TimeSpan.MaxValue;
            var expected = new KeyRange<TimeSpan>(Key<TimeSpan>.CreateKey(TimeSpan.MinValue, true), Key<TimeSpan>.CreateKey(TimeSpan.MaxValue, false));
            var actual = KeyValueExpressionEvaluator<TimeSpan, string>.GetKeyRange(x => min <= x.Key && x.Key < fmax());
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test nullable Guid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test nullable Guid expression")]
        public void TestNullableGuidExpression()
        {
            Func<Guid?> f = () => Guid.Empty;
            var expected = new KeyRange<Guid>(Key<Guid>.CreateKey(Guid.Empty, true), Key<Guid>.CreateKey(Guid.Empty, true));
            var actual = KeyValueExpressionEvaluator<Guid, string>.GetKeyRange(x => x.Key == f());
            Assert.AreEqual(expected, actual);
        }

        #endregion

        #region String-specific tests

        /// <summary>
        /// Test String equals expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String == expression")]
        public void TestStringEquality()
        {
            string s = "foo";
            KeyRange<string> keyRange = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => s == x.Key);
            Assert.AreEqual(s, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
            Assert.AreEqual(s, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test String null is ignored
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String null is ignored")]
        public void TestStringNullIsIgnored()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => x.Key.CompareTo(null) > 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Equals expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Equals")]
        public void TestStringEquals()
        {
            string s = "baz";
            KeyRange<string> keyRange = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => x.Key.Equals(s));
            Assert.AreEqual(s, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
            Assert.AreEqual(s, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test a NOT of a String.Equals expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test !String.Equals")]
        public void TestNotStringEquals()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => !x.Key.Equals("e"));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Equals expression with the parameter as the argument
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Equals with the parameter as the argument")]
        public void TestStringEqualsParametersAsArgument()
        {
            // This isn't currently recognized
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => "?".Equals(x.Key));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Equals expression with the parameter as the argument
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Equals with no parameter access")]
        public void TestStringEqualsWithoutParameterAccess()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => "?".Equals("?"));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Contains
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Contains")]
        public void TestStringMatch()
        {
            // This can't be optimized
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => x.Key.Contains("*"));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.StartsWith expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.StartWith")]
        public void TestStringStartsWith()
        {
            string s = "baz";
            KeyRange<string> actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => x.Key.StartsWith(s));
            var expected = new KeyRange<string>(Key<string>.CreateKey("baz", true), Key<string>.CreatePrefixKey("baz"));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.StartsWith expression reversed
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.StartWith reversed")]
        public void TestStringStartsWithReversed()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => "foo".StartsWith(x.Key));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.StartsWith expression without parameter access
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.StartWith without parameter access")]
        public void TestStringStartsWithoutParameterAccess()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => "foo".StartsWith("bar"));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.StartsWith with value access
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.StartWith value access")]
        public void TestStringStartsValueAccess()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => x.Value.StartsWith("foo"));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.StartsWith intersection
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.StartWith intersection")]
        public void TestStringStartsWithIntersection()
        {
            string s = "baz";
            KeyRange<string> actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => x.Key.StartsWith(s) && x.Key.CompareTo("z") < 0);
            var expected = new KeyRange<string>(Key<string>.CreateKey(s, true), Key<string>.CreatePrefixKey(s));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.StartsWith union
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.StartWith union")]
        public void TestStringStartsWithUnion()
        {
            KeyRange<string> actual =
                KeyValueExpressionEvaluator<string, string>.GetKeyRange(
                    x => x.Key.StartsWith("b") || (x.Key.CompareTo("a") > 0 && x.Key.CompareTo("b") <= 0));
            var expected = new KeyRange<string>(Key<string>.CreateKey("a", false), Key<string>.CreatePrefixKey("b"));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.CompareTo
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.CompareTo")]
        public void TestStringCompareTo()
        {
            KeyRange<string> keyRange = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => x.Key.CompareTo("foo") < 0);
            Assert.IsNull(keyRange.Min);
            Assert.AreEqual("foo", keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test String.CompareTo reversed
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.CompareTo reversed")]
        public void TestStringCompareToReversed()
        {
            KeyRange<string> keyRange = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => 0 <= x.Key.CompareTo("bar"));
            Assert.AreEqual("bar", keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Test String.CompareTo argument folding
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.CompareTo argument folding")]
        public void TestStringCompareToArgumentFolding()
        {
            KeyRange<string> keyRange = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => 0 <= x.Key.CompareTo("foo" + "bar"));
            Assert.AreEqual("foobar", keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Test String.Compare
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare")]
        public void TestStringCompare()
        {
            KeyRange<string> keyRange =
                KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Compare(x.Key, "foo") > 0);
            Assert.AreEqual("foo", keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Test String.Compare 2
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare 2")]
        public void TestStringCompare2()
        {
            KeyRange<string> keyRange =
                KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Compare("foo", x.Key) <= 0);
            Assert.AreEqual("foo", keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Test String.Compare 3
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare 3")]
        public void TestStringCompare3()
        {
            KeyRange<string> keyRange =
                KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => 0 == String.Compare(x.Key, "b" + "ar"));
            Assert.AreEqual("bar", keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual("bar", keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test String.Compare 4
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare 4")]
        public void TestStringCompare4()
        {
            KeyRange<string> keyRange =
                KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => 0 > String.Compare(5.ToString(), x.Key));
            Assert.AreEqual("5", keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Test String.Compare without parameter access
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare without parameter access")]
        public void TestStringCompareWithoutParameterAccess()
        {
            KeyRange<string> keyRange =
                KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => 0 > String.Compare("a", "b"));
            Assert.AreEqual(keyRange, KeyRange<string>.OpenRange);
        }

        /// <summary>
        /// Test String.Compare with non zero comparand 1
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare with non-zero comparand 1")]
        public void TestStringCompareNonZeroComparand1()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Compare(x.Key, "foo") > 1);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Compare with non zero comparand 2
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare with non-zero comparand 2")]
        public void TestStringCompareNonZeroComparand2()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Compare("foo", x.Key) < 1);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Compare with non zero comparand 3
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare with non-zero comparand 3")]
        public void TestStringCompareNonZeroComparand3()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => 1 >= String.Compare(x.Key, "foo"));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Compare with non zero comparand 4
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare with non-zero comparand 4")]
        public void TestStringCompareNonZeroComparand4()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => 2 == String.Compare("foo", x.Key));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Compare with non-constant comparand 1
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare with non-zero comparand 1")]
        public void TestStringCompareNonConstantComparand1()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, int>.GetKeyRange(x => String.Compare(x.Key, "foo") > x.Value);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Compare with non-constant comparand 2
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare with non-zero comparand 2")]
        public void TestStringCompareNonConstantComparand2()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, int>.GetKeyRange(x => String.Compare("foo", x.Key) < x.Value);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Compare with non-constant comparand 3
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare with non-zero comparand 3")]
        public void TestStringCompareNonConstantComparand3()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, int>.GetKeyRange(x => x.Value >= String.Compare(x.Key, "foo"));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Compare with non-constant comparand 4
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare with non-zero comparand 4")]
        public void TestStringCompareNonConstantComparand4()
        {
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, int>.GetKeyRange(x => x.Value == String.Compare("foo", x.Key));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test String.Compare case-insensitive
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Compare case-insensitive")]
        public void TestStringCompareCaseInsensitive()
        {
            // Not handled (the index is case sensitive)
            var expected = KeyRange<string>.OpenRange;
            var actual = KeyValueExpressionEvaluator<string, int>.GetKeyRange(x => x.Value == String.Compare(x.Key, "foo", true));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test the static String.Equals expression
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Equals")]
        public void TestStringStaticEquals()
        {
            KeyRange<string> actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Equals(x.Key, "baz"));
            var expected = new KeyRange<string>(Key<string>.CreateKey("baz", true), Key<string>.CreateKey("baz", true));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test the static String.Equals expression reversed
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Equals reversed")]
        public void TestStringStaticEqualsReversed()
        {
            KeyRange<string> actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Equals("baz", x.Key));
            var expected = new KeyRange<string>(Key<string>.CreateKey("baz", true), Key<string>.CreateKey("baz", true));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test the static String.Equals expression without key access
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Equals without key access")]
        public void TestStringStaticEqualsNoKeyAcess()
        {
            KeyRange<string> actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Equals("baz", x.Value));
            Assert.AreEqual(KeyRange<string>.OpenRange, actual);
        }

        /// <summary>
        /// Test the static String.Equals expression without parameter access
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Equals without parameter access")]
        public void TestStringStaticEqualsNoParameterAccess()
        {
            KeyRange<string> actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Equals("baz", "foo"));
            Assert.AreEqual(KeyRange<string>.OpenRange, actual);
        }

        /// <summary>
        /// Test the static String.Equals expression with non constant
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Equals with non constant")]
        public void TestStringStaticEqualsNonConstant()
        {
            KeyRange<string> actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Equals(x.Key, x.Value));
            Assert.AreEqual(KeyRange<string>.OpenRange, actual);
        }

        /// <summary>
        /// Test the static String.Equals expression with non constant (reversed)
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Equals with non constant (reversed)")]
        public void TestStringStaticEqualsNonConstantReversed()
        {
            KeyRange<string> actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Equals(x.Value, x.Key));
            Assert.AreEqual(KeyRange<string>.OpenRange, actual);
        }

        /// <summary>
        /// Test the static String.Equals expression with case flag
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test String.Equals with non constant with case flag")]
        public void TestStringStaticEqualsWithCaseFlag()
        {
            KeyRange<string> actual = KeyValueExpressionEvaluator<string, string>.GetKeyRange(x => String.Equals("X", x.Key, StringComparison.OrdinalIgnoreCase));
            Assert.AreEqual(KeyRange<string>.OpenRange, actual);
        }

        #endregion

        #region Equals tests

        /// <summary>
        /// Test Equals
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Equals")]
        public void TestEquals()
        {
            var expected = new KeyRange<long>(Key<long>.CreateKey(10, true), Key<long>.CreateKey(10, true));
            var actual = KeyValueExpressionEvaluator<long, string>.GetKeyRange(x => x.Key.Equals(10));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test Equals
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test !Equals")]
        public void TestNotEquals()
        {
            var expected = KeyRange<long>.OpenRange;
            var actual = KeyValueExpressionEvaluator<long, string>.GetKeyRange(x => !x.Key.Equals(10));
            Assert.AreEqual(expected, actual);
        }

        #endregion

        #region CompareTo tests

        /// <summary>
        /// Test CompareTo
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test CompareTo")]
        public void TestCompareTo()
        {
            KeyRange<long> keyRange =
                KeyValueExpressionEvaluator<long, string>.GetKeyRange(
                    x => x.Key.CompareTo(7) < 0 && x.Key.CompareTo(-8) >= 0);
            Assert.AreEqual(-8, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(7, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test CompareTo with non-zero comparand
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test CompareTo with non-zero comparand")]
        public void TestCompareToNonZeroComparand()
        {
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key.CompareTo(7) < 1);
            Assert.IsNull(keyRange.Min);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Test CompareTo reversed with non-zero comparand
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test CompareTo reversed with non-zero comparand")]
        public void TestCompareToNonZeroComparandReversed()
        {
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => 2 > x.Key.CompareTo(8));
            Assert.AreEqual(KeyRange<int>.OpenRange, keyRange);
        }

        /// <summary>
        /// Test CompareTo with the wrong type
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test CompareTo the wrong type")]
        public void TestCompareToWrongType()
        {
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key.CompareTo(Guid.NewGuid()) < 0);
            Assert.IsNull(keyRange.Min);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Test CompareTo without parameter access
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test CompareTo with no parameter access")]
        public void TestCompareToNoParameter()
        {
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => 0 < 5.CompareTo(4));
            Assert.AreEqual(keyRange, KeyRange<int>.OpenRange);
        }

        /// <summary>
        /// Test CompareTo the parameter
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test CompareTo the parameter")]
        public void TestCompareToParameter()
        {
            var expected = KeyRange<int>.OpenRange;
            var actual = KeyValueExpressionEvaluator<int, int>.GetKeyRange(x => 0 < x.Key.CompareTo(x.Value));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test CompareTo with a non-constant comparand 1
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test CompareTo non-constant comparand 1")]
        public void TestCompareToNonConstant1()
        {
            var expected = KeyRange<int>.OpenRange;
            var actual = KeyValueExpressionEvaluator<int, int>.GetKeyRange(x => x.Value < x.Key.CompareTo(5));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test CompareTo with a non-constant comparand 2
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test CompareTo non-constant comparand 2")]
        public void TestCompareToNonConstant2()
        {
            var expected = KeyRange<int>.OpenRange;
            var actual = KeyValueExpressionEvaluator<int, int>.GetKeyRange(x => x.Key.CompareTo(5) == x.Value);
            Assert.AreEqual(expected, actual);
        }

        #endregion

        #region Functional and Regression Tests

        /// <summary>
        /// Test expression 1.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 1")]
        public void TestSample1()
        {
            KeyRange<double> keyRange = KeyValueExpressionEvaluator<double, string>.GetKeyRange(
                x => 18 <= x.Key && x.Key < 99 && x.Key > 7 && x.Key <= 99 && (0 == x.Key % 2));
            Assert.AreEqual(18.0, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(99.0, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test expression 2.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 2")]
        public void TestSample2()
        {
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, string>.GetKeyRange(x => x.Key < 1 && x.Key < 2 && x.Key < 3);

            Assert.IsNull(keyRange.Min);
            Assert.AreEqual(1, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test expression 3.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 3")]
        public void TestSample3()
        {
            KeyRange<uint> keyRange = KeyValueExpressionEvaluator<uint, string>.GetKeyRange(
                x => ConstMember <= x.Key && x.Key < 99 && x.Key > 7 && x.Key <= 99 && x.Value.Length == 2);
            Assert.AreEqual(18U, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(99U, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test expression 4.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 4")]
        public void TestSample4()
        {
            staticMember = 18;
            KeyRange<ulong> keyRange = KeyValueExpressionEvaluator<ulong, string>.GetKeyRange(
                x => staticMember <= x.Key && x.Key < 99 && x.Key > 7 && x.Key <= 99 && x.Value == "bar");
            Assert.AreEqual(18UL, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(99UL, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test expression 5.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 5")]
        public void TestSample5()
        {
            DateTime? date = DateTime.UtcNow;
            TimeSpan? timespan = TimeSpan.FromDays(90);

            KeyRange<DateTime> actual = KeyValueExpressionEvaluator<DateTime, string>.GetKeyRange(d => d.Key >= date && d.Key <= date + timespan);
            KeyRange<DateTime> expected = new KeyRange<DateTime>(Key<DateTime>.CreateKey(date.Value, true), Key<DateTime>.CreateKey(date.Value + timespan.Value, true));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test expression 6.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 6")]
        public void TestSample6()
        {
            KeyRange<double> keyRange = KeyValueExpressionEvaluator<double, string>.GetKeyRange(d => d.Key >= 5.0 && !(d.Key <= 10.0));
            Assert.AreEqual(10.0, keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.IsNull(keyRange.Max);
        }

        /// <summary>
        /// Test expression 7.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 7")]
        public void TestSample7()
        {
            KeyRange<float> keyRange = KeyValueExpressionEvaluator<float, string>.GetKeyRange(d => d.Key >= 5.0F && d.Key <= 10.0F);
            Assert.AreEqual(5.0F, keyRange.Min.Value);
            Assert.IsTrue(keyRange.Min.IsInclusive);
            Assert.AreEqual(10.0F, keyRange.Max.Value);
            Assert.IsTrue(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test expression 8.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 8")]
        public void TestSample8()
        {
            KeyRange<int> keyRange =
                KeyValueExpressionEvaluator<int, int>.GetKeyRange(
                    i => i.Key < 100 && i.Key > -50 && i.Key < i.Value && i.Key < Math.Min(50, 100));
            Assert.AreEqual(-50, keyRange.Min.Value);
            Assert.IsFalse(keyRange.Min.IsInclusive);
            Assert.AreEqual(50, keyRange.Max.Value);
            Assert.IsFalse(keyRange.Max.IsInclusive);
        }

        /// <summary>
        /// Test expression 9.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 9")]
        public void TestSample9()
        {
            // Regression test for:
            //   KeyRange is too small.
            //   Expression:
            //     x => Not(((2137 >= x.Key) && Not((-3611 = x.Key))))
            KeyRange<int> actual =
                KeyValueExpressionEvaluator<int, int>.GetKeyRange(
                    x => !((2137 >= x.Key) && !(-3611 == x.Key)));
            KeyRange<int> expected = new KeyRange<int>(Key<int>.CreateKey(-3611, true), null);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test expression 10.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 10")]
        public void TestSample10()
        {
            // Regression test for:
            //   Assert.IsTrue failed.
            //   Error at entry 21. Not enough entries in actual.
            //   First missing entry is [ibh, ibh]
            //    expression = x => (x.Key.StartsWith("i") || (x.Key.Equals("ibg") || ("f" = x.Key)))
            KeyRange<string> actual =
                KeyValueExpressionEvaluator<string, string>.GetKeyRange(
                    x => (x.Key.StartsWith("i") || (x.Key.Equals("ibg") || ("f" == x.Key))));
            KeyRange<string> expected = new KeyRange<string>(Key<string>.CreateKey("f", true), Key<string>.CreatePrefixKey("i"));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test expression 11.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 11")]
        public void TestSample11()
        {
            // Regression test for:
            //   Assert.IsTrue failed.
            //   Error at entry 0. Not enough entries in actual.
            //   First missing entry is [ggi, ggi] 
            //    expression = x => ((Compare("ggi", x.Key) <= 0) && (x.Key.Equals("fag") || x.Key.StartsWith("g")))
            KeyRange<string> actual =
                KeyValueExpressionEvaluator<string, string>.GetKeyRange(
                    x => ((String.Compare("ggi", x.Key) <= 0) && (x.Key.Equals("fag") || x.Key.StartsWith("g"))));
            KeyRange<string> expected = new KeyRange<string>(
                Key<string>.CreateKey("ggi", true),
                Key<string>.CreatePrefixKey("g"));
            Assert.AreEqual(expected, actual);
            Assert.IsFalse(actual.IsEmpty);
        }

        /// <summary>
        /// Test expression 12.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 12")]
        public void TestSample12()
        {
            Expression<Predicate<int>> expression = x => x >= -1 && x < 101;
            KeyRange<int> actual = PredicateExpressionEvaluator<int>.GetKeyRange(expression.Body, null);
            KeyRange<int> expected = new KeyRange<int>(Key<int>.CreateKey(-1, true), Key<int>.CreateKey(101, false));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test expression 13.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test expression 13")]
        public void TestSample13()
        {
            string s = "foo";
            Expression<Predicate<int>> expression = x => x > -1 && s.Length < 101;
            KeyRange<int> actual = PredicateExpressionEvaluator<int>.GetKeyRange(expression.Body, null);
            KeyRange<int> expected = new KeyRange<int>(Key<int>.CreateKey(-1, false), null);
            Assert.AreEqual(expected, actual);
        }

        #endregion

        /// <summary>
        /// Common test for constant folding tests.
        /// </summary>
        /// <param name="expression">
        /// An expression which should come out to x.Key LE 32
        /// </param>
        private static void ConstantFoldingHelper(Expression<Predicate<KeyValuePair<int, long>>> expression)
        {
            KeyRange<int> actual = KeyValueExpressionEvaluator<int, long>.GetKeyRange(expression);
            KeyRange<int> expected = new KeyRange<int>(null, Key<int>.CreateKey(32, true));
            Assert.AreEqual(expected, actual);
        }
    }
}
