// --------------------------------------------------------------------------------------------------------------------
// <copyright file="RandomInt32RangeExpressionTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Reflection;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Generate expressions that can be optimized.
    /// </summary>
    [TestClass]
    public class RandomInt32RangeExpressionTests
    {
        /// <summary>
        /// The smallest value we can generate.
        /// </summary>
        private const int MinValue = -8192;

        /// <summary>
        /// The largest value we can generate.
        /// </summary>
        private const int MaxValue = 8192;

        /// <summary>
        /// The parameter expression used to build out expression trees. This 
        /// should be the same object in all places so we need a singleton.
        /// </summary>
        private static readonly ParameterExpression parameterExpression = Expression.Parameter(typeof(KeyValuePair<int, int>), "x");

        /// <summary>
        /// MemberInfo that describes the Key member of the KeyValuePair.
        /// </summary>
        private static readonly MemberInfo keyMemberInfo = typeof(KeyValuePair<int, int>).GetProperty("Key", typeof(int));

        /// <summary>
        /// Random number generator.
        /// </summary>
        private readonly Random rand = new Random();

        /// <summary>
        /// Test the Key expression evaluator with randomly generated ranges.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test the PredicateExpressionEvaluator with random ranges")]
        public void TestRandomInt32KeyRangeExpressions()
        {
            DateTime endTime = DateTime.UtcNow + TimeSpan.FromSeconds(19.5);
            int trials = 0;
            int emptyRanges = 0;
            int exactMatches = 0;
            while (DateTime.UtcNow < endTime)
            {
                bool rangeWasEmpty;
                bool rangeWasExact;
                this.DoOneTest(out rangeWasEmpty, out rangeWasExact);
                ++trials;
                if (rangeWasEmpty)
                {
                    ++emptyRanges;
                }

                if (rangeWasExact)
                {
                    ++exactMatches;
                }
            }

            Console.WriteLine("{0:N0} trials. {1:N0} empty ranges. {2:N0} exact matches", trials, emptyRanges, exactMatches);
        }

        /// <summary>
        /// Determine if the given range matches the given min/max values exactly.
        /// </summary>
        /// <param name="keyRange">The key range.</param>
        /// <param name="min">The minimum value.</param>
        /// <param name="max">The maximum value.</param>
        /// <returns>True if the KeyRange is an exact match.</returns>
        private static bool KeyRangeIsExact(KeyRange<int> keyRange, int min, int max)
        {
            bool minIsCorrect =
                (null != keyRange.Min)
                && ((keyRange.Min.Value == min && keyRange.Min.IsInclusive)
                    || (keyRange.Min.Value == min - 1 && !keyRange.Min.IsInclusive));

            bool maxIsCorrect =
                (null != keyRange.Max)
                && ((keyRange.Max.Value == max && keyRange.Max.IsInclusive)
                    || (keyRange.Max.Value == max + 1 && !keyRange.Max.IsInclusive));

            if (!minIsCorrect && MinValue == min && null == keyRange.Min)
            {
                // No min value is correct if we start at the minimum value
                minIsCorrect = true;
            }

            if (!maxIsCorrect && MaxValue - 1 == max && null == keyRange.Max)
            {
                // No max value is correct if we end at the maximum value
                maxIsCorrect = true;
            }

            return minIsCorrect && maxIsCorrect;
        }

        /// <summary>
        /// Determine if the given range matches at least the records described
        /// by the min/max values.
        /// </summary>
        /// <param name="keyRange">The key range.</param>
        /// <param name="min">The minimum value.</param>
        /// <param name="max">The maximum value.</param>
        /// <returns>True if the KeyRange is a superset of the values.</returns>
        private static bool KeyRangeIsSufficient(KeyRange<int> keyRange, int min, int max)
        {
            bool minIsSufficient =
                (null == keyRange.Min)
                || (keyRange.Min.Value == min && keyRange.Min.IsInclusive)
                || keyRange.Min.Value < min;

            bool maxIsSufficient =
                (null == keyRange.Max)
                || (keyRange.Max.Value == max && keyRange.Max.IsInclusive)
                || keyRange.Max.Value > max;

            return minIsSufficient && maxIsSufficient;
        }

        /// <summary>
        /// Create a new parameter expression.
        /// </summary>
        /// <returns>
        /// A constant expression with a value between
        /// <see cref="MinValue"/> (inclusive) and <see cref="MaxValue"/>
        /// (exclusive).
        /// </returns>
        private static ParameterExpression CreateParameterExpression()
        {
            return parameterExpression;
        }

        /// <summary>
        /// Run a test with one randomly generated expression tree.
        /// </summary>
        /// <param name="rangeWasEmpty">True if an empty range was generated.</param>
        /// <param name="rangeWasExact">
        /// True if the generated range matched the given maximums and minimums exactly.
        /// </param>
        private void DoOneTest(out bool rangeWasEmpty, out bool rangeWasExact)
        {
            // Unfortunately this generates a Func<KeyValuePair<int, int>, bool> instead
            // of a Predicate<KeyValuePair<int, int>. We work around that by calling
            // PredicateExpressionEvaluator directly.
            Expression<Func<KeyValuePair<int, int>, bool>> expression = this.CreateExpression();
            Func<KeyValuePair<int, int>, bool> func = expression.Compile();

            // This is the test Oracle: we create the KeyValuePairs and see which ones
            // are matched by the expression.
            int min = MinValue;
            int max = MaxValue;
            int count = 0;
            for (int i = MinValue; i < MaxValue; ++i)
            {
                KeyValuePair<int, int> kvp = new KeyValuePair<int, int>(i, i);
                if (func(kvp))
                {
                    if (count++ == 0)
                    {
                        min = kvp.Key;
                    }

                    max = kvp.Key;
                }
            }

            KeyRange<int> keyRange = PredicateExpressionEvaluator<int>.GetKeyRange(expression.Body, keyMemberInfo);
            if (count > 0)
            {
                Assert.IsTrue(
                    KeyRangeIsSufficient(keyRange, min, max),
                    "KeyRange is too small. Expression: {0}, Min = {1}, Max = {2}, Got {3}",
                    expression,
                    min,
                    max,
                    keyRange);

                rangeWasExact = KeyRangeIsExact(keyRange, min, max);
                rangeWasEmpty = false;
            }
            else
            {
                rangeWasExact = keyRange.IsEmpty;
                rangeWasEmpty = true;
            }
        }

        /// <summary>
        /// Create a random expression tree.
        /// </summary>
        /// <returns>A random expression tree.</returns>
        private Expression<Func<KeyValuePair<int, int>, bool>> CreateExpression()
        {
            return (Expression<Func<KeyValuePair<int, int>, bool>>)Expression.Lambda(this.CreateBooleanExpression(), CreateParameterExpression());
        }

        /// <summary>
        /// Create a boolean expression. This is the top-level expression.
        /// It is either AND, OR, key comparison or a negation of the same.
        /// </summary>
        /// <returns>A boolean expression.</returns>
        private Expression CreateBooleanExpression()
        {
            switch (this.rand.Next(16))
            {
                case 0:
                case 1:
                    return Expression.AndAlso(this.CreateBooleanExpression(), this.CreateBooleanExpression());
                case 2:
                case 3:
                    return Expression.OrElse(this.CreateBooleanExpression(), this.CreateBooleanExpression());
                case 4:
                    return Expression.Not(Expression.AndAlso(this.CreateBooleanExpression(), this.CreateBooleanExpression()));
                case 5:
                    return Expression.Not(Expression.OrElse(this.CreateBooleanExpression(), this.CreateBooleanExpression()));
                case 6:
                    return Expression.Not(this.CreateKeyComparisonExpression());
                default:
                    return this.CreateKeyComparisonExpression();
            }
        }

        /// <summary>
        /// Create a key comparison expression.
        /// </summary>
        /// <returns>A key comparison expression.</returns>
        private BinaryExpression CreateKeyComparisonExpression()
        {
            Expression parameter = CreateParameterExpression();
            Expression constant = this.CreateConstantExpression();
            Expression key = Expression.MakeMemberAccess(parameter, keyMemberInfo);

            Expression left;
            Expression right;
            if (0 == this.rand.Next(2))
            {
                left = key;
                right = constant;
            }
            else
            {
                left = constant;
                right = key;
            }

            switch (this.rand.Next(6))
            {
                case 0:
                    return Expression.LessThan(left, right);
                case 1:
                    return Expression.LessThanOrEqual(left, right);
                case 2:
                    return Expression.Equal(left, right);
                case 3:
                    return Expression.GreaterThan(left, right);
                case 4:
                    return Expression.NotEqual(left, right);
                default:
                    return Expression.GreaterThanOrEqual(left, right);
            }
        }

        /// <summary>
        /// Create a new (random) constant expression.
        /// </summary>
        /// <returns>
        /// A constant expression with a value between
        /// <see cref="MinValue"/> (inclusive) and <see cref="MaxValue"/>
        /// (exclusive).
        /// </returns>
        private ConstantExpression CreateConstantExpression()
        {
            return Expression.Constant(this.rand.Next(MinValue, MaxValue));
        }
    }
}