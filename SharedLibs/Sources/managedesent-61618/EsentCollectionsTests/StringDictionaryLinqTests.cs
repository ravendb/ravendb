// --------------------------------------------------------------------------------------------------------------------
// <copyright file="StringDictionaryLinqTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Compare a PersistentDictionary against a generic dictionary.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Generate string expressions that can be optimized.
    /// </summary>
    [TestClass]
    public class StringDictionaryLinqTests
    {
        /// <summary>
        /// The location of the dictionary.
        /// </summary>
        private const string DictionaryLocation = "RandomStringDictionary";

        /// <summary>
        /// A MethodInfo describing String.Compare(string, string).
        /// </summary>
        private static readonly MethodInfo stringCompareMethod = typeof(string).GetMethod("Compare", new[] { typeof(string), typeof(string) });

        /// <summary>
        /// A MethodInfo describing String.Equals(string).
        /// </summary>
        private static readonly MethodInfo stringEqualsMethod = typeof(string).GetMethod("Equals", new[] { typeof(string) });

        /// <summary>
        /// A MethodInfo describing String.StartsWith(string).
        /// </summary>
        private static readonly MethodInfo stringStartsWithMethod = typeof(string).GetMethod("StartsWith", new[] { typeof(string) }); 

        /// <summary>
        /// The parameter expression used to build out expression trees. This 
        /// should be the same object in all places so we need a singleton.
        /// </summary>
        private static readonly ParameterExpression parameterExpression = Expression.Parameter(typeof(KeyValuePair<string, string>), "x");

        /// <summary>
        /// MemberInfo that describes the Key member of the KeyValuePair.
        /// </summary>
        private static readonly MemberInfo keyMemberInfo = typeof(KeyValuePair<string, string>).GetProperty("Key", typeof(string));

        /// <summary>
        /// Random number generator.
        /// </summary>
        private readonly Random rand = new Random();

        /// <summary>
        /// Data for tests.
        /// </summary>
        private static KeyValuePair<string, string>[] data;

        /// <summary>
        /// The dictionary we are testing.
        /// </summary>
        private PersistentDictionary<string, string> dictionary;

        /// <summary>
        /// Test initialization.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);
            CreateTestData();
            this.dictionary = new PersistentDictionary<string, string>(data, DictionaryLocation);
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
        /// Test the Key expression evaluator with randomly generated ranges.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test the string dictionary with random expressions")]
        public void TestRandomStringKeyRangeExpressions()
        {
            const double TimeLimit = 19;
            this.RunTest(TimeLimit);
        }

        /// <summary>
        /// Creates the test data.
        /// </summary>
        private static void CreateTestData()
        {
            const int MinChar = 97;
            const int MaxChar = 107; // 123;

            List<KeyValuePair<string, string>> tempData = new List<KeyValuePair<string, string>>();
            for (int i = MinChar; i < MaxChar; ++i)
            {
                var sb1 = new StringBuilder(1);
                sb1.Append((char)i);
                tempData.Add(new KeyValuePair<string, string>(sb1.ToString(), sb1.ToString()));
                for (int j = MinChar; j < MaxChar; ++j)
                {
                    var sb2 = new StringBuilder(2);
                    sb2.Append((char)i);
                    sb2.Append((char)j);
                    tempData.Add(new KeyValuePair<string, string>(sb2.ToString(), sb2.ToString()));
                    for (int k = MinChar; k < MaxChar; ++k)
                    {
                        var sb3 = new StringBuilder(3);
                        sb3.Append((char)i);
                        sb3.Append((char)j);
                        sb3.Append((char)k);
                        tempData.Add(new KeyValuePair<string, string>(sb3.ToString(), sb3.ToString()));
                    }
                }
            }

            data = tempData.ToArray();
        }

        /// <summary>
        /// Create a new parameter expression.
        /// </summary>
        /// <returns>
        /// A constant expression with a value taken from the data array.
        /// </returns>
        private static ParameterExpression CreateParameterExpression()
        {
            return parameterExpression;
        }

        /// <summary>
        /// Create an expression that accesses the key of the parameter.
        /// </summary>
        /// <returns>
        /// An expression that accesses the key of the parameter.
        /// </returns>
        private static Expression CreateKeyAccess()
        {
            Expression parameter = CreateParameterExpression();
            return Expression.MakeMemberAccess(parameter, keyMemberInfo);
        }

        /// <summary>
        /// Run a test for a specified period of time.
        /// </summary>
        /// <param name="timeLimit">The time to run for.</param>
        private void RunTest(double timeLimit)
        {
            DateTime endTime = DateTime.UtcNow + TimeSpan.FromSeconds(timeLimit);
            int trials = 0;
            while (DateTime.UtcNow < endTime)
            {
                this.DoOneTest();
                ++trials;
            }

            Console.WriteLine("{0:N0} trials ({1:N1} trials/second)", trials, trials / timeLimit);
        }

        /// <summary>
        /// Run a test with one randomly generated expression tree.
        /// </summary>
        private void DoOneTest()
        {
            // Unfortunately this generates a Func<KeyValuePair<int, int>, bool> instead
            // of a Predicate<KeyValuePair<int, int>. We work around that by calling
            // PredicateExpressionEvaluator directly.
            Expression<Predicate<KeyValuePair<string, string>>> expression = this.CreateExpression();
            Predicate<KeyValuePair<string, string>> func = expression.Compile();

            var actual = this.dictionary.Where(expression);
            var expected = data.Where(x => func(x)).ToList();
            EnumerableAssert.AreEqual(expected, actual, "expression = {0}", expression);
            expected.Reverse();
            EnumerableAssert.AreEqual(expected, actual.Reverse(), "expression = {0}", expression);
        }

        /// <summary>
        /// Create a random expression tree.
        /// </summary>
        /// <returns>A random expression tree.</returns>
        private Expression<Predicate<KeyValuePair<string, string>>> CreateExpression()
        {
            return Expression.Lambda<Predicate<KeyValuePair<string, string>>>(this.CreateBooleanExpression(), CreateParameterExpression());
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
        private Expression CreateKeyComparisonExpression()
        {
            switch (this.rand.Next(4))
            {
                case 0:
                    return this.CreateKeyEqualityComparisonExpression();
                case 1:
                    return this.CreateStartsWithExpression();
                case 2:
                    return this.CreateEqualsExpression();
                default:
                    return this.CreateStringCompareExpression();
            }
        }

        /// <summary>
        /// Create an expression for String.StartsWith.
        /// </summary>
        /// <returns>A String.StartsWith expression.</returns>
        private Expression CreateStartsWithExpression()
        {
            Expression parameter = CreateKeyAccess();
            Expression value = this.CreateConstantExpression();
            return Expression.Call(parameter, stringStartsWithMethod, value);
        }

        /// <summary>
        /// Create an expression for String.Equals.
        /// </summary>
        /// <returns>A String.Equals expression.</returns>
        private Expression CreateEqualsExpression()
        {
            Expression parameter = CreateKeyAccess();
            Expression value = this.CreateConstantExpression();
            return Expression.Call(parameter, stringEqualsMethod, value);
        }

        /// <summary>
        /// Create an expression like 'String.Compare(x.Key, "foo") == 0'.
        /// </summary>
        /// <returns>A key comparison expression.</returns>
        private BinaryExpression CreateStringCompareExpression()
        {
            Expression key = CreateKeyAccess();
            Expression value = this.CreateConstantExpression();
            Expression stringCompare = (0 == this.rand.Next(2))
                                           ? Expression.Call(null, stringCompareMethod, key, value)
                                           : Expression.Call(null, stringCompareMethod, value, key);

            Expression zero = Expression.Constant(0);

            Expression left;
            Expression right;
            if (0 == this.rand.Next(2))
            {
                left = stringCompare;
                right = zero;
            }
            else
            {
                left = zero;
                right = stringCompare;
            }

            switch (this.rand.Next(6))
            {
                case 0:
                    return Expression.LessThan(left, right);
                case 1:
                    return Expression.LessThanOrEqual(left, right);
                case 2:
                    return Expression.GreaterThanOrEqual(left, right);
                case 3:
                    return Expression.GreaterThan(left, right);
                case 4:
                    return Expression.NotEqual(left, right);
                default:
                    return Expression.Equal(left, right);
            }
        }

        /// <summary>
        /// Create a key equality comparison expression.
        /// </summary>
        /// <returns>A key comparison expression.</returns>
        private BinaryExpression CreateKeyEqualityComparisonExpression()
        {
            Expression parameter = CreateParameterExpression();
            Expression key = Expression.MakeMemberAccess(parameter, keyMemberInfo);
            Expression value = this.CreateConstantExpression();

            Expression left;
            Expression right;
            if (0 == this.rand.Next(2))
            {
                left = key;
                right = value;
            }
            else
            {
                left = value;
                right = key;
            }

            switch (this.rand.Next(2))
            {
                case 0:
                    return Expression.Equal(left, right);
                default:
                    return Expression.NotEqual(left, right);
            }
        }

        /// <summary>
        /// Create a new (random) constant expression.
        /// </summary>
        /// <returns>
        /// A constant expression with a value taken from the sample data.
        /// </returns>
        private ConstantExpression CreateConstantExpression()
        {
            return Expression.Constant(data[this.rand.Next(0, data.Length)].Key);
        }
    }
}