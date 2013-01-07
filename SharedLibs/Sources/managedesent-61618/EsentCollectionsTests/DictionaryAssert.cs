// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DictionaryAssert.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Dictionary-related assertions. These provide asserts that are similar
//   to the CollectionAssert class.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Dictionary-related assertions.
    /// </summary>
    public static class DictionaryAssert
    {
        /// <summary>
        /// Compare the expected and actual dictionaries.
        /// </summary>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary value.</typeparam>
        /// <param name="expected">The expected dictionary values.</param>
        /// <param name="actual">The actual dictionary values.</param>
        public static void AreEqual<TKey, TValue>(IDictionary<TKey, TValue> expected, IDictionary<TKey, TValue> actual)
        {
            Assert.AreEqual(expected.Count, actual.Count);
            Assert.AreEqual(expected.Keys.Count, actual.Keys.Count);
            Assert.AreEqual(expected.Values.Count, actual.Values.Count);

            Assert.IsTrue(AreEquivalent(expected.Keys, actual.Keys));
            Assert.IsTrue(AreEquivalent(expected.Values, actual.Values));

            var enumeratedKeys = from i in actual select i.Key;
            Assert.IsTrue(AreEquivalent(expected.Keys, enumeratedKeys));

            var enumeratedValues = from i in actual select i.Value;
            Assert.IsTrue(AreEquivalent(expected.Values, enumeratedValues));

            var expectedItems = expected.OrderBy(x => x.Key);
            var actualItems = actual.OrderBy(x => x.Key);
            Assert.IsTrue(expectedItems.SequenceEqual(actualItems));

            if (expected.Count > 0)
            {
                Assert.AreEqual(expected.Keys.Min(), actual.Keys.Min());
                Assert.AreEqual(expected.Keys.Max(), actual.Keys.Max());
            }

            foreach (TKey k in expected.Keys)
            {
                Assert.IsTrue(actual.ContainsKey(k));
                Assert.IsTrue(actual.Keys.Contains(k));

                TValue v;
                Assert.IsTrue(actual.TryGetValue(k, out v));
                Assert.AreEqual(expected[k], v);
                Assert.AreEqual(expected[k], actual[k]);
                Assert.IsTrue(actual.Values.Contains(v));
                Assert.IsTrue(actual.Contains(new KeyValuePair<TKey, TValue>(k, v)));
            }
        }

        /// <summary>
        /// Compare the expected and actual dictionaries. The dictionaries should be sorted in the 
        /// same order.
        /// </summary>
        /// <typeparam name="TKey">The type of the dictionary key.</typeparam>
        /// <typeparam name="TValue">The type of the dictionary value.</typeparam>
        /// <param name="expected">The expected dictionary values.</param>
        /// <param name="actual">The actual dictionary values.</param>
        public static void AreSortedAndEqual<TKey, TValue>(IDictionary<TKey, TValue> expected, IDictionary<TKey, TValue> actual)
        {
            Assert.AreEqual(expected.Count, actual.Count);
            Assert.AreEqual(expected.Keys.Count, actual.Keys.Count);
            Assert.AreEqual(expected.Values.Count, actual.Values.Count);

            Assert.IsTrue(expected.SequenceEqual(actual));
            Assert.IsTrue(expected.Keys.SequenceEqual(actual.Keys));
            Assert.IsTrue(expected.Values.SequenceEqual(actual.Values));

            Assert.AreEqual(expected.FirstOrDefault(), actual.FirstOrDefault());
            Assert.AreEqual(expected.LastOrDefault(), actual.LastOrDefault());

            Assert.AreEqual(expected.Keys.FirstOrDefault(), actual.Keys.FirstOrDefault());
            Assert.AreEqual(expected.Keys.LastOrDefault(), actual.Keys.LastOrDefault());

            if (expected.Count > 0)
            {
                Assert.AreEqual(expected.Keys.Min(), actual.Keys.Min());
                Assert.AreEqual(expected.Keys.Max(), actual.Keys.Max());
                Assert.AreEqual(expected.Keys.First(), actual.Keys.First());
                Assert.AreEqual(expected.Keys.Last(), actual.Keys.Last());
                Assert.AreEqual(expected.First(), actual.First());
                Assert.AreEqual(expected.Last(), actual.Last());
            }
        }

        /// <summary>
        /// Determine if two enumerations are equivalent. Enumerations are
        /// equivalent if they contain the same members in any order.
        /// </summary>
        /// <typeparam name="T">The type of the enumeration.</typeparam>
        /// <param name="c1">The first enumeration.</param>
        /// <param name="c2">The second enumeration.</param>
        /// <returns>True if the enumerations are equivalent.</returns>
        private static bool AreEquivalent<T>(IEnumerable<T> c1, IEnumerable<T> c2)
        {
            var s1 = c1.OrderBy(x => x);
            var s2 = c2.OrderBy(x => x);
            return s1.SequenceEqual(s2);
        }
    }
}
