//-----------------------------------------------------------------------
// <copyright file="EnumerableTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System.Collections;
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test an enumerable class.
    /// </summary>
    internal class EnumerableTests
    {
        /// <summary>
        /// Test an enumerable.
        /// </summary>
        /// <typeparam name="T">The type returned by the enumerator.</typeparam>
        /// <param name="enumerable">The enumerable to test.</param>
        internal static void TestEnumerable<T>(IEnumerable<T> enumerable)
        {
            TestEnumerator(enumerable.GetEnumerator());
            Assert.IsNotNull(((IEnumerable)enumerable).GetEnumerator());

            IEnumerator<T> enumerator1 = enumerable.GetEnumerator();
            IEnumerator<T> enumerator2 = enumerable.GetEnumerator();
            Assert.AreNotEqual(enumerator1, enumerator2, "Got back the same enumerator");
            enumerator1.Dispose();
            enumerator2.Dispose();

            // Creating an enumerator should use very few resources. This shouldn't fail.);
            for (int i = 0; i < 10000; ++i)
            {
                enumerable.GetEnumerator();
            }
        }

        /// <summary>
        /// Test an enumerator.
        /// </summary>
        /// <typeparam name="T">The type returned by the enumerator.</typeparam>
        /// <param name="enumerator">The enumerator to test this must be non-empty.</param>
        private static void TestEnumerator<T>(IEnumerator<T> enumerator)
        {
            Assert.IsTrue(enumerator.MoveNext(), "First move next failed");
            T first = enumerator.Current;
            object obj = ((IEnumerator)enumerator).Current;
            Assert.AreEqual(obj, first, "Current and IEnumerator.Current returned different results");
            while (enumerator.MoveNext())
            {
            }

            Assert.IsFalse(enumerator.MoveNext(), "MoveNext when at end should return false");
            enumerator.Reset();
            Assert.IsTrue(enumerator.MoveNext(), "MoveNext after Reset fails");
            Assert.IsNotNull(enumerator.Current);
            enumerator.Dispose();
        }        
    }
}