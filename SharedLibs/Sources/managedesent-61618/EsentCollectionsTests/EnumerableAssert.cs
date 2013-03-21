// --------------------------------------------------------------------------------------------------------------------
// <copyright file="EnumerableAssert.cs" company="Microsoft Corporation">
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
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Asserts that deal with IEnumerable.
    /// </summary>
    public static class EnumerableAssert
    {
        /// <summary>
        /// Assert that two enumerable sequences are identical.
        /// </summary>
        /// <typeparam name="T">The type of object being enumerated.</typeparam>
        /// <param name="expected">The expected sequence.</param>
        /// <param name="actual">The actual sequence.</param>
        /// <param name="format">Format string for error message.</param>
        /// <param name="parameters">Parameters for the error message.</param>
        public static void AreEqual<T>(IEnumerable<T> expected, IEnumerable<T> actual, string format, params object[] parameters)
        {
            string message = (null == format) ? String.Empty : String.Format(format, parameters);
            using (IEnumerator<T> expectedEnumerator = expected.GetEnumerator())
            using (IEnumerator<T> actualEnumerator = actual.GetEnumerator())
            {
                int i = 0;
                while (expectedEnumerator.MoveNext())
                {
                    Assert.IsTrue(
                        actualEnumerator.MoveNext(),
                        "Error at entry {0}. Not enough entries in actual. First missing entry is {1} ({2})",
                        i,
                        expectedEnumerator.Current,
                        message);
                    Assert.AreEqual(
                        expectedEnumerator.Current,
                        actualEnumerator.Current,
                        "Error at entry {0}. Enumerators differ. ({1})",
                        i,
                        message);
                    i++;
                }

                Assert.IsFalse(
                    actualEnumerator.MoveNext(),
                    "Error. Expected enumerator has {0} entries. Actual enumerator has more. First extra entry is {1}. ({2})",
                    i,
                    actualEnumerator.Current,
                    message);
            }
        }
    }
}