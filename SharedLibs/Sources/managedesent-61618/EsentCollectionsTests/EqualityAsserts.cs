// --------------------------------------------------------------------------------------------------------------------
// <copyright file="EqualityAsserts.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Test the Key class.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Asserts to verify Equals() and GetHashCode() implementations.
    /// </summary>
    internal static class EqualityAsserts
    {
        /// <summary>
        /// Helper method to compare two objects for equality. This
        /// verifies Object.Equals, IEquatable.Equals and
        /// Object.GetHashCode.
        /// </summary>
        /// <typeparam name="T">The type of the object.</typeparam>
        /// <param name="a">The first object.</param>
        /// <param name="b">The second object.</param>
        /// <param name="areEqual">True if the object are the same.</param>
        public static void TestEqualsAndHashCode<T>(T a, T b, bool areEqual) where T : IEquatable<T>
        {
            // IEquatable<T>.Equals
            Assert.AreEqual(a.Equals(b), areEqual);
            Assert.AreEqual(b.Equals(a), areEqual);

            // Object.Equals
            Assert.AreEqual(a.Equals((object)b), areEqual);
            Assert.AreEqual(b.Equals((object)a), areEqual);

            // GetHashCode
            if (areEqual)
            {
                Assert.AreEqual(a.GetHashCode(), b.GetHashCode());
                Assert.AreEqual(a, b);
            }
            else
            {
                Assert.AreNotEqual(a.GetHashCode(), b.GetHashCode());
                Assert.AreNotEqual(a, b);
            }
        }
    }
}