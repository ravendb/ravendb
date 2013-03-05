//-----------------------------------------------------------------------
// <copyright file="ComparableTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for classes that implement IComparable
    /// </summary>
    [TestClass]
    public class ComparableTests
    {
        /// <summary>
        /// Check that JET_LGPOS structures can be compared.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_LGPOS structures can be compared")]
        public void VerifyJetLgposComparison()
        {
            // These positions are in ascending order
            var positions = new[]
            {
                new JET_LGPOS { lGeneration = 1, isec = 3, ib = 5 },
                new JET_LGPOS { lGeneration = 1, isec = 3, ib = 6 },
                new JET_LGPOS { lGeneration = 1, isec = 4, ib = 4 },
                new JET_LGPOS { lGeneration = 1, isec = 4, ib = 5 },
                new JET_LGPOS { lGeneration = 2, isec = 2, ib = 2 },
                new JET_LGPOS { lGeneration = 2, isec = 3, ib = 5 },
            };

            // It would be nice if this was a generic helper method, but that won't
            // work for the operators.
            for (int i = 0; i < positions.Length - 1; ++i)
            {
                TestEqualObjects(positions[i], positions[i]);
                Assert.IsTrue(positions[i] <= positions[i], "<=");
                Assert.IsTrue(positions[i] >= positions[i], ">=");

                for (int j = i + 1; j < positions.Length; ++j)
                {
                    TestOrderedObjects(positions[i], positions[j]);
                    Assert.IsTrue(positions[i] < positions[j], "<");
                    Assert.IsTrue(positions[i] <= positions[j], "<=");
                    Assert.IsTrue(positions[j] > positions[i], ">");
                    Assert.IsTrue(positions[j] >= positions[i], ">=");
                }
            }
        }

        /// <summary>
        /// Check that JET_COLUMNID structures can be compared.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that JET_COLUMNID structures can be compared")]
        public void VerifyJetColumnidComparison()
        {
            // These positions are in ascending order
            var columnids = new[]
            {
                JET_COLUMNID.Nil,
                new JET_COLUMNID { Value = 1U },
                new JET_COLUMNID { Value = 2U },
                new JET_COLUMNID { Value = 3U },
                new JET_COLUMNID { Value = 4U },
            };

            // It would be nice if this was a generic helper method, but that won't
            // work for the operators.);
            for (int i = 0; i < columnids.Length - 1; ++i)
            {
                TestEqualObjects(columnids[i], columnids[i]);
                Assert.IsTrue(columnids[i] <= columnids[i], "<=");
                Assert.IsTrue(columnids[i] >= columnids[i], ">=");

                for (int j = i + 1; j < columnids.Length; ++j)
                {
                    TestOrderedObjects(columnids[i], columnids[j]);
                    Assert.IsTrue(columnids[i] < columnids[j], "<");
                    Assert.IsTrue(columnids[i] <= columnids[j], "<=");
                    Assert.IsTrue(columnids[j] > columnids[i], ">");
                    Assert.IsTrue(columnids[j] >= columnids[i], ">=");
                }
            }
        }

        /// <summary>
        /// Helper method to compare two equal objects.
        /// </summary>
        /// <typeparam name="T">The object type.</typeparam>
        /// <param name="a">The first object.</param>
        /// <param name="b">The second object.</param>
        private static void TestEqualObjects<T>(T a, T b) where T : struct, IComparable<T>
        {
            Assert.AreEqual(0, a.CompareTo(b), "{0}.CompareTo({1})", a, b);
            Assert.AreEqual(0, b.CompareTo(a), "{0}.CompareTo({1})", b, a);
        }

        /// <summary>
        /// Helper method to compare two ordered objects.
        /// </summary>
        /// <typeparam name="T">The object type.</typeparam>
        /// <param name="smaller">The smaller object.</param>
        /// <param name="larger">The larer object.</param>
        private static void TestOrderedObjects<T>(T smaller, T larger) where T : struct, IComparable<T>
        {
            int compare = smaller.CompareTo(larger);
            Assert.IsTrue(compare < 0, "expected < 0 ({0})", compare);
            compare = larger.CompareTo(smaller);
            Assert.IsTrue(compare > 0, "expected > 0 ({0})", compare);
        }
    }
}