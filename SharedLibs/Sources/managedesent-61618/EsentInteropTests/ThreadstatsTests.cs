//-----------------------------------------------------------------------
// <copyright file="ThreadstatsTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Diagnostics;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// JET_THREADSTATS tests
    /// </summary>
    [TestClass]
    public class ThreadstatTests
    {
        /// <summary>
        /// Test the Create method.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_THREADSTATS.Create sets the members")]
        public void TestCreateSetsMembers()
        {
            JET_THREADSTATS actual = JET_THREADSTATS.Create(1, 2, 3, 4, 5, 6, 7);
            Assert.AreEqual(1, actual.cPageReferenced);
            Assert.AreEqual(2, actual.cPageRead);
            Assert.AreEqual(3, actual.cPagePreread);
            Assert.AreEqual(4, actual.cPageDirtied);
            Assert.AreEqual(5, actual.cPageRedirtied);
            Assert.AreEqual(6, actual.cLogRecord);
            Assert.AreEqual(7, actual.cbLogRecord);
        }

        /// <summary>
        /// Test adding two JET_THREADSTATS
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test adding two JET_THREADSTATS")]
        public void TestJetThreadstatsAddition()
        {
            var t1 = new JET_THREADSTATS
            {
                cPageReferenced = 1,
                cPageRead = 2,
                cPagePreread = 3,
                cPageDirtied = 4,
                cPageRedirtied = 5,
                cLogRecord = 6,
                cbLogRecord = 7,
            };
            var t2 = new JET_THREADSTATS
            {
                cPageReferenced = 8,
                cPageRead = 9,
                cPagePreread = 10,
                cPageDirtied = 11,
                cPageRedirtied = 12,
                cLogRecord = 13,
                cbLogRecord = 14,
            };

            JET_THREADSTATS sum = t1 + t2;
            Assert.AreEqual(9, sum.cPageReferenced);
            Assert.AreEqual(11, sum.cPageRead);
            Assert.AreEqual(13, sum.cPagePreread);
            Assert.AreEqual(15, sum.cPageDirtied);
            Assert.AreEqual(17, sum.cPageRedirtied);
            Assert.AreEqual(19, sum.cLogRecord);
            Assert.AreEqual(21, sum.cbLogRecord);

            Assert.AreEqual(sum, JET_THREADSTATS.Add(t1, t2));
        }

        /// <summary>
        /// Test subtracting two JET_THREADSTATS
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test subtracting two JET_THREADSTATS")]
        public void TestJetThreadstatsSubtraction()
        {
            var t1 = new JET_THREADSTATS
            {
                cPageReferenced = 20,
                cPageRead = 19,
                cPagePreread = 18,
                cPageDirtied = 17,
                cPageRedirtied = 16,
                cLogRecord = 15,
                cbLogRecord = 14,
            };
            var t2 = new JET_THREADSTATS
            {
                cPageReferenced = 8,
                cPageRead = 9,
                cPagePreread = 10,
                cPageDirtied = 11,
                cPageRedirtied = 12,
                cLogRecord = 13,
                cbLogRecord = 14,
            };

            JET_THREADSTATS difference = t1 - t2;
            Assert.AreEqual(12, difference.cPageReferenced);
            Assert.AreEqual(10, difference.cPageRead);
            Assert.AreEqual(8, difference.cPagePreread);
            Assert.AreEqual(6, difference.cPageDirtied);
            Assert.AreEqual(4, difference.cPageRedirtied);
            Assert.AreEqual(2, difference.cLogRecord);
            Assert.AreEqual(0, difference.cbLogRecord);

            Assert.AreEqual(difference, JET_THREADSTATS.Subtract(t1, t2));
        }

        /// <summary>
        /// Test JET_THREADSTATS.ToString()
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_THREADSTATS.ToString with singular counts")]
        public void TestJetThreadstatsToStringSingular()
        {
            var t = new JET_THREADSTATS
            {
                cPageReferenced = 1,
                cPageRead = 1,
                cPagePreread = 1,
                cPageDirtied = 1,
                cPageRedirtied = 1,
                cLogRecord = 1,
                cbLogRecord = 1,
            };
            const string Expected = "1 page reference, 1 page read, 1 page preread, 1 page dirtied, 1 page redirtied, 1 log record, 1 byte logged";
            Assert.AreEqual(Expected, t.ToString());
        }

        /// <summary>
        /// Test JET_THREADSTATS.ToString()
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_THREADSTATS.ToString with zero counts")]
        public void TestJetThreadstatsToStringZero()
        {
            var t = new JET_THREADSTATS
            {
                cPageReferenced = 0,
                cPageRead = 0,
                cPagePreread = 0,
                cPageDirtied = 0,
                cPageRedirtied = 0,
                cLogRecord = 0,
                cbLogRecord = 0,
            };
            const string Expected = "0 page references, 0 pages read, 0 pages preread, 0 pages dirtied, 0 pages redirtied, 0 log records, 0 bytes logged";
            Assert.AreEqual(Expected, t.ToString());
        }

        /// <summary>
        /// Test JET_THREADSTATS.ToString()
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_THREADSTATS.ToString with plural counts")]
        public void TestJetThreadstatsToString()
        {
            var t = new JET_THREADSTATS
            {
                cPageReferenced = 10,
                cPageRead = 2,
                cPagePreread = 3,
                cPageDirtied = 4,
                cPageRedirtied = 5,
                cLogRecord = 6,
                cbLogRecord = 7,
            };
            const string Expected = "10 page references, 2 pages read, 3 pages preread, 4 pages dirtied, 5 pages redirtied, 6 log records, 7 bytes logged";
            Assert.AreEqual(Expected, t.ToString());
        }

        /// <summary>
        /// Test JET_THREADSTATS.ToString() performance.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JET_THREADSTATS.ToString performance")]
        public void TestJetThreadstatsToStringPerf()
        {
            var t = new JET_THREADSTATS
            {
                cPageReferenced = 10,
                cPageRead = 5,
                cPagePreread = 4,
                cPageDirtied = 3,
                cPageRedirtied = 2,
                cLogRecord = 1,
                cbLogRecord = 0,
            };

            // Call the method once to make sure it is compiled.
            string ignored = t.ToString();

            const int N = 100000;
            Stopwatch s = Stopwatch.StartNew();
            for (int i = 0; i < N; ++i)
            {
                ignored = t.ToString();
            }

            s.Stop();

            double ms = Math.Max(1, s.ElapsedMilliseconds);
            Console.WriteLine("{0} calls in {1} ({2} ms/call)", N, s.Elapsed, ms / N);
        }
    }
}