//-----------------------------------------------------------------------
// <copyright file="ThreadstatsTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// JET_THREADSTATS tests
    /// </summary>
    [TestClass]
    public class ThreadstatTests
    {
        /// <summary>
        /// Native version of the threadstats.
        /// </summary>
        private NATIVE_THREADSTATS native;

        /// <summary>
        /// Managed version of the threadstats, created from the native.
        /// </summary>
        private JET_THREADSTATS managed;
 
        /// <summary>
        /// Initialize the native and managed objects.
        /// </summary>
        [TestInitialize]
        [Description("Setup the ThreadstatTests fixture")]
        public void Setup()
        {
            this.native = new NATIVE_THREADSTATS
            {
                cPageReferenced = 1,
                cPageRead = 2,
                cPagePreread = 3,
                cPageDirtied = 4,
                cPageRedirtied = 5,
                cLogRecord = 6,
                cbLogRecord = 7,
            };
            this.managed = new JET_THREADSTATS();
            this.managed.SetFromNativeThreadstats(this.native);
        }

        /// <summary>
        /// Test conversion from the native stuct
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_THREADSTATS.SetFromNativeThreadstats sets cPageReferenced")]
        public void TestSetFromNativeSetsCpageReferenced()
        {
            Assert.AreEqual(1, this.managed.cPageReferenced);
        }

        /// <summary>
        /// Test conversion from the native stuct
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_THREADSTATS.SetFromNativeThreadstats sets cPageRead")]
        public void TestSetFromNativeSetsCpageRead()
        {
            Assert.AreEqual(2, this.managed.cPageRead);
        }

        /// <summary>
        /// Test conversion from the native stuct
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_THREADSTATS.SetFromNativeThreadstats sets cPagePreread")]
        public void TestSetFromNativeSetsCpagePreread()
        {
            Assert.AreEqual(3, this.managed.cPagePreread);
        }

        /// <summary>
        /// Test conversion from the native stuct
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_THREADSTATS.SetFromNativeThreadstats sets cPageDirtied")]
        public void TestSetFromNativeSetsCpageDirtied()
        {
            Assert.AreEqual(4, this.managed.cPageDirtied);
        }

        /// <summary>
        /// Test conversion from the native stuct
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_THREADSTATS.SetFromNativeThreadstats sets cPageRedirtied")]
        public void TestSetFromNativeSetsCpageRedirtied()
        {
            Assert.AreEqual(5, this.managed.cPageRedirtied);
        }

        /// <summary>
        /// Test conversion from the native stuct
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_THREADSTATS.SetFromNativeThreadstats sets cLogRecord")]
        public void TestSetFromNativeSetsClogrecord()
        {
            Assert.AreEqual(6, this.managed.cLogRecord);
        }

        /// <summary>
        /// Test conversion from the native stuct
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_THREADSTATS.SetFromNativeThreadstats sets cbLogRecord")]
        public void TestSetFromNativeSetsCblogrecord()
        {
            Assert.AreEqual(7, this.managed.cbLogRecord);
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

            JET_THREADSTATS sum = t1 - t2;
            Assert.AreEqual(12, sum.cPageReferenced);
            Assert.AreEqual(10, sum.cPageRead);
            Assert.AreEqual(8, sum.cPagePreread);
            Assert.AreEqual(6, sum.cPageDirtied);
            Assert.AreEqual(4, sum.cPageRedirtied);
            Assert.AreEqual(2, sum.cLogRecord);
            Assert.AreEqual(0, sum.cbLogRecord);
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
    }
}