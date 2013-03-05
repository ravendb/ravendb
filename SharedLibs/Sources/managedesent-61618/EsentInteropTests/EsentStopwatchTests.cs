//-----------------------------------------------------------------------
// <copyright file="EsentStopwatchTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the EsentStopwatch class.
    /// </summary>
    [TestClass]
    public class EsentStopwatchTests
    {
        /// <summary>
        /// Start and then stop an EsentStopwatch.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Start and then stop an EsentStopwatch")]
        public void TestStartAndStopEsentStopwatch()
        {
            var stopwatch = new EsentStopwatch();
            stopwatch.Start();
            stopwatch.Stop();
        }

        /// <summary>
        /// StartNew and then stop an EsentStopwatch.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("StartNew and then stop an EsentStopwatch")]
        public void TestStartNewAndStopEsentStopwatch()
        {
            var stopwatch = EsentStopwatch.StartNew();
            stopwatch.Stop();
        }

        /// <summary>
        /// Reset an EsentStopwatch.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Reset an EsentStopwatch")]
        public void TestResetEsentStopwatch()
        {
            var stopwatch = EsentStopwatch.StartNew();
            stopwatch.Stop();
            stopwatch.Reset();
            Assert.AreEqual(TimeSpan.Zero, stopwatch.Elapsed);
        }

        /// <summary>
        /// Test EsentStopwatch.ToString() on a running stopwatch.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test EsentStopwatch.ToString() on a running stopwatch")]
        public void TestRunningStopwatchToString()
        {
            var stopwatch = new EsentStopwatch();
            stopwatch.Start();
            Assert.AreEqual("EsentStopwatch (running)", stopwatch.ToString());
        }

        /// <summary>
        /// Test EsentStopwatch.ToString() on a running stopwatch.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test EsentStopwatch.ToString() on a stopped stopwatch")]
        public void TestStoppedStopwatchToString()
        {
            var stopwatch = new EsentStopwatch();
            stopwatch.Start();
            stopwatch.Stop();
            Assert.AreEqual(stopwatch.Elapsed.ToString(), stopwatch.ToString());
        }
    }
}