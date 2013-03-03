//-----------------------------------------------------------------------
// <copyright file="LogtimeToDateTimeTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test converting a JET_LOGTIME to a DateTime.
    /// </summary>
    [TestClass]
    public partial class LogtimeToDateTimeTests
    {
        /// <summary>
        /// Verify ToDateTime returns null when the JET_LOGTIME is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify ToDateTime returns null when the JET_LOGTIME is null")]
        public void TestDateTimeFromNullLogtime()
        {
            var logtime = new JET_LOGTIME();
            Assert.IsNull(logtime.ToDateTime());
        }

        /// <summary>
        /// Verify ToDateTime returns null when the JET_BKLOGTIME is null.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify ToDateTime returns null when the JET_BKLOGTIME is null")]
        public void TestDateTimeFromNullBklogtime()
        {
            var logtime = new JET_BKLOGTIME();
            Assert.IsNull(logtime.ToDateTime());
        }

        /// <summary>
        /// Test converting a local logtime to a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting a local logtime to a DateTime")]
        public void TestDateTimeFromLocalLogtime()
        {
            var expected = new DateTime(1972, 11, 5, 1, 23, 45, DateTimeKind.Local);
            var logtime = new JET_LOGTIME(expected);
            DateTime? actual = logtime.ToDateTime();
            Assert.AreEqual(expected, actual.Value);
            Assert.AreEqual(expected.Kind, actual.Value.Kind);
        }

        /// <summary>
        /// Test converting a local bklogtime to a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting a local bklogtime to a DateTime")]
        public void TestDateTimeFromLocalBklogtime()
        {
            var expected = new DateTime(1972, 11, 5, 1, 23, 45, DateTimeKind.Local);
            var logtime = new JET_BKLOGTIME(expected, false);
            DateTime? actual = logtime.ToDateTime();
            Assert.AreEqual(expected, actual.Value);
            Assert.AreEqual(expected.Kind, actual.Value.Kind);
        }

        /// <summary>
        /// Test converting a UTC logtime to a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting a UTC logtime to a DateTime")]
        public void TestDateTimeFromUtcLogtime()
        {
            var expected = new DateTime(1972, 11, 5, 1, 23, 45, DateTimeKind.Utc);
            var logtime = new JET_LOGTIME(expected);
            DateTime? actual = logtime.ToDateTime();
            Assert.AreEqual(expected, actual.Value);
            Assert.AreEqual(expected.Kind, actual.Value.Kind);
        }

        /// <summary>
        /// Test converting a UTC bklogtime to a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test converting a UTC bklogtime to a DateTime")]
        public void TestDateTimeFromUtcBklogtime()
        {
            var expected = new DateTime(1972, 11, 5, 1, 23, 45, DateTimeKind.Utc);
            var logtime = new JET_BKLOGTIME(expected, false);
            DateTime? actual = logtime.ToDateTime();
            Assert.AreEqual(expected, actual.Value);
            Assert.AreEqual(expected.Kind, actual.Value.Kind);
        }
    }
}