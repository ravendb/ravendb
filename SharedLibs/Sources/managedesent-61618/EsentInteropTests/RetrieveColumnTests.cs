//-----------------------------------------------------------------------
// <copyright file="RetrieveColumnTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test methods on the JET_RETRIEVECOLUMN class.
    /// </summary>
    [TestClass]
    public class RetrieveColumnTests
    {
        /// <summary>
        /// CheckDataSize should detect a negative data length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.CheckDataSize throws an exception when cbData is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckThrowsExceptionWhenCbDataIsNegative()
        {
            var setcolumn = new JET_RETRIEVECOLUMN
            {
                cbData = -1,
                pvData = new byte[1],
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// CheckDataSize should detect a negative data offset.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.CheckDataSize throws an exception when ibData is negative")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckThrowsExceptionWhenIbDataIsNegative()
        {
            var setcolumn = new JET_RETRIEVECOLUMN
            {
                ibData = -1,
                pvData = new byte[1],
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// CheckDataSize should detect null pvData and non-zero cbData.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.CheckDataSize throws an exception when cbData is invalid")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckThrowsExceptionWhenCbDataIsNonZeroPvDataIsNull()
        {
            var setcolumn = new JET_RETRIEVECOLUMN
            {
                cbData = 1,
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// CheckDataSize should detect null pvData and non-zero ibData.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.CheckDataSize throws an exception when ibData is invalid")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckThrowsExceptionWhenIbDataIsNonZeroAndPvDataIsNull()
        {
            var setcolumn = new JET_RETRIEVECOLUMN
            {
                ibData = 1,
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// CheckDataSize should detect cbData that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.CheckDataSize throws an exception when cbData is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckThrowsExceptionWhenCbDataIsTooLong()
        {
            var setcolumn = new JET_RETRIEVECOLUMN
            {
                cbData = 100,
                pvData = new byte[9],
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// CheckDataSize should detect ibData that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.CheckDataSize throws an exception when ibData is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckThrowsExceptionWhenIbDataIsTooLong()
        {
            var setcolumn = new JET_RETRIEVECOLUMN
            {
                ibData = 9,
                pvData = new byte[9],
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// CheckDataSize should detect ibData/cbData that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.CheckDataSize throws an exception when ibData/cbData is too long")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckThrowsExceptionWhenIbCbDataIsTooLong()
        {
            var setcolumn = new JET_RETRIEVECOLUMN
            {
                ibData = 8,
                cbData = 3,
                pvData = new byte[10],
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// UpdateFromNativeRetrievecolumn should set the cbActual member
        /// on the JET_RETRIEVECOLUMN.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.UpdateFromNativeRetrievecolumns sets cbActual")]
        public void VerifyUpdateFromNativeRetrievecolumnSetsCbactual()
        {
            var setcolumn = new JET_RETRIEVECOLUMN();
            var native = new NATIVE_RETRIEVECOLUMN { cbActual = 0x100 };
            setcolumn.UpdateFromNativeRetrievecolumn(ref native);
            Assert.AreEqual(0x100, setcolumn.cbActual);
        }

        /// <summary>
        /// UpdateFromNativeRetrievecolumn should set the columnidNextTagged
        /// member on the JET_RETRIEVECOLUMN.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.UpdateFromNativeRetrievecolumns sets columnidNextTagged")]
        public void VerifyUpdateFromNativeRetrievecolumnSetsColumnidNextTagged()
        {
            var setcolumn = new JET_RETRIEVECOLUMN();
            var native = new NATIVE_RETRIEVECOLUMN { columnidNextTagged = 0x20 };
            setcolumn.UpdateFromNativeRetrievecolumn(ref native);
            var expected = new JET_COLUMNID { Value = 0x20 };
            Assert.AreEqual(expected, setcolumn.columnidNextTagged);
        }

        /// <summary>
        /// UpdateFromNativeRetrievecolumn should set the itagSequence
        /// member on the JET_RETRIEVECOLUMN.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.UpdateFromNativeRetrievecolumns sets itagSequence")]
        public void VerifyUpdateFromNativeRetrievecolumnSetsItagSequence()
        {
            var setcolumn = new JET_RETRIEVECOLUMN();
            var native = new NATIVE_RETRIEVECOLUMN { itagSequence = 7 };
            setcolumn.UpdateFromNativeRetrievecolumn(ref native);
            Assert.AreEqual(7, setcolumn.itagSequence);
        }

        /// <summary>
        /// UpdateFromNativeRetrievecolumn should set the err
        /// member on the JET_RETRIEVECOLUMN.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.UpdateFromNativeRetrievecolumns sets err")]
        public void VerifyUpdateFromNativeRetrievecolumnSetsErr()
        {
            var setcolumn = new JET_RETRIEVECOLUMN();
            var native = new NATIVE_RETRIEVECOLUMN { err = 1004 };
            setcolumn.UpdateFromNativeRetrievecolumn(ref native);
            Assert.AreEqual(JET_wrn.ColumnNull, setcolumn.err);
        }
    }
}