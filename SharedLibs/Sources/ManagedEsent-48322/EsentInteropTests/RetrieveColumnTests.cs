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
        /// CheckDataSize should detect null pvData and non-zero cbData.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.CheckDataSize throws an exception when cbData is invalid")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckThrowsExceptionWhenPvDataIsNull()
        {
            var setcolumn = new JET_RETRIEVECOLUMN
            {
                cbData = 1,
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
        /// UpdateFromNativeRetrievecolumn should set the cbActual member
        /// on the JET_RETRIEVECOLUMN.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.UpdateFromNAtiveRetrievecolumns sets cbActual")]
        public void VerifyUpdateFromNativeRetrievecolumnSetsCbactual()
        {
            var setcolumn = new JET_RETRIEVECOLUMN();
            var native = new NATIVE_RETRIEVECOLUMN { cbActual = 0x100 };
            setcolumn.UpdateFromNativeRetrievecolumn(native);
            Assert.AreEqual(0x100, setcolumn.cbActual);
        }

        /// <summary>
        /// UpdateFromNativeRetrievecolumn should set the columnidNextTagged
        /// member on the JET_RETRIEVECOLUMN.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.UpdateFromNAtiveRetrievecolumns sets columnidNextTagged")]
        public void VerifyUpdateFromNativeRetrievecolumnSetsColumnidNextTagged()
        {
            var setcolumn = new JET_RETRIEVECOLUMN();
            var native = new NATIVE_RETRIEVECOLUMN { columnidNextTagged = 0x20 };
            setcolumn.UpdateFromNativeRetrievecolumn(native);
            var expected = new JET_COLUMNID { Value = 0x20 };
            Assert.AreEqual(expected, setcolumn.columnidNextTagged);
        }

        /// <summary>
        /// UpdateFromNativeRetrievecolumn should set the err
        /// member on the JET_RETRIEVECOLUMN.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_RETRIEVECOLUMN.UpdateFromNAtiveRetrievecolumns sets err")]
        public void VerifyUpdateFromNativeRetrievecolumnSetsErr()
        {
            var setcolumn = new JET_RETRIEVECOLUMN();
            var native = new NATIVE_RETRIEVECOLUMN { err = 1004 };
            setcolumn.UpdateFromNativeRetrievecolumn(native);
            Assert.AreEqual(JET_wrn.ColumnNull, setcolumn.err);
        }
    }
}
