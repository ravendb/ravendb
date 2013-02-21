//-----------------------------------------------------------------------
// <copyright file="SetColumnValidationTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test JET_SETCOLUMN.CheckDataSize.
    /// </summary>
    [TestClass]
    public class SetColumnValidationTests
    {
        /// <summary>
        /// CheckDataSize should detect a negative data length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_SETCOLUMN.CheckDataSize throws an exception on negative data length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckDataSizeThrowsExceptionWhenCbDataIsNegative()
        {
            var setcolumn = new JET_SETCOLUMN
            {
                cbData = -1,
                pvData = new byte[1],
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// CheckDataSize should detect a negative data length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_SETCOLUMN.CheckDataSize throws an exception on negative data offset")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckDataSizeThrowsExceptionWhenIbDataIsNegative()
        {
            var setcolumn = new JET_SETCOLUMN
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
        [Description("Verify that JET_SETCOLUMN.CheckDataSize throws an exception on invalid cbData")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckDataSizeThrowsExceptionWhenCbDataIsNonZeroAndPvDataIsNull()
        {
            var setcolumn = new JET_SETCOLUMN
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
        [Description("Verify that JET_SETCOLUMN.CheckDataSize throws an exception on invalid ibData")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckDataSizeThrowsExceptionWhenIbDataIsNonZeroAndPvDataIsNull()
        {
            var setcolumn = new JET_SETCOLUMN
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
        [Description("Verify that JET_SETCOLUMN.CheckDataSize throws an exception on too-long cbData")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckDataSizeThrowsExceptionWhenCbDataIsTooLong()
        {
            var setcolumn = new JET_SETCOLUMN
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
        [Description("Verify that JET_SETCOLUMN.CheckDataSize throws an exception on too-long ibData")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckDataSizeThrowsExceptionWhenIbDataIsTooLong()
        {
            var setcolumn = new JET_SETCOLUMN
            {
                ibData = 100,
                pvData = new byte[9],
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// CheckDataSize should detect ibData/cbData that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_SETCOLUMN.CheckDataSize throws an exception on too-long ibData/cbData")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckDataSizeThrowsExceptionWhenIbDataCbDataIsTooLong()
        {
            var setcolumn = new JET_SETCOLUMN
            {
                ibData = 4,
                pvData = new byte[4],
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// CheckDataSize should detect a negative ibLongValue.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_SETCOLUMN.CheckDataSize throws an exception on negative ibLongValue")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckDataSizeThrowsExceptionWhenIbLongValueIsNegative()
        {
            var setcolumn = new JET_SETCOLUMN
            {
                ibLongValue = -1,
            };
            setcolumn.CheckDataSize();
        }

        /// <summary>
        /// CheckDataSize should detect a negative itagSequence.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_SETCOLUMN.CheckDataSize throws an exception on negative itagSequence")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyCheckDataSizeThrowsExceptionWhenItagSequenceIsNegative()
        {
            var setcolumn = new JET_SETCOLUMN
            {
                itagSequence = -1,
            };
            setcolumn.CheckDataSize();
        } 
    }
}