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
    /// Test JET_SETCOLUMN.Validate.
    /// </summary>
    [TestClass]
    public class SetColumnValidationTests
    {
        /// <summary>
        /// Validate should detect a negative data length.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_SETCOLUMN.Validate throws an exception on negative data length")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyValidateThrowsExceptionWhenCbDataIsNegative()
        {
            var setcolumn = new JET_SETCOLUMN
            {
                cbData = -1,
                pvData = new byte[1],
            };
            setcolumn.Validate();
        }

        /// <summary>
        /// Validate should detect null pvData and non-zero cbData.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_SETCOLUMN.Validate throws an exception on invalid cbData")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyValidateThrowsExceptionWhenPvDataIsNull()
        {
            var setcolumn = new JET_SETCOLUMN
            {
                cbData = 1,
            };
            setcolumn.Validate();
        }

        /// <summary>
        /// Validate should detect cbData that is too long.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_SETCOLUMN.Validate throws an exception on too-long cbData")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyValidateThrowsExceptionWhenCbDataIsTooLong()
        {
            var setcolumn = new JET_SETCOLUMN
            {
                cbData = 100,
                pvData = new byte[9],
            };
            setcolumn.Validate();
        }

        /// <summary>
        /// Validate should detect a negative ibLongValue.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_SETCOLUMN.Validate throws an exception on negative ibLongValue")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyValidateThrowsExceptionWhenIbLongValueIsNegative()
        {
            var setcolumn = new JET_SETCOLUMN
            {
                ibLongValue = -1,
            };
            setcolumn.Validate();
        }

        /// <summary>
        /// Validate should detect a negative itagSequence.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_SETCOLUMN.Validate throws an exception on negative itagSequence")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyValidateThrowsExceptionWhenItagSequenceIsNegative()
        {
            var setcolumn = new JET_SETCOLUMN
            {
                itagSequence = -1,
            };
            setcolumn.Validate();
        } 
    }
}