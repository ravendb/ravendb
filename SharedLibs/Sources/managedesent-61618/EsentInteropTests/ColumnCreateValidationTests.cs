//-----------------------------------------------------------------------
// <copyright file="ColumnCreateValidationTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test JET_COLUMNCREATE.CheckMembersAreValid.
    /// </summary>
    [TestClass]
    public class ColumnCreateValidationTests
    {
        /// <summary>
        /// Check that CheckMembersAreValid catches null column name.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentNullException))]
        [Description("Check that CheckMembersAreValid catches null column name")]
        public void VerifyValidityCatchesNullColumnName()
        {
            var x = new JET_COLUMNCREATE
            {
                szColumnName = null,
            };
            x.CheckMembersAreValid();
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches negative cbDefault.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches negative cbDefault")]
        public void VerifyValidityCatchesNegativeCbDefault()
        {
            var x = new JET_COLUMNCREATE
            {
                szColumnName = "column9",
                pvDefault = new byte[0],
                cbDefault = -53,
            };
            x.CheckMembersAreValid();
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches invalid cbDefault.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches invalid cbDefault")]
        public void VerifyValidityCatchesInvalidCbDefault()
        {
            var x = new JET_COLUMNCREATE
            {
                szColumnName = "column9",
                pvDefault = null,
                cbDefault = 1,
            };
            x.CheckMembersAreValid();
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches too long cbDefault.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches wrong cbDefault")]
        public void VerifyValidityCatchesWrongCbDefault()
        {
            var x = new JET_COLUMNCREATE
            {
                szColumnName = "column9",
                pvDefault = new byte[5],
                cbDefault = 6,
            };
            x.CheckMembersAreValid();
        }
    }
}