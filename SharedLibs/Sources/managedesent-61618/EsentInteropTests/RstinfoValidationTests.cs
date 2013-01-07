//-----------------------------------------------------------------------
// <copyright file="RstinfoValidationTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test JET_RSTINFO.CheckMembersAreValid.
    /// </summary>
    [TestClass]
    public class RstinfoValidationTests
    {
        /// <summary>
        /// Check that CheckMembersAreValid catches negative crstmap.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches negative crstmap")]
        public void VerifyValidityCatchesNegativeCrstmap()
        {
            var x = new JET_RSTINFO { crstmap = -1 };
            x.CheckMembersAreValid();
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches invalid crstmap.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches invalid crstmap")]
        public void VerifyValidityCatchesInvalidCrstmap()
        {
            var x = new JET_RSTINFO { crstmap = 1 };
            x.CheckMembersAreValid();
        }

        /// <summary>
        /// Check that CheckMembersAreValid catches too-long crstmap.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        [Description("Check that CheckMembersAreValid catches a too long crstmap")]
        public void VerifyValidityCatchesTooLongCrstmap()
        {
            var x = new JET_RSTINFO { crstmap = 3, rgrstmap = new JET_RSTMAP[2] };
            x.CheckMembersAreValid();
        }
    }
}