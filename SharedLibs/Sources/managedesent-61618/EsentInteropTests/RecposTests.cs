//-----------------------------------------------------------------------
// <copyright file="RecposTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// JET_RECPOS tests.
    /// </summary>
    [TestClass]
    public class RecposTests
    {
        /// <summary>
        /// Test conversion to the native stuct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion of JET_RECPOS to NATIVE_RECPOS")]
        public void ConvertRecposToNative()
        {
            var recpos = new JET_RECPOS();
            recpos.centriesLT = 5;
            recpos.centriesTotal = 10;

            NATIVE_RECPOS native = recpos.GetNativeRecpos();
            Assert.AreEqual(5U, native.centriesLT);
            Assert.AreEqual(10U, native.centriesTotal);
        }

        /// <summary>
        /// Test conversion to the native stuct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        [Description("Test conversion of JET_RECPOS to NATIVE_RECPOS with a negative centriesLT")]
        public void ConvertRecposToNativeWithNegativeCentriesLt()
        {
            var recpos = new JET_RECPOS();
            recpos.centriesLT = -1;
            recpos.centriesTotal = 10;

            NATIVE_RECPOS native = recpos.GetNativeRecpos();
        }

        /// <summary>
        /// Test conversion to the native stuct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        [Description("Test conversion of JET_RECPOS to NATIVE_RECPOS with a negative centriesTotal")]
        public void ConvertRecposToNativeWithNegativeCentriesTotal()
        {
            var recpos = new JET_RECPOS();
            recpos.centriesLT = 1;
            recpos.centriesTotal = Int64.MinValue;

            NATIVE_RECPOS native = recpos.GetNativeRecpos();
        }

        /// <summary>
        /// Test conversion from the native stuct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion of NATIVE_RECPOS to JET_RECPOS")]
        public void ConvertRecposFromNative()
        {
            var native = new NATIVE_RECPOS();
            native.centriesLT = 1;
            native.centriesTotal = 2;

            var recpos = new JET_RECPOS();
            recpos.SetFromNativeRecpos(native);

            Assert.AreEqual(1, recpos.centriesLT);
            Assert.AreEqual(2, recpos.centriesTotal);
        }
    }
}
