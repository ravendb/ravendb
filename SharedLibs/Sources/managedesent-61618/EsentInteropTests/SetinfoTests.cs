//-----------------------------------------------------------------------
// <copyright file="SetinfoTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// JET_SETINFO tests.
    /// </summary>
    [TestClass]
    public class SetinfoTests
    {
        /// <summary>
        /// Test conversion to the native stuct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion of JET_SETINFO to NATIVE_SETINFO")]
        public void ConvertSetinfoToNative()
        {
            var setinfo = new JET_SETINFO { ibLongValue = 1, itagSequence = 2 };

            NATIVE_SETINFO native = setinfo.GetNativeSetinfo();
            Assert.AreEqual(1U, native.ibLongValue);
            Assert.AreEqual(2U, native.itagSequence);
        }

        /// <summary>
        /// Test conversion to the native stuct when ibLongValue is negative.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        [Description("Test conversion of JET_SETINFO to NATIVE_SETINFO when ibLongValue is negative")]
        public void ConvertSetinfoToNativeWhenIbLongValueIsNegative()
        {
            var setinfo = new JET_SETINFO { ibLongValue = -1, itagSequence = 2 };

            NATIVE_SETINFO native = setinfo.GetNativeSetinfo();
        }

        /// <summary>
        /// Test conversion to the native stuct when itagSequence is negative
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        [Description("Test conversion of JET_SETINFO to NATIVE_SETINFO when itagSequence is negative")]
        public void ConvertSetinfoToNativeWhenItagSequenceIsNegative()
        {
            var setinfo = new JET_SETINFO { ibLongValue = 0, itagSequence = Int32.MinValue };

            NATIVE_SETINFO native = setinfo.GetNativeSetinfo();
        }
    }
}
