//-----------------------------------------------------------------------
// <copyright file="RetinfoTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// JET_RETINFO tests
    /// </summary>
    [TestClass]
    public class RetinfoTests
    {
        /// <summary>
        /// Test conversion to the native stuct
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion of JET_RETINFO to NATIVE_RETINFO")]
        public void ConvertRetinfoToNative()
        {
            var retinfo = new JET_RETINFO { ibLongValue = 1, itagSequence = 2 };

            NATIVE_RETINFO native = retinfo.GetNativeRetinfo();
            Assert.AreEqual(1U, native.ibLongValue);
            Assert.AreEqual(2U, native.itagSequence);
        }

        /// <summary>
        /// Test conversion to the native stuct when ibLongValue is negative
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        [Description("Test conversion of JET_RETINFO to NATIVE_RETINFO when ibLongValue is negative")]
        public void ConvertRetinfoToNativeWhenIbLongValueIsNegative()
        {
            var retinfo = new JET_RETINFO { ibLongValue = -1, itagSequence = 2 };

            NATIVE_RETINFO native = retinfo.GetNativeRetinfo();
        }

        /// <summary>
        /// Test conversion to the native stuct when itagSequence is negative
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        [Description("Test conversion of JET_RETINFO to NATIVE_RETINFO when itagSequence is negative")]
        public void ConvertRetinfoToNativeWhenItagSequenceIsNegative()
        {
            var retinfo = new JET_RETINFO { ibLongValue = 0, itagSequence = Int32.MinValue };

            NATIVE_RETINFO native = retinfo.GetNativeRetinfo();
        }

        /// <summary>
        /// Test conversion from the native stuct
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion of NATIVE_RETINFO to JET_RETINFO")]
        public void ConvertRetinfoFromNative()
        {
            var native = new NATIVE_RETINFO { columnidNextTagged = 257 };

            var retinfo = new JET_RETINFO();
            retinfo.SetFromNativeRetinfo(native);

            Assert.AreEqual(257U, retinfo.columnidNextTagged.Value);
        }

        /// <summary>
        /// Test conversion to a native struct and back again
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion of JET_RETINFO to NATIVE_RETINFO and back")]
        public void ConvertRetinfo()
        {
            var retinfo = new JET_RETINFO { ibLongValue = 1, itagSequence = 2 };

            NATIVE_RETINFO native = retinfo.GetNativeRetinfo();
            native.columnidNextTagged = 300;

            retinfo.SetFromNativeRetinfo(native);
            Assert.AreEqual(1, retinfo.ibLongValue);
            Assert.AreEqual(2, retinfo.itagSequence);
            Assert.AreEqual(300U, retinfo.columnidNextTagged.Value);
        }
    }
}
