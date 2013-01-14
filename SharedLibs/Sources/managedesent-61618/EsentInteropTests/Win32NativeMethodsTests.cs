//-----------------------------------------------------------------------
// <copyright file="Win32NativeMethodsTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop.Win32;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the the Win32NativeMethods helper class.
    /// </summary>
    [TestClass]
    public class Win32NativeMethodsTests
    {
        /// <summary>
        /// Verify ThrowExceptionOnNull does nothing when passed a non-null pointer.
        /// </summary>
        [TestMethod]
        [Description("Verify ThrowExceptionOnNull does nothing when passed a non-null pointer")]
        [Priority(0)]
        public void VerifyThrowExceptionOnNullDoesNothingOnNonNull()
        {
            NativeMethods.ThrowExceptionOnNull(new IntPtr(1), "unexpected");
        }

        /// <summary>
        /// Verify ThrowExceptionOnNull throws an exception when passed a null pointer.
        /// </summary>
        [TestMethod]
        [Description("Verify ThrowExceptionOnNull throws an exception when passed a null pointer")]
        [Priority(0)]
        [ExpectedException(typeof(System.ComponentModel.Win32Exception))]
        public void VerifyThrowExceptionOnNullThrowsExceptionOnNull()
        {
            NativeMethods.ThrowExceptionOnNull(IntPtr.Zero, "expected");
        }

        /// <summary>
        /// Verify ThrowExceptionOnFailure does nothing on success.
        /// </summary>
        [TestMethod]
        [Description("Verify ThrowExceptionOnFailure does nothing on success")]
        [Priority(0)]
        public void VerifyThrowExceptionOnFailureDoesNothingOnSuccess()
        {
            NativeMethods.ThrowExceptionOnFailure(true, "unexpected");
        }

        /// <summary>
        /// Verify ThrowExceptionOnFailure throws an exception on failure.
        /// </summary>
        [TestMethod]
        [Description("Verify ThrowExceptionOnFailure throws an exception on failure")]
        [Priority(0)]
        [ExpectedException(typeof(System.ComponentModel.Win32Exception))]
        public void VerifyThrowExceptionOnFailureThrowsExceptionOnFailure()
        {
            NativeMethods.ThrowExceptionOnFailure(false, "expected");
        }
    }
}