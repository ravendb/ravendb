//-----------------------------------------------------------------------
// <copyright file="JetApiHelpersTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for helper methods in the JetApi class.
    /// </summary>
    [TestClass]
    public class JetApiHelpersTests
    {
        /// <summary>
        /// Verify GetActualBookmarkSize returns 0 when passed 0.
        /// </summary>
        [TestMethod]
        [Description("Verify GetActualBookmarkSize returns 0 when passed 0")]
        [Priority(0)]
        public void VerifyGetActualBookmarkSizeReturnsZeroForZero()
        {
            Assert.AreEqual(0, JetApi.GetActualBookmarkSize(0U));
        }

        /// <summary>
        /// Verify GetActualBookmarkSize returns a positive number passed to it.
        /// </summary>
        [TestMethod]
        [Description("Verify GetActualBookmarkSize returns a positive number passed to it")]
        [Priority(0)]
        public void VerifyGetActualBookmarkSizeReturnsPositiveNumber()
        {
            Assert.AreEqual(17, JetApi.GetActualBookmarkSize(17U));
        }

        /// <summary>
        /// Verify GetActualBookmarkSize throws exception on overflow.
        /// </summary>
        [TestMethod]
        [Description("Verify GetActualBookmarkSize throws an exception on overflow")]
        [Priority(0)]
        [ExpectedException(typeof(OverflowException))]
        public void VerifyGetActualBookmarkSizeThrowsExceptionOnOverflow()
        {
            int ignored = JetApi.GetActualBookmarkSize(UInt32.MaxValue);
        }

        /// <summary>
        /// Verify GetActualBookmarkSize returns 0 for debug fill.
        /// </summary>
        [TestMethod]
        [Description("Verify GetActualBookmarkSize returns 0 for debug fill")]
        [Priority(0)]
        public void VerifyGetActualBookmarkSizeReturnsZeroForDebugFill()
        {
            Assert.AreEqual(0, JetApi.GetActualBookmarkSize(0xDDDDDDDD));
        }
    }
}