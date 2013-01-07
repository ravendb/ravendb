//-----------------------------------------------------------------------
// <copyright file="SnprogConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test conversion from an NATIVE_SNPROG to JET_SNPROG.
    /// </summary>
    [TestClass]
    public class SnprogConversionTests
    {
        /// <summary>
        /// The native structure.
        /// </summary>
        private NATIVE_SNPROG native;

        /// <summary>
        /// The managed version set from the native structure.
        /// </summary>
        private JET_SNPROG managed;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the SnprogConversionTests test fixture")]
        public void Setup()
        {
            this.native = new NATIVE_SNPROG
            {
                cbStruct = checked((uint)NATIVE_SNPROG.Size),
                cunitDone = 2,
                cunitTotal = 7,
            };
            this.managed = new JET_SNPROG();
            this.managed.SetFromNative(this.native);
        }

        /// <summary>
        /// Check the conversion to a native structure sets cunitDone.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from NATIVE_SNPROG to JET_SNPROG sets cunitDone")]
        public void VerifySetFromNativeSetsCunitDone()
        {
            Assert.AreEqual(2, this.managed.cunitDone);
        }

        /// <summary>
        /// Check the conversion to a native structure sets cunitTotal.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from NATIVE_SNPROG to JET_SNPROG sets cunitTotal")]
        public void VerifySetFromNativeSetsCunitTotal()
        {
            Assert.AreEqual(7, this.managed.cunitTotal);
        }
    }
}