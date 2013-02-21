//-----------------------------------------------------------------------
// <copyright file="UnicodeIndexTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test conversion to NATIVE_COLUMNDEF
    /// </summary>
    [TestClass]
    public class UnicodeIndexTests
    {
        /// <summary>
        /// Managed object being tested.
        /// </summary>
        private JET_UNICODEINDEX managed;

        /// <summary>
        /// The native conditional column structure created from the JET_UNICODEINDEX
        /// object.
        /// </summary>
        private NATIVE_UNICODEINDEX native;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the UnicodeIndexTests fixture")]
        public void Setup()
        {
            this.managed = new JET_UNICODEINDEX()
            {
                lcid = 1033,
                dwMapFlags = 0x400,
            };
            this.native = this.managed.GetNativeUnicodeIndex();
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the map flags
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that converting a JET_UNICODEINDEX to a NATIVE_UNICODEINDEX sets the map flags")]
        public void VerifyConversionToNativeSetsDwMapFlags()
        {
            Assert.AreEqual((uint)0x400, this.native.dwMapFlags);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the lcid
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that converting a JET_UNICODEINDEX to a NATIVE_UNICODEINDEX sets the lcid")]
        public void VerifyConversionToNativeSetsLcid()
        {
            Assert.AreEqual((uint)1033, this.native.lcid);
        }
    }
}