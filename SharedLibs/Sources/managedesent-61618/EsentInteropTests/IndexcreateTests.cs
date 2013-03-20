//-----------------------------------------------------------------------
// <copyright file="IndexcreateTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Runtime.InteropServices;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test conversion to NATIVE_INDEXCREATE
    /// </summary>
    [TestClass]
    public class IndexcreateTests
    {
        /// <summary>
        /// Managed version of the indexcreate structure.
        /// </summary>
        private JET_INDEXCREATE managed;

        /// <summary>
        /// The native conditional column structure created from the JET_INDEXCREATE
        /// object.
        /// </summary>
        private NATIVE_INDEXCREATE native;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the IndexcreateTests fixture")]
        public void Setup()
        {
            this.managed = new JET_INDEXCREATE()
                           {
                               szIndexName = "index",
                               szKey = "+foo\0-bar\0\0",
                               cbKey = 8,
                               grbit = CreateIndexGrbit.IndexSortNullsHigh,
                               ulDensity = 100,
                               pidxUnicode = null,
                               cbVarSegMac = 200,
                               rgconditionalcolumn = null,
                               cConditionalColumn = 0,
                           };
            this.native = this.managed.GetNativeIndexcreate();
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the structure size
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE sets the structure size")]
        public void VerifyConversionToNativeSetsCbStruct()
        {
            Assert.AreEqual((uint)Marshal.SizeOf(this.native), this.native.cbStruct);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the name
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE sets the index name")]
        public void VerifyConversionToNativeSetsName()
        {
            // Done at pinvoke time.
            Assert.AreEqual(IntPtr.Zero, this.native.szIndexName);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the key
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE sets the index key")]
        public void VerifyConversionToNativeSetsKey()
        {
            // Done at pinvoke time.
            Assert.AreEqual(IntPtr.Zero, this.native.szKey);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the key length
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE sets the key length")]
        public void VerifyConversionToNativeSetsKeyLength()
        {
            Assert.AreEqual((uint)8, this.native.cbKey);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the grbit
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE sets the grbit")]
        public void VerifyConversionToNativeSetsGrbit()
        {
            Assert.AreEqual((uint)CreateIndexGrbit.IndexSortNullsHigh, this.native.grbit);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the density
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE sets the density")]
        public void VerifyConversionToNativeSetsDensity()
        {
            Assert.AreEqual((uint)100, this.native.ulDensity);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the JET_UNICODEINDEX
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE sets the unicode index")]
        public unsafe void VerifyConversionToNativeSetsUnicodeIndexToNull()
        {
            Assert.IsTrue(null == this.native.pidxUnicode);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the cbVarSegMac
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE sets the cbVarSegMac")]
        public void VerifyConversionToNativeSetsCbVarSegMac()
        {
            Assert.AreEqual(new IntPtr(200), this.native.cbVarSegMac);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the JET_CONDITIONALCOLUMNs
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE sets the conditional columns")]
        public void VerifyConversionToNativeSetsConditionalColumnsToNull()
        {
            Assert.AreEqual(IntPtr.Zero, this.native.rgconditionalcolumn);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE sets the cConditionalColumn
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE sets the cConditionalColumn")]
        public void VerifyConversionToNativeSetsCConditionalColumn()
        {
            Assert.AreEqual((uint)0, this.native.cConditionalColumn);
        }
    }
}