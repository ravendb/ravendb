//-----------------------------------------------------------------------
// <copyright file="Indexcreate1Tests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Runtime.InteropServices;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test conversion to NATIVE_INDEXCREATE1
    /// </summary>
    [TestClass]
    public class Indexcreate1Tests
    {
        /// <summary>
        /// Managed version of the indexcreate structure.
        /// </summary>
        private JET_INDEXCREATE managed;

        /// <summary>
        /// The native conditional column structure created from the JET_INDEXCREATE
        /// object.
        /// </summary>
        private NATIVE_INDEXCREATE1 native;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Initialize the Indexcreate1Tests fixture")]
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
                cbKeyMost = 500,
            };
            this.native = this.managed.GetNativeIndexcreate1();
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the structure size
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets the structure size")]
        public void VerifyConversionToNativeSetsCbStruct()
        {
            Assert.AreEqual((uint)Marshal.SizeOf(this.native), this.native.indexcreate.cbStruct);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the name
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets the index name")]
        public void VerifyConversionToNativeSetsName()
        {
            // Done at pinvoke time.
            Assert.AreEqual(IntPtr.Zero, this.native.indexcreate.szIndexName);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the key
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets the key")]
        public void VerifyConversionToNativeSetsKey()
        {
            // Done at pinvoke time.
            Assert.AreEqual(IntPtr.Zero, this.native.indexcreate.szKey);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the key length
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets the key length")]
        public void VerifyConversionToNativeSetsKeyLength()
        {
            Assert.AreEqual((uint)8, this.native.indexcreate.cbKey);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the grbit
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets the grbit")]
        public void VerifyConversionToNativeSetsGrbit()
        {
            Assert.IsTrue(0 != ((uint)CreateIndexGrbit.IndexSortNullsHigh & this.native.indexcreate.grbit));
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the density
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets the density")]
        public void VerifyConversionToNativeSetsDensity()
        {
            Assert.AreEqual((uint)100, this.native.indexcreate.ulDensity);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the JET_UNICODEINDEX
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets the Unicode index")]
        public unsafe void VerifyConversionToNativeSetsUnicodeIndexToNull()
        {
            Assert.IsTrue(null == this.native.indexcreate.pidxUnicode);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the cbVarSegMac
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets the cbVarSegMac")]
        public void VerifyConversionToNativeSetsCbVarSegMac()
        {
            Assert.AreEqual(new IntPtr(200), this.native.indexcreate.cbVarSegMac);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the JET_CONDITIONALCOLUMNs
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets the conditional columns")]
        public void VerifyConversionToNativeSetsConditionalColumnsToNull()
        {
            Assert.AreEqual(IntPtr.Zero, this.native.indexcreate.rgconditionalcolumn);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the cConditionalColumn
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets cConditionalColumn")]
        public void VerifyConversionToNativeSetsCConditionalColumn()
        {
            Assert.AreEqual((uint)0, this.native.indexcreate.cConditionalColumn);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the cbKeyMost
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets cbKeyMost")]
        public void VerifyConversionToNativeSetsCbKeyMost()
        {
            Assert.AreEqual((uint)500, this.native.cbKeyMost);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXCREATE1 sets the cbKeyMost grbit
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_INDEXCREATE to a NATIVE_INDEXCREATE1 sets the cbKeyMost grbit")]
        public void VerifyConversionToNativeSetsKeyMostGrbitWhenKeyMostIsSet()
        {
            Assert.IsTrue(0 != ((uint)VistaGrbits.IndexKeyMost & this.native.indexcreate.grbit));
        }
    }
}