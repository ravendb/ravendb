//-----------------------------------------------------------------------
// <copyright file="ColumnCreateConversionTests.cs" company="Microsoft Corporation">
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
    /// Test conversion to NATIVE_COLUMNCREATE
    /// </summary>
    [TestClass]
    public class ColumnCreateConversationTests
    {
        /// <summary>
        /// Managed version of the indexcreate structure.
        /// </summary>
        private JET_COLUMNCREATE managed;

        /// <summary>
        /// The native conditional column structure created from the JET_COLUMNCREATE
        /// object.
        /// </summary>
        private NATIVE_COLUMNCREATE native;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Initialize the ColumnCreateTests fixture")]
        public void Setup()
        {
            this.managed = new JET_COLUMNCREATE()
            {
                szColumnName = "column9",
                coltyp = JET_coltyp.Binary,
                cbMax = 0x42,
                grbit = ColumndefGrbit.ColumnAutoincrement,
                pvDefault = null,
                cbDefault = 0,
                cp = JET_CP.Unicode,
                columnid = new JET_COLUMNID { Value = 7 },
                err = JET_err.RecoveredWithoutUndo,
            };
            this.native = this.managed.GetNativeColumnCreate();
        }

        /// <summary>
        /// Test conversion from JET_COLUMNCREATE to NATIVE_COLUMNCREATE sets szColumnName.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets szColumnName.")]
        public void VerifyConversionToNativeSetsSzColumnName()
        {
            // The current model is to do the string conversion at pinvoke time.
            Assert.AreEqual(IntPtr.Zero, this.native.szColumnName);
        }

        /// <summary>
        /// Test conversion from JET_COLUMNCREATE to NATIVE_COLUMNCREATE sets coltyp.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets coltyp.")]
        public void VerifyConversionToNativeSetsColtyp()
        {
            Assert.AreEqual<uint>(9, this.native.coltyp);
        }

        /// <summary>
        /// Test conversion from JET_COLUMNCREATE to NATIVE_COLUMNCREATE sets cbMax
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets cbMax.")]
        public void VerifyConversionToNativeSetscbMax()
        {
            Assert.AreEqual<uint>(0x42, this.native.cbMax);
        }

        /// <summary>
        /// Test conversion from JET_COLUMNCREATE to NATIVE_COLUMNCREATE sets grbit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets grbit.")]
        public void VerifyConversionToNativeSetsGrbit()
        {
            Assert.AreEqual<uint>(0x10, this.native.grbit);
        }

        /// <summary>
        /// Test conversion from JET_COLUMNCREATE to NATIVE_COLUMNCREATE sets pvDefault.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets pvDefault.")]
        public void VerifyConversionToNativeSetsPvDefault()
        {
            Assert.AreEqual(IntPtr.Zero, this.native.pvDefault);
        }

        /// <summary>
        /// Test conversion from JET_COLUMNCREATE to NATIVE_COLUMNCREATE sets cbDefault.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets cbDefault.")]
        public void VerifyConversionToNativeSetsCbDefault()
        {
            Assert.AreEqual<uint>(0, this.native.cbDefault);
        }

        /// <summary>
        /// Test conversion from JET_COLUMNCREATE to NATIVE_COLUMNCREATE sets cp.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets cp.")]
        public void VerifyConversionToNativeSetsCp()
        {
            Assert.AreEqual<uint>(1200, this.native.cp);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_COLUMNCREATE sets the structure size
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_COLUMNCREATE to a NATIVE_COLUMNCREATE sets the structure size")]
        public void VerifyConversionToNativeSetsCbStruct()
        {
            Assert.AreEqual((uint)Marshal.SizeOf(this.native), this.native.cbStruct);
        }

        /// <summary>
        /// Verifies that the ToString() conversion is correct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verifies that the ToString() conversion is correct.")]
        public void VerifyToString()
        {
            Assert.AreEqual("JET_COLUMNCREATE(column9,Binary,ColumnAutoincrement)", this.managed.ToString());
        }
    }
}