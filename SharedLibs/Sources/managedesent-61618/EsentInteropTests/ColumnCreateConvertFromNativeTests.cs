//-----------------------------------------------------------------------
// <copyright file="ColumnCreateConvertFromNativeTests.cs" company="Microsoft Corporation">
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
    /// Test conversion from NATIVE_COLUMNCREATE
    /// </summary>
    [TestClass]
    public class ColumnCreateConvertFromNativeTests
    {
        /// <summary>
        /// The native conditional column structure set from the JET_COLUMNCREATE
        /// object.
        /// </summary>
        private NATIVE_COLUMNCREATE nativeSource;

        /// <summary>
        /// Managed version of the indexcreate structure.
        /// </summary>
        private JET_COLUMNCREATE managedTarget;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Initialize the ColumnCreateTests fixture")]
        public void Setup()
        {
            this.nativeSource = new NATIVE_COLUMNCREATE()
            {
                szColumnName = Marshal.StringToHGlobalAnsi("column9"),
                coltyp = (uint)JET_coltyp.Binary,
                cbMax = 0x42,
                grbit = (uint)ColumndefGrbit.ColumnAutoincrement,
                pvDefault = IntPtr.Zero,
                cbDefault = 0,
                cp = (uint)JET_CP.Unicode,
                columnid = 7,
                err = (int)JET_err.RecoveredWithoutUndo,
            };

            this.managedTarget = new JET_COLUMNCREATE();
            this.managedTarget.SetFromNativeColumnCreate(this.nativeSource);
        }

        /// <summary>
        /// Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets columnid.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets columnid.")]
        public void VerifyConversionFromNativeSetsColumnId()
        {
            Assert.AreEqual<uint>(7, this.managedTarget.columnid.Value);
        }

        /// <summary>
        /// Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets err.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets err.")]
        public void VerifyConversionFromNativeSetsErr()
        {
            Assert.AreEqual<int>(-579, (int)this.managedTarget.err);
        }
    }
}