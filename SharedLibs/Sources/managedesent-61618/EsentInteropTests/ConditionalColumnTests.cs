//-----------------------------------------------------------------------
// <copyright file="ConditionalColumnTests.cs" company="Microsoft Corporation">
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
    /// Test conversion to NATIVE_COLUMNDEF
    /// </summary>
    [TestClass]
    public class ConditionalColumnTests
    {
        /// <summary>
        /// Managed version of the conditional column.
        /// </summary>
        private JET_CONDITIONALCOLUMN managed;

        /// <summary>
        /// The native conditional column structure created from the JET_CONDITIONALCOLUMN
        /// object.
        /// </summary>
        private NATIVE_CONDITIONALCOLUMN native;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the ConditionalColumnTests fixture")]
        public void Setup()
        {
            this.managed = new JET_CONDITIONALCOLUMN
            {
                szColumnName = "column",
                grbit = ConditionalColumnGrbit.ColumnMustBeNonNull,
            };
            this.native = this.managed.GetNativeConditionalColumn();
        }

        /// <summary>
        /// Check the conversion to a NATIVE_CONDITIONALCOLUMN sets the structure size
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion to a NATIVE_CONDITIONALCOLUMN sets the structure size")]
        public void VerifyConversionToNativeSetsCbStruct()
        {
            Assert.AreEqual((uint)Marshal.SizeOf(this.native), this.native.cbStruct);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_CONDITIONALCOLUMN sets the column name
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion to a NATIVE_CONDITIONALCOLUMN sets the column name")]
        public void VerifyConversionToNativeSetsColumnName()
        {
            Assert.AreEqual(IntPtr.Zero, this.native.szColumnName);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_CONDITIONALCOLUMN sets the grbit
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion to a NATIVE_CONDITIONALCOLUMN sets the grbit")]
        public void VerifyConversionToNativeSetsGrbit()
        {
            Assert.AreEqual((uint)ConditionalColumnGrbit.ColumnMustBeNonNull, this.native.grbit);
        }
    }
}