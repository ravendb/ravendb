//-----------------------------------------------------------------------
// <copyright file="SetColumnConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test conversion to NATIVE_SETCOLUMN
    /// </summary>
    [TestClass]
    public class SetColumnConversionTests
    {
        /// <summary>
        /// The managed version of the struct.
        /// </summary>
        private JET_SETCOLUMN managed;

        /// <summary>
        /// The native structure created from the managed version.
        /// </summary>
        private NATIVE_SETCOLUMN native;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the SetColumnConversionTests test fixture")]
        public void Setup()
        {
            this.managed = new JET_SETCOLUMN
            {
                cbData = 1,
                columnid = new JET_COLUMNID { Value = 2 },
                grbit = SetColumnGrbit.AppendLV,
                ibLongValue = 3,
                itagSequence = 4,
            };
            this.native = this.managed.GetNativeSetcolumn();
        }

        /// <summary>
        /// Check the conversion to a native structure sets the cbData
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversation from JET_SETCOLUMN to NATIVE_SETCOLUMN sets cbData")]
        public void VerifyConversionToNativeSetsCbData()
        {
            Assert.AreEqual((uint)1, this.native.cbData);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the columnid
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversation from JET_SETCOLUMN to NATIVE_SETCOLUMN sets columnid")]
        public void VerifyConversionToNativeSetsColumnid()
        {
            Assert.AreEqual((uint)2, this.native.columnid);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the grbit
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversation from JET_SETCOLUMN to NATIVE_SETCOLUMN sets grbit")]
        public void VerifyConversionToNativeSetsGrbit()
        {
            Assert.AreEqual((uint)SetColumnGrbit.AppendLV, this.native.grbit);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the ibLongValue
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversation from JET_SETCOLUMN to NATIVE_SETCOLUMN sets ibLongValue")]
        public void VerifyConversionToNativeSetsIbLongValue()
        {
            Assert.AreEqual((uint)3, this.native.ibLongValue);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the itagSequence
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversation from JET_SETCOLUMN to NATIVE_SETCOLUMN sets itagSequence")]
        public void VerifyConversionToNativeSetsItagSequence()
        {
            Assert.AreEqual((uint)4, this.native.itagSequence);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the pvData
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversation from JET_SETCOLUMN to NATIVE_SETCOLUMN sets pvData")]
        public void VerifyConversionToNativeDoesNotSetPvData()
        {
            Assert.AreEqual(IntPtr.Zero, this.native.pvData);
        }
    }
}