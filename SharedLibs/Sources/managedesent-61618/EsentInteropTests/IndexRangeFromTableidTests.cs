//-----------------------------------------------------------------------
// <copyright file="IndexRangeFromTableidTests.cs" company="Microsoft Corporation">
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
    /// Tests for the NATIVE_INDEXRANGE class
    /// </summary>
    [TestClass]
    public class IndexRangeFromTableidTests
    {
        /// <summary>
        /// The tableid to be converted.
        /// </summary>
        private JET_TABLEID tableid;

        /// <summary>
        /// The native index list that will be created from the tableid.
        /// </summary>
        private NATIVE_INDEXRANGE native;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the IndexRangeFromTableidTests fixture")]
        public void Setup()
        {
            this.tableid = new JET_TABLEID { Value = new IntPtr(0x55) };
            this.native = NATIVE_INDEXRANGE.MakeIndexRangeFromTableid(this.tableid);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXRANGE
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that conversion from a JET_TABLEID to a NATIVE_INDEXRANGE sets the cbStruct")]
        public void VerifyMakeIndexRangeFromTableidSetsCbstruct()
        {
            Assert.AreEqual((uint)Marshal.SizeOf(this.native), this.native.cbStruct);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXRANGE
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that conversion from a JET_TABLEID to a NATIVE_INDEXRANGE sets the tableid")]
        public void VerifyMakeIndexRangeFromTableidSetsTableid()
        {
            Assert.AreEqual(this.tableid.Value, this.native.tableid);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXRANGE
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that conversion from a JET_TABLEID to a NATIVE_INDEXRANGE sets the grbit")]
        public void VerifyMakeIndexRangeFromTableidSetsGrbit()
        {
            Assert.AreEqual((uint)IndexRangeGrbit.RecordInIndex, this.native.grbit);
        }
    }
}