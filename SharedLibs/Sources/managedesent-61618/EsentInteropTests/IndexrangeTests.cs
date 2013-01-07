//-----------------------------------------------------------------------
// <copyright file="IndexRangeTests.cs" company="Microsoft Corporation">
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
    /// Tests for the JET_INDEXRANGE class
    /// </summary>
    [TestClass]
    public class IndexRangeTests
    {
        /// <summary>
        /// The managed indexrange that will be converted to a native
        /// structure.
        /// </summary>
        private JET_INDEXRANGE managed;

        /// <summary>
        /// The native index list made from the JET_INDEXRANGE.
        /// </summary>
        private NATIVE_INDEXRANGE native;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the IndexRangeTests fixture")]
        public void Setup()
        {
            this.managed = new JET_INDEXRANGE
            {
                tableid = new JET_TABLEID { Value = (IntPtr)0x1234 },
                grbit = IndexRangeGrbit.RecordInIndex,
            };
            this.native = this.managed.GetNativeIndexRange();
        }

        /// <summary>
        /// Make sure the JET_INDEXRANGE constructor sets the grbit to
        /// a valid value (there is only one valid grbit, but it is
        /// non-zero).
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify JET_INDEXRANGE constructor sets grbit")]
        public void VerifyIndexRangeConstructorSetsGrbit()
        {
            var indexrange = new JET_INDEXRANGE();
            Assert.AreEqual(IndexRangeGrbit.RecordInIndex, indexrange.grbit);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXRANGE
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting JET_INDEXRANGE to a NATIVE_INDEXRANGE sets cbStruct")]
        public void VerifyMakeIndexRangeFromTableidSetsCbstruct()
        {
            Assert.AreEqual((uint)Marshal.SizeOf(this.native), this.native.cbStruct);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXRANGE
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting JET_INDEXRANGE to a NATIVE_INDEXRANGE sets the tableid")]
        public void VerifyMakeIndexRangeFromTableidSetsTableid()
        {
            Assert.AreEqual(this.managed.tableid.Value, this.native.tableid);
        }

        /// <summary>
        /// Check the conversion to a NATIVE_INDEXRANGE
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify converting JET_INDEXRANGE to a NATIVE_INDEXRANGE sets the grbit")]
        public void VerifyMakeIndexRangeFromTableidSetsGrbit()
        {
            Assert.AreEqual((uint)this.managed.grbit, this.native.grbit);
        }
    }
}