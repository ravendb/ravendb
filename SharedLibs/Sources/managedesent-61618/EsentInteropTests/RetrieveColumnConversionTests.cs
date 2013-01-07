//-----------------------------------------------------------------------
// <copyright file="RetrieveColumnConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test conversion to NATIVE_RETRIEVECOLUMN
    /// </summary>
    [TestClass]
    public class RetrieveColumnConversionTests
    {
        /// <summary>
        /// The managed version of the struct.
        /// </summary>
        private JET_RETRIEVECOLUMN managed;

        /// <summary>
        /// The native structure created from the managed version.
        /// </summary>
        private NATIVE_RETRIEVECOLUMN native;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the RetrieveColumnConversionTests test fixture")]
        public void Setup()
        {
            this.managed = new JET_RETRIEVECOLUMN
            {
                cbData = 1,
                columnid = new JET_COLUMNID { Value = 2 },
                grbit = RetrieveColumnGrbit.RetrieveCopy,
                ibLongValue = 3,
                itagSequence = 4,
            };
            this.managed.GetNativeRetrievecolumn(ref this.native);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the cbData
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from JET_RETRIEVECOLUMN to NATIVE_RETRIEVECOLUMN sets cbData")]
        public void VerifyConversionToNativeSetsCbData()
        {
            Assert.AreEqual((uint)1, this.native.cbData);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the columnid
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from JET_RETRIEVECOLUMN to NATIVE_RETRIEVECOLUMN sets columnid")]
        public void VerifyConversionToNativeSetsColumnid()
        {
            Assert.AreEqual((uint)2, this.native.columnid);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the grbit
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from JET_RETRIEVECOLUMN to NATIVE_RETRIEVECOLUMN sets grbit")]
        public void VerifyConversionToNativeSetsGrbit()
        {
            Assert.AreEqual((uint)RetrieveColumnGrbit.RetrieveCopy, this.native.grbit);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the ibLongValue
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from JET_RETRIEVECOLUMN to NATIVE_RETRIEVECOLUMN sets ibLongValue")]
        public void VerifyConversionToNativeSetsIbLongValue()
        {
            Assert.AreEqual((uint)3, this.native.ibLongValue);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the itagSequence
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from JET_RETRIEVECOLUMN to NATIVE_RETRIEVECOLUMN sets itagSequence")]
        public void VerifyConversionToNativeSetsItagSequence()
        {
            Assert.AreEqual((uint)4, this.native.itagSequence);
        }

        /// <summary>
        /// Check the conversion to a native structure sets the pvData
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that conversion from JET_RETRIEVECOLUMN to NATIVE_RETRIEVECOLUMN sets pvData")]
        public void VerifyConversionToNativeDoesNotSetPvData()
        {
            Assert.AreEqual(IntPtr.Zero, this.native.pvData);
        }
    }
}