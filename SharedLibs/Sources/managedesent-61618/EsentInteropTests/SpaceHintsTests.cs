//-----------------------------------------------------------------------
// <copyright file="SpaceHintsTests.cs" company="Microsoft Corporation">
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
    /// Test conversion to/from NATIVE_SPACEHINTS.
    /// </summary>
    [TestClass]
    public class SpaceHintsTests
    {
        /// <summary>
        /// Managed version of the indexcreate structure.
        /// </summary>
        private JET_SPACEHINTS managedSource;

        /// <summary>
        /// The native conditional column structure created from the JET_SPACEHINTS
        /// object.
        /// </summary>
        private NATIVE_SPACEHINTS nativeTarget;

        /// <summary>
        /// Managed version of the indexcreate structure.
        /// </summary>
        private JET_SPACEHINTS managedTarget;

        /// <summary>
        /// The native conditional column structure created from the JET_SPACEHINTS
        /// object.
        /// </summary>
        private NATIVE_SPACEHINTS nativeSource;

        /// <summary>
        /// Setup the test fixture. This creates a native structure and converts
        /// it to a managed object.
        /// </summary>
        [TestInitialize]
        [Description("Initialize the SpaceHintsTests fixture")]
        public void Setup()
        {
            this.managedSource = new JET_SPACEHINTS()
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = SpaceHintsGrbit.CreateHintAppendSequential | SpaceHintsGrbit.RetrieveHintTableScanForward,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };
            this.nativeTarget = this.managedSource.GetNativeSpaceHints();

            this.nativeSource = new NATIVE_SPACEHINTS()
            {
                ulInitialDensity = 33,
                cbInitial = 4096,
                grbit = 0x12,
                ulMaintDensity = 44,
                ulGrowth = 144,
                cbMinExtent = 1024 * 1024,
                cbMaxExtent = 3 * 1024 * 1024,
            };

            this.managedTarget = new JET_SPACEHINTS();
            this.managedTarget.SetFromNativeSpaceHints(this.nativeSource);
        }

        /// <summary>
        /// Verifies that the ToString() conversion is correct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verifies that the ToString() conversion is correct.")]
        public void VerifyToString()
        {
            Assert.AreEqual<string>("JET_SPACEHINTS(CreateHintAppendSequential, RetrieveHintTableScanForward)", this.managedTarget.ToString());
        }

        #region Managed-to-native conversion.

        /// <summary>
        /// Test conversion from JET_SPACEHINTS to NATIVE_SPACEHINTS sets ulInitialDensity.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets ulInitialDensity.")]
        public void VerifyConversionToNativeSetUlInitialDensity()
        {
            Assert.AreEqual<uint>(33, this.nativeTarget.ulInitialDensity);
        }

        /// <summary>
        /// Test conversion from JET_SPACEHINTS to NATIVE_SPACEHINTS sets cbInitial.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets cbInitial.")]
        public void VerifyConversionToNativeSetsCbInitial()
        {
            Assert.AreEqual<uint>(4096, this.nativeTarget.cbInitial);
        }

        /// <summary>
        /// Test conversion from JET_SPACEHINTS to NATIVE_SPACEHINTS sets grbit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets grbit.")]
        public void VerifyConversionToNativeSetsGrbit()
        {
            Assert.AreEqual<uint>(0x12, this.nativeTarget.grbit);
        }

        /// <summary>
        /// Test conversion from JET_SPACEHINTS to NATIVE_SPACEHINTS sets ulMaintDensity.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets ulMaintDensity.")]
        public void VerifyConversionToNativeSetsUlMaintDensity()
        {
            Assert.AreEqual<uint>(44, this.nativeTarget.ulMaintDensity);
        }

        /// <summary>
        /// Test conversion from JET_SPACEHINTS to NATIVE_SPACEHINTS sets ulGrowth.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets ulGrowth.")]
        public void VerifyConversionToNativeSetsUlGrowth()
        {
            Assert.AreEqual<uint>(144, this.nativeTarget.ulGrowth);
        }

        /// <summary>
        /// Test conversion from JET_SPACEHINTS to NATIVE_SPACEHINTS sets cbMinExtent.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets cbMinExtent.")]
        public void VerifyConversionToNativeSetsCbMinExtent()
        {
            Assert.AreEqual<uint>(1024 * 1024, this.nativeTarget.cbMinExtent);
        }

        /// <summary>
        /// Test conversion from JET_SPACEHINTS to NATIVE_SPACEHINTS sets cbMaxExtent.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from JET_COLUMNDEF to NATIVE_COLUMNDEF sets cbMaxExtent.")]
        public void VerifyConversionToNativeSetsCbMaxExtent()
        {
            Assert.AreEqual<uint>(3 * 1024 * 1024, this.nativeTarget.cbMaxExtent);
        }
#endregion

        /// <summary>
        /// Check the conversion to a NATIVE_SPACEHINTS sets the structure size
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check the conversion from JET_SPACEHINTS to a NATIVE_SPACEHINTS sets the structure size")]
        public void VerifyConversionToNativeSetsCbStruct()
        {
            Assert.AreEqual((uint)Marshal.SizeOf(this.nativeTarget), this.nativeTarget.cbStruct);
        }

        #region Managed-to-native conversion

        /// <summary>
        /// Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets ulInitialDensity.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets ulInitialDensity.")]
        public void VerifyConversionFromNativeSetsUlInitialDensity()
        {
            Assert.AreEqual<uint>(33, checked((uint)this.managedTarget.ulInitialDensity));
        }

        /// <summary>
        /// Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets cbInitial.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets cbInitial.")]
        public void VerifyConversionFromNativeSetsCbInitial()
        {
            Assert.AreEqual<uint>(4096, checked((uint)this.managedTarget.cbInitial));
        }

        /// <summary>
        /// Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets grbit.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets grbit.")]
        public void VerifyConversionFromNativeSetsGrbit()
        {
            Assert.AreEqual<uint>(0x12, checked((uint)this.managedTarget.grbit));
        }

        /// <summary>
        /// Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets ulMaintDensity.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets ulMaintDensity.")]
        public void VerifyConversionFromNativeSetsUlMaintDensity()
        {
            Assert.AreEqual<uint>(044, checked((uint)this.managedTarget.ulMaintDensity));
        }

        /// <summary>
        /// Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets ulGrowth.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets ulGrowth.")]
        public void VerifyConversionFromNativeSetsUlGrowth()
        {
            Assert.AreEqual<uint>(144, checked((uint)this.managedTarget.ulGrowth));
        }

        /// <summary>
        /// Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets cbMinExtent.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets cbMinExtent.")]
        public void VerifyConversionFromNativeSetsCbMinExtent()
        {
            Assert.AreEqual<uint>(1024 * 1024, checked((uint)this.managedTarget.cbMinExtent));
        }

        /// <summary>
        /// Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets cbMaxExtent.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test conversion from NATIVE_COLUMNCREATE to JET_COLUMNCREATE sets cbMaxExtent.")]
        public void VerifyConversionFromNativeSetsCbMaxExtent()
        {
            Assert.AreEqual<uint>(3 * 1024 * 1024, checked((uint)this.managedTarget.cbMaxExtent));
        }

        #endregion
    }
}