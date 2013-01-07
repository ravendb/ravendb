//-----------------------------------------------------------------------
// <copyright file="RecsizeConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test setting a JET_RECSIZE from a NATIVE_RECSIZE.
    /// </summary>
    [TestClass]
    public class RecsizeConversationTests
    {
        /// <summary>
        /// Native version of the recsize.
        /// </summary>
        private NATIVE_RECSIZE native;

        /// <summary>
        /// Managed version of the recsize, created from the native.
        /// </summary>
        private JET_RECSIZE managed;

        /// <summary>
        /// Initialize the native and managed objects.
        /// </summary>
        [TestInitialize]
        [Description("Setup the RecsizeConversionTests fixture")]
        public void Setup()
        {
            this.native = new NATIVE_RECSIZE
            {
                cbData = 1,
                cbLongValueData = 2,
                cbLongValueOverhead = 3,
                cbOverhead = 4,
                cLongValues = 5,
                cMultiValues = 6,
                cNonTaggedColumns = 7,
                cTaggedColumns = 8,                
            };
            this.managed = new JET_RECSIZE();
            this.managed.SetFromNativeRecsize(this.native);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cbData")]
        public void TestSetFromNativeSetsCbData()
        {
            Assert.AreEqual(1, this.managed.cbData);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets a default value for cbDataCompressed")]
        public void TestSetFromNativeSetsDefaultCbDataCompressed()
        {
            Assert.AreEqual(this.managed.cbData, this.managed.cbDataCompressed);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cbLongValueData")]
        public void TestSetFromNativeSetsCbLongValueData()
        {
            Assert.AreEqual(2, this.managed.cbLongValueData);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets a default value for cbLongValueDataCompressed")]
        public void TestSetFromNativeSetsDefaultCbLongValueDataCompressed()
        {
            Assert.AreEqual(this.managed.cbLongValueData, this.managed.cbLongValueDataCompressed);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cbLongValueOverhead")]
        public void TestSetFromNativeSetsCbLongValueOverhead()
        {
            Assert.AreEqual(3, this.managed.cbLongValueOverhead);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cbOverhead")]
        public void TestSetFromNativeSetsCbOverhead()
        {
            Assert.AreEqual(4, this.managed.cbOverhead);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets a default value for cCompressedColumns")]
        public void TestSetFromNativeSetsDefaultCCompressedColumns()
        {
            Assert.AreEqual(0, this.managed.cCompressedColumns);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cLongValues")]
        public void TestSetFromNativeSetsCLongValues()
        {
            Assert.AreEqual(5, this.managed.cLongValues);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cMultiValues")]
        public void TestSetFromNativeSetsCMultiValues()
        {
            Assert.AreEqual(6, this.managed.cMultiValues);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cNonTaggedColumns")]
        public void TestSetFromNativeSetsCNonTaggedColumns()
        {
            Assert.AreEqual(7, this.managed.cNonTaggedColumns);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cTaggedColumns")]
        public void TestSetFromNativeSetsCTaggedColumns()
        {
            Assert.AreEqual(8, this.managed.cTaggedColumns);
        }
    }
}