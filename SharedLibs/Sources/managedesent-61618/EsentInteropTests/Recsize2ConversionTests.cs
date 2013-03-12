//-----------------------------------------------------------------------
// <copyright file="Recsize2ConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test setting a JET_RECSIZE from a NATIVE_RECSIZE2.
    /// </summary>
    [TestClass]
    public class Recsize2ConversionTests
    {
        /// <summary>
        /// Native version of the recsize.
        /// </summary>
        private NATIVE_RECSIZE2 native;

        /// <summary>
        /// Managed version of the recsize, created from the native.
        /// </summary>
        private JET_RECSIZE managed;

        /// <summary>
        /// Initialize the native and managed objects.
        /// </summary>
        [TestInitialize]
        [Description("Setup the Recsize2ConversionTests fixture")]
        public void Setup()
        {
            this.native = new NATIVE_RECSIZE2
            {
                cbData = 1,
                cbDataCompressed = 2,
                cbLongValueData = 3,
                cbLongValueDataCompressed = 4,
                cbLongValueOverhead = 5,
                cbOverhead = 6,
                cCompressedColumns = 7,
                cLongValues = 8,
                cMultiValues = 9,
                cNonTaggedColumns = 10,
                cTaggedColumns = 11,
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
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cbDataCompressed")]
        public void TestSetFromNativeSetsCbDataCompressed()
        {
            Assert.AreEqual(2, this.managed.cbDataCompressed);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cbLongValueData")]
        public void TestSetFromNativeSetsCbLongValueData()
        {
            Assert.AreEqual(3, this.managed.cbLongValueData);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cbLongValueDataCompressed")]
        public void TestSetFromNativeSetsCbLongValueDataCompressed()
        {
            Assert.AreEqual(4, this.managed.cbLongValueDataCompressed);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cbLongValueOverhead")]
        public void TestSetFromNativeSetsCbLongValueOverhead()
        {
            Assert.AreEqual(5, this.managed.cbLongValueOverhead);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cbOverhead")]
        public void TestSetFromNativeSetsCbOverhead()
        {
            Assert.AreEqual(6, this.managed.cbOverhead);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cCompressedColumns")]
        public void TestSetFromNativeSetsCCompressedColumns()
        {
            Assert.AreEqual(7, this.managed.cCompressedColumns);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cLongValues")]
        public void TestSetFromNativeSetsCLongValues()
        {
            Assert.AreEqual(8, this.managed.cLongValues);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cMultiValues")]
        public void TestSetFromNativeSetsCMultiValues()
        {
            Assert.AreEqual(9, this.managed.cMultiValues);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cNonTaggedColumns")]
        public void TestSetFromNativeSetsCNonTaggedColumns()
        {
            Assert.AreEqual(10, this.managed.cNonTaggedColumns);
        }

        /// <summary>
        /// Test conversion from the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.SetFromNativeRecsize sets cTaggedColumns")]
        public void TestSetFromNativeSetsCTaggedColumns()
        {
            Assert.AreEqual(11, this.managed.cTaggedColumns);
        }
    }
}