//-----------------------------------------------------------------------
// <copyright file="NativeRecsize2ConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test creating a NATIVE_RECSIZE2 from a JET_RECSIZE.
    /// </summary>
    [TestClass]
    public class NativeRecsize2ConversionTests
    {
        /// <summary>
        /// Native version of the recsize, created from the managed.
        /// </summary>
        private NATIVE_RECSIZE2 native;

        /// <summary>
        /// Managed version of the recsize.
        /// </summary>
        private JET_RECSIZE managed;

        /// <summary>
        /// Initialize the native and managed objects.
        /// </summary>
        [TestInitialize]
        [Description("Setup the Recsize2ConversionTests fixture")]
        public void Setup()
        {
            this.managed = new JET_RECSIZE
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
            this.native = this.managed.GetNativeRecsize2();
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cbData")]
        public void TestGetNative2SetsCbData()
        {
            Assert.AreEqual(1U, this.native.cbData);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cbDataCompressed")]
        public void TestGetNative2SetsCbDataCompressed()
        {
            Assert.AreEqual(2U, this.native.cbDataCompressed);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cbLongValueData")]
        public void TestGetNative2SetsCbLongValueData()
        {
            Assert.AreEqual(3U, this.native.cbLongValueData);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cbLongValueDataCompressed")]
        public void TestGetNative2SetsCbLongValueDataCompressed()
        {
            Assert.AreEqual(4U, this.native.cbLongValueDataCompressed);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cbLongValueOverhead")]
        public void TestGetNative2SetsCbLongValueOverhead()
        {
            Assert.AreEqual(5U, this.native.cbLongValueOverhead);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cbOverhead")]
        public void TestGetNative2SetsCbOverhead()
        {
            Assert.AreEqual(6U, this.native.cbOverhead);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cCompressedColumns")]
        public void TestGetNative2SetsCCompressedColumns()
        {
            Assert.AreEqual(7U, this.native.cCompressedColumns);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cLongValues")]
        public void TestGetNative2SetsCLongValues()
        {
            Assert.AreEqual(8U, this.native.cLongValues);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cMultiValues")]
        public void TestGetNative2SetsCMultiValues()
        {
            Assert.AreEqual(9U, this.native.cMultiValues);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cNonTaggedColumns")]
        public void TestGetNative2SetsCNonTaggedColumns()
        {
            Assert.AreEqual(10U, this.native.cNonTaggedColumns);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize2 sets cTaggedColumns")]
        public void TestGetNative2SetsCTaggedColumns()
        {
            Assert.AreEqual(11U, this.native.cTaggedColumns);
        }
    }
}