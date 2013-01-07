//-----------------------------------------------------------------------
// <copyright file="NativeRecsizeConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test getting a NATIVE_RECSIZE from a JET_RECSIZE.
    /// </summary>
    [TestClass]
    public class NativeRecsizeConversationTests
    {
        /// <summary>
        /// Native version of the recsize, created from the managed.
        /// </summary>
        private NATIVE_RECSIZE native;

        /// <summary>
        /// Managed version of the recsize.
        /// </summary>
        private JET_RECSIZE managed;

        #region Setup/Teardown

        /// <summary>
        /// Initialize the native and managed objects.
        /// </summary>
        [TestInitialize]
        [Description("Setup the NativeRecsizeConversionTests fixture")]
        public void Setup()
        {
            this.managed = new JET_RECSIZE
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
            this.native = this.managed.GetNativeRecsize();
        }

        /// <summary>
        /// Verifies no instances are leaked.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            SetupHelper.CheckProcessForInstanceLeaks();
        }

        #endregion

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize sets cbData")]
        public void TestGetNativeSetsCbData()
        {
            Assert.AreEqual(1U, this.native.cbData);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize sets cbLongValueData")]
        public void TestGetNativeSetsCbLongValueData()
        {
            Assert.AreEqual(2U, this.native.cbLongValueData);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize sets cbLongValueOverhead")]
        public void TestGetNativeSetsCbLongValueOverhead()
        {
            Assert.AreEqual(3U, this.native.cbLongValueOverhead);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize sets cbOverhead")]
        public void TestGetNativeSetsCbOverhead()
        {
            Assert.AreEqual(4U, this.native.cbOverhead);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize sets cLongValues")]
        public void TestGetNativeSetsCLongValues()
        {
            Assert.AreEqual(5U, this.native.cLongValues);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize sets cMultiValues")]
        public void TestGetNativeSetsCMultiValues()
        {
            Assert.AreEqual(6U, this.native.cMultiValues);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize sets cNonTaggedColumns")]
        public void TestGetNativeSetsCNonTaggedColumns()
        {
            Assert.AreEqual(7U, this.native.cNonTaggedColumns);
        }

        /// <summary>
        /// Test conversion to the native struct.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that JET_RECSIZE.GetNativeRecsize sets cTaggedColumns")]
        public void TestGetNativeSetsCTaggedColumns()
        {
            Assert.AreEqual(8U, this.native.cTaggedColumns);
        }
    }
}