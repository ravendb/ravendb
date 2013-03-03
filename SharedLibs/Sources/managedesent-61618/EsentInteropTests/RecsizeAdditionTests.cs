//-----------------------------------------------------------------------
// <copyright file="RecsizeAdditionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test adding two JET_RECSIZE structures.
    /// </summary>
    [TestClass]
    public class RecsizeAdditionTests
    {
        /// <summary>
        /// Result of the addition using operator overloading.
        /// </summary>
        private JET_RECSIZE result;

        /// <summary>
        /// Result of the addition using the named operator.
        /// </summary>
        private JET_RECSIZE namedResult;

        /// <summary>
        /// Setup the fixture by adding two JET_RECSIZE objects.
        /// </summary>
        [TestInitialize]
        [Description("Setup the RecsizeAdditionTests fixture")]
        public void Setup()
        {
            var s1 = new JET_RECSIZE
            {
                cbData = 0x10,
                cbDataCompressed = 0x20,
                cbLongValueData = 0x30,
                cbLongValueDataCompressed = 0x40,
                cbLongValueOverhead = 0x50,
                cbOverhead = 0x60,
                cCompressedColumns = 0x70,
                cLongValues = 0x80,
                cMultiValues = 0x90,
                cNonTaggedColumns = 0xa0,
                cTaggedColumns = 0xb0,
            };

            var s2 = new JET_RECSIZE
            {
                cbData = 0x1,
                cbDataCompressed = 0x2,
                cbLongValueData = 0x3,
                cbLongValueDataCompressed = 0x4,
                cbLongValueOverhead = 0x5,
                cbOverhead = 0x6,
                cCompressedColumns = 0x7,
                cLongValues = 0x8,
                cMultiValues = 0x9,
                cNonTaggedColumns = 0xa,
                cTaggedColumns = 0xb,
            };

            this.result = s1 + s2;
            this.namedResult = JET_RECSIZE.Add(s1, s2);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cbData")]
        public void TestJetRecsizeAdditionSetsCbData()
        {
            Assert.AreEqual(0x11, this.result.cbData);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cbDataCompressed")]
        public void TestJetRecsizeAdditionSetsCbDataCompressed()
        {
            Assert.AreEqual(0x22, this.result.cbDataCompressed);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cbLongValueData")]
        public void TestJetRecsizeAdditionSetsCbLongValueData()
        {
            Assert.AreEqual(0x33, this.result.cbLongValueData);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cbLongValueDataCompressed")]
        public void TestJetRecsizeAdditionSetsCbLongValueDataCompressed()
        {
            Assert.AreEqual(0x44, this.result.cbLongValueDataCompressed);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cbLongValueOverhead")]
        public void TestJetRecsizeAdditionSetsCbLongValueOverhead()
        {
            Assert.AreEqual(0x55, this.result.cbLongValueOverhead);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cbOverhead")]
        public void TestJetRecsizeAdditionSetsCbOverhead()
        {
            Assert.AreEqual(0x66, this.result.cbOverhead);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cCompressedColumns")]
        public void TestJetRecsizeAdditionSetsCCompressedColumns()
        {
            Assert.AreEqual(0x77, this.result.cCompressedColumns);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cLongValues")]
        public void TestJetRecsizeAdditionSetsCLongValues()
        {
            Assert.AreEqual(0x88, this.result.cLongValues);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cMultiValues")]
        public void TestJetRecsizeAdditionSetsCMultiValues()
        {
            Assert.AreEqual(0x99, this.result.cMultiValues);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cNonTaggedColumns")]
        public void TestJetRecsizeAdditionSetsCNonTaggedColumns()
        {
            Assert.AreEqual(0xaa, this.result.cNonTaggedColumns);
        }

        /// <summary>
        /// Test adding two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that adding two JET_RECSIZE structs sets cTaggedColumns")]
        public void TestJetRecsizeAdditionSetsCTaggedColumns()
        {
            Assert.AreEqual(0xbb, this.result.cTaggedColumns);
        }

        /// <summary>
        /// Verify that the operator overload is equivalent to the named operation.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the operator overload is equivalent to the named operation.")]
        public void TestOperatorOverloadIsEquivalentToAdd()
        {
            Assert.AreEqual(this.namedResult, this.result);
        }
    }
}