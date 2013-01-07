//-----------------------------------------------------------------------
// <copyright file="RecsizeSubtractionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test subtracting two JET_RECSIZE structures.
    /// </summary>
    [TestClass]
    public class RecsizeSubtractionTests
    {
        /// <summary>
        /// Result of the subtraction.
        /// </summary>
        private JET_RECSIZE result;

        /// <summary>
        /// Result of the addition using the named operator.
        /// </summary>
        private JET_RECSIZE namedResult;

        /// <summary>
        /// Setup the fixture by subtracting two JET_RECSIZE objects.
        /// </summary>
        [TestInitialize]
        [Description("Setup the RecsizeSubtractionTests fixture")]
        public void Setup()
        {
            var s1 = new JET_RECSIZE
            {
                cbData = 0x11,
                cbDataCompressed = 0x22,
                cbLongValueData = 0x33,
                cbLongValueDataCompressed = 0x44,
                cbLongValueOverhead = 0x55,
                cbOverhead = 0x66,
                cCompressedColumns = 0x77,
                cLongValues = 0x88,
                cMultiValues = 0x99,
                cNonTaggedColumns = 0xaa,
                cTaggedColumns = 0xbb,
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

            this.result = s1 - s2;
            this.namedResult = JET_RECSIZE.Subtract(s1, s2);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cbData")]
        public void TestJetRecsizeSubtractionSetsCbData()
        {
            Assert.AreEqual(0x10, this.result.cbData);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cbDataCompressed")]
        public void TestJetRecsizeSubtractionSetsCbDataCompressed()
        {
            Assert.AreEqual(0x20, this.result.cbDataCompressed);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cbLongValueData")]
        public void TestJetRecsizeSubtractionSetsCbLongValueData()
        {
            Assert.AreEqual(0x30, this.result.cbLongValueData);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cbLongValueDataCompressed")]
        public void TestJetRecsizeSubtractionSetsCbLongValueDataCompressed()
        {
            Assert.AreEqual(0x40, this.result.cbLongValueDataCompressed);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cbLongValueOverhead")]
        public void TestJetRecsizeSubtractionSetsCbLongValueOverhead()
        {
            Assert.AreEqual(0x50, this.result.cbLongValueOverhead);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cbOverhead")]
        public void TestJetRecsizeSubtractionSetsCbOverhead()
        {
            Assert.AreEqual(0x60, this.result.cbOverhead);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cCompressedColumns")]
        public void TestJetRecsizeSubtractionSetsCCompressedColumns()
        {
            Assert.AreEqual(0x70, this.result.cCompressedColumns);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cLongValues")]
        public void TestJetRecsizeSubtractionSetsCLongValues()
        {
            Assert.AreEqual(0x80, this.result.cLongValues);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cMultiValues")]
        public void TestJetRecsizeSubtractionSetsCMultiValues()
        {
            Assert.AreEqual(0x90, this.result.cMultiValues);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cNonTaggedColumns")]
        public void TestJetRecsizeSubtractionSetsCNonTaggedColumns()
        {
            Assert.AreEqual(0xa0, this.result.cNonTaggedColumns);
        }

        /// <summary>
        /// Test subtracting two structs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that subtracting two JET_RECSIZE structs sets cTaggedColumns")]
        public void TestJetRecsizeSubtractionSetsCTaggedColumns()
        {
            Assert.AreEqual(0xb0, this.result.cTaggedColumns);
        }

        /// <summary>
        /// Verify that the operator overload is equivalent to the named operation.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the operator overload is equivalent to the named operation.")]
        public void TestOperatorOverloadIsEquivalentToSubtract()
        {
            Assert.AreEqual(this.namedResult, this.result);
        }
    }
}