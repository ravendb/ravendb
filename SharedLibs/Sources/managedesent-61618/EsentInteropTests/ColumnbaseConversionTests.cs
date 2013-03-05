//-----------------------------------------------------------------------
// <copyright file="ColumnbaseConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test creating a JET_COLUMNBASE from a NATIVE_COLUMNBASE.
    /// </summary>
    [TestClass]
    public class ColumnbaseConversionTests
    {
        /// <summary>
        /// The native version of the structure.
        /// </summary>
        private NATIVE_COLUMNBASE native;

        /// <summary>
        /// The managed version created from the native.
        /// </summary>
        private JET_COLUMNBASE managed;

        /// <summary>
        /// Initialize the ColumnbaseConversionTests fixture.
        /// </summary>
        [TestInitialize]
        [Description("Initialize the ColumnbaseConversionTests fixture")]
        public void Setup()
        {
            this.native = new NATIVE_COLUMNBASE
            {
                cbMax = 1,
                coltyp = unchecked((uint)JET_coltyp.Text),
                columnid = 2,
                cp = unchecked((ushort)JET_CP.Unicode),
                grbit = unchecked((uint)ColumndefGrbit.ColumnNotNULL),
                szBaseColumnName = "basecolumn",
                szBaseTableName = "basetable",
            };

            this.managed = new JET_COLUMNBASE(this.native);
        }

        /// <summary>
        /// Verify cbMax is converted.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify cbMax is converted")]
        public void VerifyCbMaxIsConverted()
        {
            Assert.AreEqual(1, this.managed.cbMax);           
        }

        /// <summary>
        /// Verify coltyp is converted.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify coltyp is converted")]
        public void VerifyColtypIsConverted()
        {
            Assert.AreEqual(JET_coltyp.Text, this.managed.coltyp);
        }

        /// <summary>
        /// Verify columnid is converted.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify columnid is converted")]
        public void VerifyColumnidIsConverted()
        {
            Assert.AreEqual(new JET_COLUMNID { Value = 2 }, this.managed.columnid);
        }

        /// <summary>
        /// Verify cp is converted.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify cp is converted")]
        public void VerifyCpIsConverted()
        {
            Assert.AreEqual(JET_CP.Unicode, this.managed.cp);
        }

        /// <summary>
        /// Verify grbit is converted.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify grbit is converted")]
        public void VerifyGrbitIsConverted()
        {
            Assert.AreEqual(ColumndefGrbit.ColumnNotNULL, this.managed.grbit);
        }

        /// <summary>
        /// Verify szBaseColumnName is converted.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify szBaseColumnName is converted")]
        public void VerifyBaseColumnNameIsConverted()
        {
            Assert.AreEqual("basecolumn", this.managed.szBaseColumnName);
        }

        /// <summary>
        /// Verify szBaseTableName is converted.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify szBaseTableName is converted")]
        public void VerifyBaseTableNameIsConverted()
        {
            Assert.AreEqual("basetable", this.managed.szBaseTableName);
        }
    }
}