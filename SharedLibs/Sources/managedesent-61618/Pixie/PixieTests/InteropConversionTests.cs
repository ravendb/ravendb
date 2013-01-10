//-----------------------------------------------------------------------
// <copyright file="InteropConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent;
using Microsoft.Isam.Esent.Interop;
using Microsoft.Isam.Esent.Interop.Vista;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Tests for converting Esent.Interop objects.
    /// </summary>
    [TestClass]
    public class InteropConversionTests
    {
        private InteropConversion converter;
        private JET_COLUMNID columnid;

        [TestInitialize]
        public void Setup()
        {
            this.converter = Dependencies.Container.Resolve<InteropConversion>();
            this.columnid = new JET_COLUMNID
                {
                    Value = Any.UInt16
                };
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithBoolColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(JET_coltyp.Bit, ColumnType.Bool);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithByteColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(JET_coltyp.UnsignedByte, ColumnType.Byte);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithShortColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(JET_coltyp.Short, ColumnType.Int16);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithUShortColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(VistaColtyp.UnsignedShort, ColumnType.UInt16);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithIntColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(JET_coltyp.Long, ColumnType.Int32);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithUIntColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(VistaColtyp.UnsignedLong, ColumnType.UInt32);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithLongColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(JET_coltyp.Currency, ColumnType.Int64);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithFloatColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(JET_coltyp.IEEESingle, ColumnType.Float);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithDoubleColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(JET_coltyp.IEEEDouble, ColumnType.Double);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithDateTimeColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(JET_coltyp.DateTime, ColumnType.DateTime);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithGuidColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(VistaColtyp.GUID, ColumnType.Guid);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithBinaryColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(JET_coltyp.Binary, ColumnType.Binary);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithLongBinaryColumn()
        {
            this.VerifyCreateColumnMetaDataSetsType(JET_coltyp.LongBinary, ColumnType.Binary);
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithAsciiColumn()
        {
            var info = new ColumnInfo(
                "ascii",
                this.columnid,
                JET_coltyp.LongText,
                JET_CP.ASCII,
                0,
                null,
                ColumndefGrbit.None);

            var expected = new ColumnMetaData
                               {
                                   Name = "ascii",
                                   Type = ColumnType.AsciiText,
                                   Columnid = this.columnid,
                               };

            AssertColumnMetaDataAreEqual(expected, this.converter.CreateColumnMetaDataFromColumnInfo(info));
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithUnicodeColumn()
        {
            var info = new ColumnInfo(
                "unicode",
                this.columnid,
                JET_coltyp.Text,
                JET_CP.Unicode,
                0,
                null,
                ColumndefGrbit.None);

            var expected = new ColumnMetaData
                               {
                                   Name = "unicode",
                                   Type = ColumnType.Text,
                                   Columnid = this.columnid,
                               };

            AssertColumnMetaDataAreEqual(expected, this.converter.CreateColumnMetaDataFromColumnInfo(info));
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithAutoinc()
        {
            var info = new ColumnInfo(
                "autoinc",
                this.columnid,
                JET_coltyp.Long,
                JET_CP.None,
                0,
                null,
                ColumndefGrbit.ColumnAutoincrement);

            var expected = new ColumnMetaData
                               {
                                   Name = "autoinc",
                                   Type = ColumnType.Int32,
                                   Columnid = this.columnid,
                                   IsAutoincrement = true,
                               };

            AssertColumnMetaDataAreEqual(expected, this.converter.CreateColumnMetaDataFromColumnInfo(info));
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithVersionColumn()
        {
            var info = new ColumnInfo(
                "version",
                this.columnid,
                JET_coltyp.Long,
                JET_CP.None,
                0,
                null,
                ColumndefGrbit.ColumnVersion);

            var expected = new ColumnMetaData
                               {
                                   Name = "version",
                                   Type = ColumnType.Int32,
                                   Columnid = this.columnid,
                                   IsVersion = true,
                               };

            AssertColumnMetaDataAreEqual(expected, this.converter.CreateColumnMetaDataFromColumnInfo(info));
        }

        [TestMethod]
        [Priority(1)]
        public void TestCreateColumnMetaDataFromColumnInfoWithEscrowUpdate()
        {
            var info = new ColumnInfo(
                "escrow_update",
                this.columnid,
                JET_coltyp.Long,
                JET_CP.None,
                0,
                null,
                ColumndefGrbit.ColumnEscrowUpdate);

            var expected = new ColumnMetaData
               {
                   Name = "escrow_update",
                   Type = ColumnType.Int32,
                   Columnid = this.columnid,
                   IsEscrowUpdate = true,
               };

            AssertColumnMetaDataAreEqual(expected, this.converter.CreateColumnMetaDataFromColumnInfo(info));
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsColtypBit()
        {
            ColumnDefinition def = DefinedAs.BoolColumn(Any.String);
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(JET_coltyp.Bit, columndef.coltyp);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsColtypByte()
        {
            ColumnDefinition def = DefinedAs.ByteColumn(Any.String);
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(JET_coltyp.UnsignedByte, columndef.coltyp);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsColtypShort()
        {
            ColumnDefinition def = DefinedAs.Int16Column(Any.String);
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(JET_coltyp.Short, columndef.coltyp);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsColtypSingle()
        {
            ColumnDefinition def = DefinedAs.FloatColumn(Any.String);
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(JET_coltyp.IEEESingle, columndef.coltyp);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsColtypDouble()
        {
            ColumnDefinition def = DefinedAs.DoubleColumn(Any.String);
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(JET_coltyp.IEEEDouble, columndef.coltyp);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsCodePageForUnicodeColumn()
        {
            ColumnDefinition def = DefinedAs.TextColumn(Any.String);
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(JET_CP.Unicode, columndef.cp);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsCodePageForAsciiColumn()
        {
            ColumnDefinition def = DefinedAs.AsciiTextColumn(Any.String);
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(JET_CP.ASCII, columndef.cp);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsCbMax()
        {
            ColumnDefinition def = DefinedAs.BinaryColumn(Any.String).WithMaxSize(25);
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(25, columndef.cbMax);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsAutoincrement()
        {
            ColumnDefinition def = DefinedAs.Int64Column(Any.String).AsAutoincrement();
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(ColumndefGrbit.ColumnAutoincrement, columndef.grbit);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsIsNotNull()
        {
            ColumnDefinition def = DefinedAs.Int64Column(Any.String).MustBeNonNull();
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(ColumndefGrbit.ColumnNotNULL, columndef.grbit);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyCreateColumndefFromColumnDefinitionSetsVersion()
        {
            ColumnDefinition def = DefinedAs.Int64Column(Any.String).AsVersion();
            JET_COLUMNDEF columndef = this.converter.CreateColumndefFromColumnDefinition(def);

            Assert.AreEqual(ColumndefGrbit.ColumnVersion, columndef.grbit);
        }

        /// <summary>
        /// Assert that the two ColumnMetaData objects are equal.
        /// </summary>
        /// <param name="expected">The expected column meta-data.</param>
        /// <param name="actual">The actual column meta-data.</param>
        private static void AssertColumnMetaDataAreEqual(ColumnMetaData expected, ColumnMetaData actual)
        {
            Assert.AreEqual(expected.Name, actual.Name);
            Assert.AreEqual(expected.Type, actual.Type);
            Assert.AreEqual(expected.IsAutoincrement, actual.IsAutoincrement);
            Assert.AreEqual(expected.IsNotNull, actual.IsNotNull);
            Assert.AreEqual(expected.IsVersion, actual.IsVersion);
            Assert.AreEqual(expected.IsEscrowUpdate, actual.IsEscrowUpdate);
            Assert.AreEqual(expected.MaxSize, actual.MaxSize);
            Assert.AreEqual(expected.Columnid, actual.Columnid);
            Assert.AreEqual(expected.IsIndexed, actual.IsIndexed);
            Assert.AreEqual(expected.IsUnique, actual.IsUnique);

            if (null != expected.DefaultValue)
            {
                if (expected.DefaultValue.GetType() == typeof(byte[]))
                {
                    CollectionAssert.AreEqual((byte[]) expected.DefaultValue, (byte[]) actual.DefaultValue);
                }
                else
                {
                    Assert.AreEqual(expected.DefaultValue, actual.DefaultValue);
                }
            }
        }

        /// <summary>
        /// Verify that CreateColumnMetaDataFromColumnInfo sets the specified ColumnType.
        /// </summary>
        /// <param name="jetColtyp">The coltyp for the ColumnInfo.</param>
        /// <param name="type">The expected ColumnType in the ColumnMetaData.</param>
        private void VerifyCreateColumnMetaDataSetsType(JET_coltyp jetColtyp, ColumnType type)
        {
            var info = new ColumnInfo(
                "test",
                this.columnid,
                jetColtyp,
                JET_CP.None,
                0,
                null,
                ColumndefGrbit.None);

            var expected = new ColumnMetaData
                               {
                                   Name = "test",
                                   Type = type,
                                   Columnid = this.columnid,
                               };

            AssertColumnMetaDataAreEqual(expected, this.converter.CreateColumnMetaDataFromColumnInfo(info));
        }
    }
}
