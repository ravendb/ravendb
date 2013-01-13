//-----------------------------------------------------------------------
// <copyright file="DefinedAsTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the DefinedAs class
    /// </summary>
    [TestClass]
    public class DefinedAsTests
    {
        [TestMethod]
        [Priority(1)]
        public void VerifyBoolColumnCreatesBoolColumn()
        {
            ColumnDefinition columndef = DefinedAs.BoolColumn("bool");
            Assert.AreEqual("bool", columndef.Name);
            Assert.AreEqual(ColumnType.Bool, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyByteColumnCreatesByteColumn()
        {
            ColumnDefinition columndef = DefinedAs.ByteColumn("Byte");
            Assert.AreEqual("Byte", columndef.Name);
            Assert.AreEqual(ColumnType.Byte, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyShortColumnCreatesShortColumn()
        {
            ColumnDefinition columndef = DefinedAs.Int16Column("Short");
            Assert.AreEqual("Short", columndef.Name);
            Assert.AreEqual(ColumnType.Int16, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyUShortColumnCreatesUShortColumn()
        {
            ColumnDefinition columndef = DefinedAs.UInt16Column("UShort");
            Assert.AreEqual("UShort", columndef.Name);
            Assert.AreEqual(ColumnType.UInt16, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyIntColumnCreatesIntColumn()
        {
            ColumnDefinition columndef = DefinedAs.Int32Column("int");
            Assert.AreEqual("int", columndef.Name);
            Assert.AreEqual(ColumnType.Int32, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyUIntColumnCreatesUIntColumn()
        {
            ColumnDefinition columndef = DefinedAs.UInt32Column("UInt");
            Assert.AreEqual("UInt", columndef.Name);
            Assert.AreEqual(ColumnType.UInt32, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyLongColumnCreatesLongColumn()
        {
            ColumnDefinition columndef = DefinedAs.Int64Column("Long");
            Assert.AreEqual("Long", columndef.Name);
            Assert.AreEqual(ColumnType.Int64, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyFloatColumnCreatesFloatColumn()
        {
            ColumnDefinition columndef = DefinedAs.FloatColumn("Float");
            Assert.AreEqual("Float", columndef.Name);
            Assert.AreEqual(ColumnType.Float, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDoubleColumnCreatesDoubleColumn()
        {
            ColumnDefinition columndef = DefinedAs.DoubleColumn("Double");
            Assert.AreEqual("Double", columndef.Name);
            Assert.AreEqual(ColumnType.Double, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDateTimeColumnCreatesDateTimeColumn()
        {
            ColumnDefinition columndef = DefinedAs.DateTimeColumn("DateTime");
            Assert.AreEqual("DateTime", columndef.Name);
            Assert.AreEqual(ColumnType.DateTime, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyGuidColumnCreatesGuidColumn()
        {
            ColumnDefinition columndef = DefinedAs.GuidColumn("Guid");
            Assert.AreEqual("Guid", columndef.Name);
            Assert.AreEqual(ColumnType.Guid, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyBinaryColumnCreatesBinaryColumn()
        {
            ColumnDefinition columndef = DefinedAs.BinaryColumn("Binary");
            Assert.AreEqual("Binary", columndef.Name);
            Assert.AreEqual(ColumnType.Binary, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyTextColumnCreatesTextColumn()
        {
            ColumnDefinition columndef = DefinedAs.TextColumn("text");
            Assert.AreEqual("text", columndef.Name);
            Assert.AreEqual(ColumnType.Text, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyAsciiTextColumnCreatesAsciiTextColumn()
        {
            ColumnDefinition columndef = DefinedAs.AsciiTextColumn("asciitext");
            Assert.AreEqual("asciitext", columndef.Name);
            Assert.AreEqual(ColumnType.AsciiText, columndef.Type);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyDefinedAsSetsDefaultOptions()
        {
            ColumnDefinition columndef = DefinedAs.Int32Column("sample");
            Assert.IsFalse(columndef.IsAutoincrement);
            Assert.IsFalse(columndef.IsNotNull);
            Assert.IsFalse(columndef.IsVersion);
            Assert.AreEqual(0, columndef.MaxSize);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyAsAutoincrementSetsAutoincrement()
        {
            ColumnDefinition columndef = DefinedAs.Int32Column("col").AsAutoincrement();
            Assert.IsTrue(columndef.IsAutoincrement);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyWithMaxSizeSetsMaxLength()
        {
            ColumnDefinition columndef = DefinedAs.Int32Column("col").WithMaxSize(100);
            Assert.AreEqual(100, columndef.MaxSize);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyWithDefaultValueSetsDefaultValue()
        {
            ColumnDefinition columndef = DefinedAs.Int32Column("col").WithDefaultValue(56);
            Assert.AreEqual(56, columndef.DefaultValue);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyMustBeNonNullSetsIsNotNull()
        {
            ColumnDefinition columndef = DefinedAs.Int32Column("col").MustBeNonNull();
            Assert.IsTrue(columndef.IsNotNull);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyAsVersionSetsVersion()
        {
            ColumnDefinition columndef = DefinedAs.Int32Column("col").AsVersion();
            Assert.IsTrue(columndef.IsVersion);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyChainingSetterMethods()
        {
            ColumnDefinition columndef = DefinedAs.TextColumn("chained").WithMaxSize(64).WithDefaultValue("default");
            Assert.AreEqual("chained", columndef.Name);
            Assert.AreEqual(64, columndef.MaxSize);
            Assert.AreEqual("default", columndef.DefaultValue);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyChainingAsAutoincrementProducesNewObject()
        {
            ColumnDefinition columndef1 = DefinedAs.TextColumn("col");
            ColumnDefinition columndef2 = columndef1.AsAutoincrement();
            Assert.IsFalse(columndef1.IsAutoincrement);
            Assert.IsTrue(columndef2.IsAutoincrement);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyChainingAsVersionProducesNewObject()
        {
            ColumnDefinition columndef1 = DefinedAs.TextColumn("col");
            ColumnDefinition columndef2 = columndef1.AsVersion();
            Assert.IsFalse(columndef1.IsVersion);
            Assert.IsTrue(columndef2.IsVersion);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyChainingMustBeNonNullProducesNewObject()
        {
            ColumnDefinition columndef1 = DefinedAs.TextColumn("col");
            ColumnDefinition columndef2 = columndef1.MustBeNonNull();
            Assert.IsFalse(columndef1.IsNotNull);
            Assert.IsTrue(columndef2.IsNotNull);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyChainingWithMaxSizeProducesNewObject()
        {
            ColumnDefinition columndef1 = DefinedAs.TextColumn("col");
            ColumnDefinition columndef2 = columndef1.WithMaxSize(64);
            Assert.AreEqual(0, columndef1.MaxSize);
            Assert.AreEqual(64, columndef2.MaxSize);
        }

        [TestMethod]
        [Priority(1)]
        public void VerifyChainingWithDefaultValueProducesNewObject()
        {
            ColumnDefinition columndef1 = DefinedAs.BinaryColumn("col");
            ColumnDefinition columndef2 = columndef1.WithDefaultValue(8);
            Assert.IsNull(columndef1.DefaultValue);
            Assert.AreEqual(8, columndef2.DefaultValue);
        }
    }
}