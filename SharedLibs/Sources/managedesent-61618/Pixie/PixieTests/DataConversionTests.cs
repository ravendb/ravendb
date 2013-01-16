//-----------------------------------------------------------------------
// <copyright file="DataConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the ColumnDefinition class.
    /// </summary>
    [TestClass]
    public class DataConversionTests
    {
        private DataConversion dataConversion;

        [TestInitialize]
        public void Setup()
        {
            this.dataConversion = Dependencies.Container.Resolve<DataConversion>();
        }

        #region AsciiText

        [TestMethod]
        [Priority(1)]
        public void TestConvertAsciiText()
        {
            string expected = Any.String;
            object actual = this.RoundTripConversion(expected, ColumnType.AsciiText);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertAsciiTextFromNull()
        {
            this.TestNullConversion(ColumnType.AsciiText);
        }

        #endregion AsciiText

        #region Binary

        [TestMethod]
        [Priority(1)]
        public void TestConvertBinary()
        {
            byte[] expected = Any.Bytes;
            object actual = this.RoundTripConversion(expected, ColumnType.Binary);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertNullBinary()
        {
            object actual = this.RoundTripConversion(null, ColumnType.Binary);
            Assert.IsNull(actual);
        }

        [TestMethod]
        [Priority(1)]
        [ExpectedException(typeof(InvalidCastException))]
        public void VerifyBinaryConversionThrowsInvalidCastExceptionOnBadType()
        {
            this.RoundTripConversion(Any.Float, ColumnType.Binary);
        }

        #endregion Binary

        #region Boolean

        [TestMethod]
        [Priority(1)]
        public void VerifyBooleanConversionReturnsNullableBoolean()
        {
            bool expected = Any.Boolean;
            object result = this.dataConversion.ConvertToObject[ColumnType.Bool](expected);
            Assert.IsInstanceOfType(result, typeof(bool?));
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertBoolean()
        {
            bool expected = Any.Boolean;
            object actual = this.RoundTripConversion(expected, ColumnType.Bool);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertBooleanFromString()
        {
            bool expected = Any.Boolean;
            object actual = this.RoundTripConversion(expected.ToString(), ColumnType.Bool);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertBooleanFromNull()
        {
            this.TestNullConversion(ColumnType.Bool);
        }

        #endregion Boolean

        #region Byte

        [TestMethod]
        [Priority(1)]
        public void VerifyByteConversionReturnsNullableByte()
        {
            byte expected = Any.Byte;
            object result = this.dataConversion.ConvertToObject[ColumnType.Byte](expected);
            Assert.IsInstanceOfType(result, typeof(byte?));
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertByte()
        {
            byte expected = Any.Byte;
            object actual = this.RoundTripConversion(expected, ColumnType.Byte);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertByteFromString()
        {
            byte expected = Any.Byte;
            object actual = this.RoundTripConversion(expected.ToString(), ColumnType.Byte);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertByteFromBool()
        {
            const byte Expected = 1;
            object actual = this.dataConversion.ConvertToObject[ColumnType.Byte](true);
            Assert.AreEqual(Expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertByteFromInt()
        {
            const byte Expected = 123;
            object actual = this.dataConversion.ConvertToObject[ColumnType.Byte](123);
            Assert.AreEqual(Expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertByteFromNull()
        {
            this.TestNullConversion(ColumnType.Byte);
        }

        #endregion Byte

        #region DateTime

        [TestMethod]
        [Priority(1)]
        public void VerifyDateTimeConversionReturnsNullableDateTime()
        {
            DateTime expected = Any.DateTime;
            object result = this.dataConversion.ConvertToObject[ColumnType.DateTime](expected);
            Assert.IsInstanceOfType(result, typeof(DateTime?));
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertDateTime()
        {
            var expected = new DateTime(2008, 11, 10, 4, 56, 13);
            object actual = this.RoundTripConversion(expected, ColumnType.DateTime);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertDateTimeFromString()
        {
            var expected = new DateTime(2008, 11, 10, 4, 56, 13);
            object actual = this.RoundTripConversion(expected.ToString(), ColumnType.DateTime);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertDateTimeFromNull()
        {
            this.TestNullConversion(ColumnType.DateTime);
        }

        #endregion DateTime

        #region Double

        [TestMethod]
        [Priority(1)]
        public void VerifyDoubleConversionReturnsNullableDouble()
        {
            double expected = Any.Double;
            object result = this.dataConversion.ConvertToObject[ColumnType.Double](expected);
            Assert.IsInstanceOfType(result, typeof(double?));
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertDouble()
        {
            double expected = Any.Double;
            object actual = this.RoundTripConversion(expected, ColumnType.Double);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertDoubleFromNull()
        {
            this.TestNullConversion(ColumnType.Double);
        }

        #endregion Double

        #region Float

        [TestMethod]
        [Priority(1)]
        public void VerifyFloatConversionReturnsNullableFloat()
        {
            float expected = Any.Float;
            object result = this.dataConversion.ConvertToObject[ColumnType.Float](expected);
            Assert.IsInstanceOfType(result, typeof(float?));
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertFloat()
        {
            float expected = Any.Float;
            object actual = this.RoundTripConversion(expected, ColumnType.Float);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertFloatFromNull()
        {
            this.TestNullConversion(ColumnType.Float);
        }

        #endregion Float

        #region Guid

        [TestMethod]
        [Priority(1)]
        public void TestConvertGuid()
        {
            Guid expected = Any.Guid;
            object actual = this.RoundTripConversion(expected, ColumnType.Guid);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertGuidFromString()
        {
            Guid expected = Any.Guid;
            object actual = this.RoundTripConversion(expected.ToString(), ColumnType.Guid);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertGuidFromBytes()
        {
            Guid expected = Any.Guid;
            object actual = this.RoundTripConversion(expected.ToByteArray(), ColumnType.Guid);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertGuidFromNull()
        {
            this.TestNullConversion(ColumnType.Guid);
        }

        [TestMethod]
        [Priority(1)]
        [ExpectedException(typeof(InvalidCastException))]
        public void VerifyGuidConversionThrowsInvalidCastExceptionOnBadType()
        {
            this.RoundTripConversion(Any.DateTime, ColumnType.Guid);
        }

        #endregion Guid

        #region Int

        [TestMethod]
        [Priority(1)]
        public void VerifyIntConversionReturnsNullableInt()
        {
            int expected = Any.Int32;
            object result = this.dataConversion.ConvertToObject[ColumnType.Int32](expected);
            Assert.IsInstanceOfType(result, typeof(int?));
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertInt()
        {
            int expected = Any.Int32;
            object actual = this.RoundTripConversion(expected, ColumnType.Int32);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertIntFromString()
        {
            int expected = Any.Int32;
            object actual = this.RoundTripConversion(expected.ToString(), ColumnType.Int32);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertIntFromShort()
        {
            int expected = Any.Int16;
            object result = this.dataConversion.ConvertToObject[ColumnType.Int32](expected);
            Assert.AreEqual((int)expected, result);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertIntFromNull()
        {
            this.TestNullConversion(ColumnType.Int32);
        }

        #endregion Int

        #region Long

        [TestMethod]
        [Priority(1)]
        public void VerifyLongConversionReturnsNullableLong()
        {
            long expected = Any.Int64;
            object result = this.dataConversion.ConvertToObject[ColumnType.Int64](expected);
            Assert.IsInstanceOfType(result, typeof(long?));
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertLong()
        {
            long expected = Any.Int64;
            object actual = this.RoundTripConversion(expected, ColumnType.Int64);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertLongFromString()
        {
            long expected = Any.Int64;
            object actual = this.RoundTripConversion(expected.ToString(), ColumnType.Int64);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertLongFromNull()
        {
            this.TestNullConversion(ColumnType.Int64);
        }

        #endregion Long

        #region Short

        [TestMethod]
        [Priority(1)]
        public void VerifyShortConversionReturnsNullableShort()
        {
            short expected = Any.Int16;
            object result = this.dataConversion.ConvertToObject[ColumnType.Int16](expected);
            Assert.IsInstanceOfType(result, typeof(short?));
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertShort()
        {
            short expected = Any.Int16;
            object actual = this.RoundTripConversion(expected, ColumnType.Int16);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertShortFromString()
        {
            short expected = Any.Int16;
            object actual = this.RoundTripConversion(expected.ToString(), ColumnType.Int16);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertShortFromNull()
        {
            this.TestNullConversion(ColumnType.Int16);
        }

        #endregion Short

        #region Text

        [TestMethod]
        [Priority(1)]
        public void TestConvertText()
        {
            string expected = Any.String;
            object actual = this.RoundTripConversion(expected, ColumnType.Text);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertTextFromNull()
        {
            this.TestNullConversion(ColumnType.Text);
        }

        #endregion Text

        #region UInt

        [TestMethod]
        [Priority(1)]
        public void VerifyUIntConversionReturnsNullableUInt()
        {
            uint expected = Any.UInt32;
            object result = this.dataConversion.ConvertToObject[ColumnType.UInt32](expected);
            Assert.IsInstanceOfType(result, typeof(uint?));
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertUInt()
        {
            uint expected = Any.UInt32;
            object actual = this.RoundTripConversion(expected, ColumnType.UInt32);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertUIntFromString()
        {
            uint expected = Any.UInt32;
            object actual = this.RoundTripConversion(expected.ToString(), ColumnType.UInt32);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertUIntFromShort()
        {
            ushort expected = Any.UInt16;
            object result = this.dataConversion.ConvertToObject[ColumnType.UInt32](expected);
            Assert.AreEqual((uint)expected, result);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertUIntFromNull()
        {
            this.TestNullConversion(ColumnType.UInt32);
        }

        #endregion UInt

        #region UShort

        [TestMethod]
        [Priority(1)]
        public void VerifyUShortConversionReturnsNullableUShort()
        {
            ushort expected = Any.UInt16;
            object result = this.dataConversion.ConvertToObject[ColumnType.UInt16](expected);
            Assert.IsInstanceOfType(result, typeof(ushort?));
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertUShort()
        {
            ushort expected = Any.UInt16;
            object actual = this.RoundTripConversion(expected, ColumnType.UInt16);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertUShortFromString()
        {
            ushort expected = Any.UInt16;
            object actual = this.RoundTripConversion(expected.ToString(), ColumnType.UInt16);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertUShortFromShort()
        {
            ushort expected = Any.UInt16;
            object result = this.dataConversion.ConvertToObject[ColumnType.UInt16](expected);
            Assert.AreEqual((ushort)expected, result);
        }

        [TestMethod]
        [Priority(1)]
        public void TestConvertUShortFromNull()
        {
            this.TestNullConversion(ColumnType.UInt16);
        }

        #endregion UShort

        /// <summary>
        /// Test converting a null value to the specified column type.
        /// </summary>
        /// <param name="coltyp">The column type to do the conversion for.</param>
        private void TestNullConversion(ColumnType coltyp)
        {
            object actual = this.RoundTripConversion(null, coltyp);
            Assert.IsNull(actual);
        }

        /// <summary>
        /// Convert an object into data for a column and then back again.
        /// </summary>
        /// <param name="obj">The object to convert.</param>
        /// <param name="coltyp">The type of the column to convert for.</param>
        /// <returns>
        /// The results of converting the input object into data for the column
        /// and then back again.
        /// </returns>
        private object RoundTripConversion(object obj, ColumnType coltyp)
        {
            object converted = this.dataConversion.ConvertToObject[coltyp](obj);
            byte[] data = this.dataConversion.ConvertObjectToBytes[coltyp](converted);
            return this.dataConversion.ConvertBytesToObject[coltyp](data);
        }
    }
}
