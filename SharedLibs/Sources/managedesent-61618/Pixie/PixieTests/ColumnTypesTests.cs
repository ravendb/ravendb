//-----------------------------------------------------------------------
// <copyright file="ColumnTypesTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the basic table operations. This fixture is created once and then reused.
    /// For each test a connection is created and the test is wrapped in a transaction
    /// which is rolled back. Do not add any tests that open new connections or the
    /// database will end up being modified which can affect other tests in the fixture.
    /// This fixture has one column of each type.
    /// </summary>
    [TestClass]
    public class ColumnTypesTests
    {
        private static string directory;
        private static string database;
        private static string tablename;

        private Connection connection;
        private Table table;
        private Transaction transaction;

        /// <summary>
        /// Create the database, table and column. Called once for a test run.
        /// </summary>
        /// <param name="ignored">Ignored TestContext.</param>
        [ClassInitialize]
        public static void CreateDatabase(TestContext ignored)
        {
            directory = "column_types_tests";
            database = Path.Combine(directory, "columns.edb");
            Directory.CreateDirectory(directory);

            tablename = "table";

            using (Connection connection = Esent.CreateDatabase(database))
            using (Transaction transaction = connection.BeginTransaction())
            using (Table table = connection.CreateTable(tablename))
            {
                table.CreateColumn(new ColumnDefinition("bool", ColumnType.Bool));
                table.CreateColumn(new ColumnDefinition("byte", ColumnType.Byte));
                table.CreateColumn(new ColumnDefinition("short", ColumnType.Int16));
                table.CreateColumn(new ColumnDefinition("ushort", ColumnType.UInt16));
                table.CreateColumn(new ColumnDefinition("int", ColumnType.Int32));
                table.CreateColumn(new ColumnDefinition("uint", ColumnType.UInt32));
                table.CreateColumn(new ColumnDefinition("long", ColumnType.Int64));
                table.CreateColumn(new ColumnDefinition("float", ColumnType.Float));
                table.CreateColumn(new ColumnDefinition("double", ColumnType.Double));
                table.CreateColumn(new ColumnDefinition("datetime", ColumnType.DateTime));
                table.CreateColumn(new ColumnDefinition("guid", ColumnType.Guid));
                table.CreateColumn(new ColumnDefinition("text", ColumnType.Text));
                table.CreateColumn(new ColumnDefinition("asciitext", ColumnType.AsciiText));
                table.CreateColumn(new ColumnDefinition("binary", ColumnType.Binary));

                transaction.Commit();
            }
        }

        /// <summary>
        /// Delete the database. Called after all tests have run.
        /// </summary>
        [ClassCleanup]
        public static void DeleteDatabase()
        {
            Directory.Delete(directory, true);
        }

        /// <summary>
        /// Create a connection, transaction and open the table. This is called once
        /// per test. The transaction will be rolled-back by the Cleanup() method.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.connection = Esent.OpenDatabase(database);
            this.transaction = this.connection.BeginTransaction();
            this.table = this.connection.OpenTable(tablename);
        }

        /// <summary>
        /// Rollback the transaction and close the connection. This is called once
        /// per test.
        /// </summary>
        [TestCleanup]
        public void Cleanup()
        {
            this.table.Dispose();
            this.transaction.Rollback();
            this.connection.Dispose();
        }

        #region Bool

        /// <summary>
        /// Set a boolean column.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetBoolColumnToTrue()
        {
            Record r = this.table.NewRecord();
            r["bool"] = true;
            r.Save();

            Assert.AreEqual(true, r["bool"]);
        }

        /// <summary>
        /// Set a boolean column to false.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetBoolColumnToFalse()
        {
            Record r = this.table.NewRecord();
            r["bool"] = false;
            r.Save();

            Assert.AreEqual(false, r["bool"]);
        }

        /// <summary>
        /// Set a boolean column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetBooleanColumnFromString()
        {
            Record r = this.table.NewRecord();
            r["bool"] = "true";
            r.Save();

            Assert.AreEqual(true, r["bool"]);
        }

        /// <summary>
        /// Set a boolean column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetBooleanColumnFromInt()
        {
            Record r = this.table.NewRecord();
            r["bool"] = 0;
            r.Save();

            Assert.AreEqual(false, r["bool"]);
        }

        #endregion Bool

        #region Byte

        /// <summary>
        /// Set a byte column to the maximum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetByteColumnToMaxValue()
        {
            Record r = this.table.NewRecord();
            r["byte"] = Byte.MaxValue;
            r.Save();

            Assert.AreEqual(Byte.MaxValue, r["byte"]);
        }

        /// <summary>
        /// Set a byte column to the minimum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetByteColumnToMinValue()
        {
            Record r = this.table.NewRecord();
            r["byte"] = Byte.MinValue;
            r.Save();

            Assert.AreEqual(Byte.MinValue, r["byte"]);
        }

        /// <summary>
        /// Set a byte column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetByteColumnFromString()
        {
            Record r = this.table.NewRecord();
            r["byte"] = "32";
            r.Save();

            Assert.AreEqual((byte)0x20, r["byte"]);
        }

        /// <summary>
        /// Set a byte column from an int.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetByteColumnFromInt()
        {
            Record r = this.table.NewRecord();
            r["byte"] = 0x45;
            r.Save();

            Assert.AreEqual((byte)0x45, r["byte"]);
        }

        #endregion Byte

        #region Short

        /// <summary>
        /// Set a short column to the maximum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetShortColumnToMaxValue()
        {
            Record r = this.table.NewRecord();
            r["short"] = short.MaxValue;
            r.Save();

            Assert.AreEqual(short.MaxValue, r["short"]);
        }

        /// <summary>
        /// Set a short column to the minimum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetShortColumnToMinValue()
        {
            Record r = this.table.NewRecord();
            r["short"] = short.MinValue;
            r.Save();

            Assert.AreEqual(short.MinValue, r["short"]);
        }

        /// <summary>
        /// Set a short column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetShortColumnFromString()
        {
            Record r = this.table.NewRecord();
            r["short"] = "999";
            r.Save();

            Assert.AreEqual((short)999, r["short"]);
        }

        #endregion Short

        #region Ushort

        /// <summary>
        /// Set a ushort column to the maximum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetUshortColumnToMaxValue()
        {
            Record r = this.table.NewRecord();
            r["ushort"] = ushort.MaxValue;
            r.Save();

            Assert.AreEqual(ushort.MaxValue, r["ushort"]);
        }

        /// <summary>
        /// Set a ushort column to the minimum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetUshortColumnToMinValue()
        {
            Record r = this.table.NewRecord();
            r["ushort"] = ushort.MinValue;
            r.Save();

            Assert.AreEqual(ushort.MinValue, r["ushort"]);
        }

        /// <summary>
        /// Set a ushort column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetUshortColumnFromString()
        {
            Record r = this.table.NewRecord();
            r["ushort"] = "999";
            r.Save();

            Assert.AreEqual((ushort)999, r["ushort"]);
        }

        #endregion Ushort

        #region Int

        /// <summary>
        /// Set an int column to the maximum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetIntColumnToMaxValue()
        {
            Record r = this.table.NewRecord();
            r["int"] = int.MaxValue;
            r.Save();

            Assert.AreEqual(int.MaxValue, r["int"]);
        }

        /// <summary>
        /// Set an int column to the minimum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetIntColumnToMinValue()
        {
            Record r = this.table.NewRecord();
            r["int"] = int.MinValue;
            r.Save();

            Assert.AreEqual(int.MinValue, r["int"]);
        }

        /// <summary>
        /// Set an int column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetIntColumnFromString()
        {
            Record r = this.table.NewRecord();
            r["int"] = "999";
            r.Save();

            Assert.AreEqual(999, r["int"]);
        }

        #endregion Int

        #region Uint

        /// <summary>
        /// Set a uint column to the maximum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetUintColumnToMaxValue()
        {
            Record r = this.table.NewRecord();
            r["uint"] = uint.MaxValue;
            r.Save();

            Assert.AreEqual(uint.MaxValue, r["uint"]);
        }

        /// <summary>
        /// Set a uint column to the minimum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetUintColumnToMinValue()
        {
            Record r = this.table.NewRecord();
            r["uint"] = uint.MinValue;
            r.Save();

            Assert.AreEqual(uint.MinValue, r["uint"]);
        }

        /// <summary>
        /// Set a uint column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetUintColumnFromString()
        {
            Record r = this.table.NewRecord();
            r["uint"] = "999";
            r.Save();

            Assert.AreEqual((uint)999, r["uint"]);
        }

        #endregion Uint

        #region Long

        /// <summary>
        /// Set a long column to the maximum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetLongColumnToMaxValue()
        {
            Record r = this.table.NewRecord();
            r["long"] = long.MaxValue;
            r.Save();

            Assert.AreEqual(long.MaxValue, r["long"]);
        }

        /// <summary>
        /// Set a long column to the minimum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetLongColumnToMinValue()
        {
            Record r = this.table.NewRecord();
            r["long"] = long.MinValue;
            r.Save();

            Assert.AreEqual(long.MinValue, r["long"]);
        }

        /// <summary>
        /// Set a long column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetLongColumnFromString()
        {
            Record r = this.table.NewRecord();
            r["long"] = "999";
            r.Save();

            Assert.AreEqual((long)999, r["long"]);
        }

        #endregion Long

        #region Float

        /// <summary>
        /// Set a float column to the maximum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetFloatColumnToMaxValue()
        {
            Record r = this.table.NewRecord();
            r["float"] = float.MaxValue;
            r.Save();

            Assert.AreEqual(float.MaxValue, r["float"]);
        }

        /// <summary>
        /// Set a float column to the minimum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetFloatColumnToMinValue()
        {
            Record r = this.table.NewRecord();
            r["float"] = float.MinValue;
            r.Save();

            Assert.AreEqual(float.MinValue, r["float"]);
        }

        /// <summary>
        /// Set a float column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetFloatColumnFromString()
        {
            float expected = 123.456F;
            Record r = this.table.NewRecord();
            r["float"] = expected.ToString();
            r.Save();

            Assert.AreEqual(expected, r["float"]);
        }

        #endregion Float

        #region Double

        /// <summary>
        /// Set a double column to the maximum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetDoubleColumnToMaxValue()
        {
            Record r = this.table.NewRecord();
            r["double"] = double.MaxValue;
            r.Save();

            Assert.AreEqual(double.MaxValue, r["double"]);
        }

        /// <summary>
        /// Set a double column to the minimum value.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetDoubleColumnToMinValue()
        {
            Record r = this.table.NewRecord();
            r["double"] = double.MinValue;
            r.Save();

            Assert.AreEqual(double.MinValue, r["double"]);
        }

        /// <summary>
        /// Set a double column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetDoubleColumnFromString()
        {
            Record r = this.table.NewRecord();
            r["double"] = Math.PI;
            r.Save();

            Assert.AreEqual(Math.PI, r["double"]);
        }

        #endregion Double

        #region DateTime

        /// <summary>
        /// Set a date time column to the current time.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetDateTimeColumnToNow()
        {
            DateTime expected = DateTime.Now;
            Record r = this.table.NewRecord();
            r["datetime"] = expected;
            r.Save();

            // The esent DateTime column doesn't have the same
            // resolution as the .NET type. Assert we are close (1 sec).
            TimeSpan diff = (DateTime)r["datetime"] - expected;
            int diffSeconds = Math.Abs(diff.Seconds);
            Assert.IsTrue(diffSeconds <= 1);
        }

        #endregion DateTime

        #region Guid 

        /// <summary>
        /// Set a GUID column to a guid.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetGuidColumn()
        {
            Guid expected = Any.Guid;
            Record r = this.table.NewRecord();
            r["guid"] = expected;
            r.Save();

            Assert.AreEqual(expected, r["guid"]);
        }

        /// <summary>
        /// Set guid column to a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetGuidColumnFromString()
        {
            Guid expected = Any.Guid;
            Record r = this.table.NewRecord();
            r["guid"] = expected.ToString();
            r.Save();

            Assert.AreEqual(expected, r["guid"]);
        }

        /// <summary>
        /// Set guid column to a byte array.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetGuidColumnFromBytes()
        {
            Guid expected = Any.Guid;
            Record r = this.table.NewRecord();
            r["guid"] = expected.ToString();
            r.Save();

            Assert.AreEqual(expected, r["guid"]);
        }

        #endregion Guid

        #region Text

        /// <summary>
        /// Set a text column to an empty String
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetTextColumnToEmptyString()
        {
            Record r = this.table.NewRecord();
            r["text"] = string.Empty;
            r.Save();

            Assert.AreEqual(string.Empty, r["text"]);
        }

        /// <summary>
        /// Set a text column to a string
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetTextColumnToString()
        {
            string expected = Any.String;

            Record r = this.table.NewRecord();
            r["text"] = expected;
            r.Save();

            Assert.AreEqual(expected, r["text"]);
        }

        /// <summary>
        /// Set a text column to an integer.
        /// This tests that .ToString() is called.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetTextColumnToInt()
        {
            Record r = this.table.NewRecord();
            r["text"] = 1024;
            r.Save();

            Assert.AreEqual("1024", r["text"]);
        }

        #endregion Text

        #region AsciiText

        /// <summary>
        /// Set an ASCII text column to an empty String
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetAsciiTextColumnToEmptyString()
        {
            Record r = this.table.NewRecord();
            r["asciitext"] = string.Empty;
            r.Save();

            Assert.AreEqual(string.Empty, r["asciitext"]);
        }

        /// <summary>
        /// Set an ASCII text column to a string
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetAsciiTextColumnToString()
        {
            string expected = Any.String;

            Record r = this.table.NewRecord();
            r["asciitext"] = expected;
            r.Save();

            Assert.AreEqual(expected, r["asciitext"]);
        }

        /// <summary>
        /// Set an ASCII text column to an integer.
        /// This tests that .ToString() is called.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetAsciiTextColumnToInt()
        {
            Record r = this.table.NewRecord();
            r["asciitext"] = 123;
            r.Save();

            Assert.AreEqual("123", r["asciitext"]);
        }

        #endregion AsciiText

        #region Binary

        /// <summary>
        /// Set a binary column to a zero-length array 
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetBinaryColumnToZeroLengthArray()
        {
            Record r = this.table.NewRecord();
            r["binary"] = new byte[0];
            r.Save();

            CollectionAssert.AreEqual(new byte[0], (byte[]) r["binary"]);
        }

        /// <summary>
        /// Set a binary column to some data
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetBinaryColumn()
        {
            byte[] expected = Any.Bytes;

            Record r = this.table.NewRecord();
            r["binary"] = expected;
            r.Save();

            CollectionAssert.AreEqual(expected, (byte[])r["binary"]);
        }

        #endregion Binary

        /// <summary>
        /// Sets every column.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetAllColumns()
        {
            bool boolean    = Any.Boolean;
            byte b          = Any.Byte;
            short i16       = Any.Int16;
            ushort u16      = Any.UInt16;
            int i32         = Any.Int32;
            uint u32        = Any.UInt32;
            long i64        = Any.Int64;
            float f         = Any.Float;
            double d        = Any.Double;
            Guid guid       = Any.Guid;
            DateTime datetime    = Any.DateTime;
            string text     = Any.String;
            string ascii    = Any.String;
            byte[] binary   = Any.Bytes;

            Record record = this.table.NewRecord();
            record["bool"]      = boolean;
            record["byte"]      = b;
            record["short"]     = i16;
            record["ushort"]    = u16;
            record["int"]       = i32;
            record["uint"]      = u32;
            record["long"]      = i64;
            record["float"]     = f;
            record["double"]    = d;
            record["guid"]      = guid;
            record["datetime"]  = datetime;
            record["text"]      = text;
            record["asciitext"] = ascii;
            record["binary"]    = binary;
            record.Save();

            Assert.AreEqual(boolean, record["bool"]);
            Assert.AreEqual(b, record["byte"]);
            Assert.AreEqual(i16, record["short"]);
            Assert.AreEqual(u16, record["ushort"]);
            Assert.AreEqual(i32, record["int"]);
            Assert.AreEqual(u32, record["uint"]);
            Assert.AreEqual(i64, record["long"]);
            Assert.AreEqual(f, record["float"]);
            Assert.AreEqual(d, record["double"]);
            Assert.AreEqual(guid, record["guid"]);
            Assert.AreEqual(datetime, record["datetime"]);
            Assert.AreEqual(text, record["text"]);
            Assert.AreEqual(ascii, record["asciitext"]);
            CollectionAssert.AreEqual(binary, (byte[])record["binary"]);
        }

        /// <summary>
        /// Sets every column inside of an update
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void SetAllColumnsInUpdate()
        {
            bool boolean = Any.Boolean;
            byte b = Any.Byte;
            short i16 = Any.Int16;
            ushort u16 = Any.UInt16;
            int i32 = Any.Int32;
            uint u32 = Any.UInt32;
            long i64 = Any.Int64;
            float f = Any.Float;
            double d = Any.Double;
            Guid guid = Any.Guid;
            DateTime datetime = Any.DateTime;
            string text = Any.String;
            string ascii = Any.String;
            byte[] binary = Any.Bytes;

            // Create a record and update it
            Record record = this.table.NewRecord();
            record.Save();

            record["bool"] = boolean;
            record["byte"] = b;
            record["short"] = i16;
            record["ushort"] = u16;
            record["int"] = i32;
            record["uint"] = u32;
            record["long"] = i64;
            record["float"] = f;
            record["double"] = d;
            record["guid"] = guid;
            record["datetime"] = datetime;
            record["text"] = text;
            record["asciitext"] = ascii;
            record["binary"] = binary;

            Assert.AreEqual(boolean, record["bool"]);
            Assert.AreEqual(b, record["byte"]);
            Assert.AreEqual(i16, record["short"]);
            Assert.AreEqual(u16, record["ushort"]);
            Assert.AreEqual(i32, record["int"]);
            Assert.AreEqual(u32, record["uint"]);
            Assert.AreEqual(i64, record["long"]);
            Assert.AreEqual(f, record["float"]);
            Assert.AreEqual(d, record["double"]);
            Assert.AreEqual(guid, record["guid"]);
            Assert.AreEqual(datetime, record["datetime"]);
            Assert.AreEqual(text, record["text"]);
            Assert.AreEqual(ascii, record["asciitext"]);
            CollectionAssert.AreEqual(binary, (byte[])record["binary"]);
        }

        /// <summary>
        /// Test setting and retrieving null values in the columns.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void TestNullColumns()
        {
            // Ideally these would be separate tests, but that is a lot
            // of typing and increases the test runtime.
            this.SetAndRetrieveNullValue("bool");
            this.SetAndRetrieveNullValue("byte");
            this.SetAndRetrieveNullValue("short");
            this.SetAndRetrieveNullValue("ushort");
            this.SetAndRetrieveNullValue("int");
            this.SetAndRetrieveNullValue("uint");
            this.SetAndRetrieveNullValue("long");
            this.SetAndRetrieveNullValue("float");
            this.SetAndRetrieveNullValue("double");
            this.SetAndRetrieveNullValue("datetime");
            this.SetAndRetrieveNullValue("guid");
            this.SetAndRetrieveNullValue("binary");
            this.SetAndRetrieveNullValue("text");
            this.SetAndRetrieveNullValue("asciitext");
        }

        /// <summary>
        /// Set and retrieve a null value in a column.
        /// </summary>
        /// <param name="column">The column to set.</param>
        private void SetAndRetrieveNullValue(string column)
        {
            Record r = this.table.NewRecord();
            r[column] = null;
            r.Save();

            Assert.IsNull(r[column]);
        }
    }
}
