//-----------------------------------------------------------------------
// <copyright file="MakeKeyHelpersTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the MakeKey() helper methods.
    /// </summary>
    [TestClass]
    public class MakeKeyHelpersTests
    {
        /// <summary>
        /// The directory being used for the database and its files.
        /// </summary>
        private string directory;

        /// <summary>
        /// The instance used by the test.
        /// </summary>
        private JET_INSTANCE instance;

        /// <summary>
        /// The session used by the test.
        /// </summary>
        private JET_SESID sesid;

        /// <summary>
        /// The tableid being used by the test.
        /// </summary>
        private JET_TABLEID tableid;

        /// <summary>
        /// The columnid of the column in the table.
        /// </summary>
        private JET_COLUMNID columnid;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Fixture setup for HelpersTests")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            // turn off logging so initialization is faster
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            this.tableid = JET_TABLEID.Nil;
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Fixture cleanup for HelpersTests")]
        public void Teardown()
        {
            if (JET_TABLEID.Nil != this.tableid)
            {
                Api.JetCloseTable(this.sesid, this.tableid);
            }

            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        /// <summary>
        /// Verify that the HelpersTests.Setup has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that the HelpersTests.Setup has setup the test fixture properly.")]
        public void VerifyFixtureSetup()
        {
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
            Assert.AreEqual(JET_TABLEID.Nil, this.tableid);
            Assert.IsNotNull(SetupHelper.ColumndefDictionary);

            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("boolean"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("byte"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("int16"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("int32"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("int64"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("float"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("double"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("binary"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("ascii"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("unicode"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("guid"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("datetime"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("uint16"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("uint32"));
            Assert.IsTrue(SetupHelper.ColumndefDictionary.ContainsKey("uint64"));

            Assert.IsFalse(SetupHelper.ColumndefDictionary.ContainsKey("nosuchcolumn"));
        }

        #endregion Setup/Teardown

        #region MakeKey Tests

        /// <summary>
        /// Test making a key from true.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from true.")]
        public void MakeKeyBooleanTrue()
        {
            this.MakeKeyBooleanHelper(true);
        }

        /// <summary>
        /// Test making a key from false.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from false.")]
        public void MakeKeyBooleanFalse()
        {
            this.MakeKeyBooleanHelper(false);
        }

        /// <summary>
        /// Test making a key from a byte.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a byte.")]
        public void MakeKeyByte()
        {
            byte expected = Any.Byte;
            byte[] expectedData = new[] { expected };
            this.CreateTempTableOnColumn("byte");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a short.")]
        public void MakeKeyInt16()
        {
            var expected = Any.Int16;
            var expectedData = BitConverter.GetBytes(expected);
            this.CreateTempTableOnColumn("int16");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a ushort.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a ushort.")]
        public void MakeKeyUInt16()
        {
            var expected = Any.UInt16;
            var expectedData = BitConverter.GetBytes(expected);
            this.CreateTempTableOnColumn("uint16");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from an int.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from an int.")]
        public void MakeKeyInt32()
        {
            var expected = Any.Int32;
            var expectedData = BitConverter.GetBytes(expected);
            this.CreateTempTableOnColumn("int32");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a uint.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a uint.")]
        public void MakeKeyUInt32()
        {
            var expected = Any.UInt32;
            var expectedData = BitConverter.GetBytes(expected);
            this.CreateTempTableOnColumn("uint32");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a long.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a long.")]
        public void MakeKeyInt64()
        {
            var expected = Any.Int64;
            var expectedData = BitConverter.GetBytes(expected);
            this.CreateTempTableOnColumn("int64");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a ulong.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a ulong.")]
        public void MakeKeyUInt64()
        {
            var expected = Any.UInt64;
            var expectedData = BitConverter.GetBytes(expected);
            this.CreateTempTableOnColumn("uint64");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a float.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a float.")]
        public void MakeKeyFloat()
        {
            var expected = Any.Float;
            var expectedData = BitConverter.GetBytes(expected);
            this.CreateTempTableOnColumn("float");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a double.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a double.")]
        public void MakeKeyDouble()
        {
            var expected = Any.Double;
            var expectedData = BitConverter.GetBytes(expected);
            this.CreateTempTableOnColumn("double");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a guid.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a guid.")]
        public void MakeKeyGuid()
        {
            var expected = Guid.NewGuid();
            var expectedData = expected.ToByteArray();
            this.CreateTempTableOnColumn("guid");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a DateTime.")]
        public void MakeKeyDateTime()
        {
            var expected = Any.DateTime;
            var expectedData = BitConverter.GetBytes(expected.ToOADate());
            this.CreateTempTableOnColumn("datetime");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a unicode string.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a unicode string.")]
        public void MakeKeyUnicode()
        {
            var expected = Any.String;
            var expectedData = Encoding.Unicode.GetBytes(expected);
            this.CreateTempTableOnColumn("unicode");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, Encoding.Unicode, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from an ASCII string.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from an ASCII string.")]
        public void MakeKeyASCII()
        {
            var expected = Any.String;
            var expectedData = Encoding.ASCII.GetBytes(expected);
            this.CreateTempTableOnColumn("ascii");
            this.InsertRecord(expectedData);
            Api.MakeKey(this.sesid, this.tableid, expected, Encoding.ASCII, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Verify making a key with an invalid encoding throws an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify making a key with an invalid encoding throws an exception.")]
        public void VerifyMakeKeyWithInvalidEncodingThrowsException()
        {
            this.CreateTempTableOnColumn("unicode");

            try
            {
                Api.MakeKey(this.sesid, this.tableid, Any.String, Encoding.UTF32, MakeKeyGrbit.NewKey);
                Assert.Fail("Expected an EsentException");
            }
            catch (ArgumentOutOfRangeException)
            {
            }
        }

        /// <summary>
        /// Test making a key from an empty string.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from an empty string.")]
        public void MakeKeyEmptyString()
        {
            var expected = String.Empty;
            var expectedData = Encoding.Unicode.GetBytes(expected);
            this.CreateTempTableOnColumn("unicode");
            this.InsertZeroLengthData();
            Api.MakeKey(this.sesid, this.tableid, expected, Encoding.Unicode, MakeKeyGrbit.NewKey);
            this.TestSeek(expectedData);
        }

        /// <summary>
        /// Test making a key from a null string.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a null string.")]
        public void MakeKeyNullString()
        {
            this.CreateTempTableOnColumn("unicode");
            this.InsertRecord(null);
            Api.MakeKey(this.sesid, this.tableid, null, Encoding.Unicode, MakeKeyGrbit.NewKey);
            this.TestSeek(null);
        }

        /// <summary>
        /// Test making a key from an array of bytes.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from an array of bytes.")]
        public void MakeKeyBinary()
        {
            var expected = Any.Bytes;
            this.CreateTempTableOnColumn("binary");
            this.InsertRecord(expected);
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expected);
        }

        /// <summary>
        /// Test making a key from a zero-length array of bytes.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a zero-length array of bytes.")]
        public void MakeKeyZeroLengthBinary()
        {
            var expected = new byte[0];
            this.CreateTempTableOnColumn("binary");
            this.InsertZeroLengthData();
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            this.TestSeek(expected);
        }

        /// <summary>
        /// Test making a key from a null array of bytes.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test making a key from a null array of bytes.")]
        public void MakeKeyNullBinary()
        {
            this.CreateTempTableOnColumn("binary");
            this.InsertRecord(null);
            Api.MakeKey(this.sesid, this.tableid, null, MakeKeyGrbit.NewKey);
            this.TestSeek(null);
        }

        #endregion MakeKey Tests

        #region Helper methods

        /// <summary>
        /// Test making a key from a boolean.
        /// </summary>
        /// <param name="expected">The expected value.</param>
        private void MakeKeyBooleanHelper(bool expected)
        {
            this.CreateTempTableOnColumn("boolean");
            this.InsertRecord(BitConverter.GetBytes(expected));
            Api.MakeKey(this.sesid, this.tableid, expected, MakeKeyGrbit.NewKey);
            Assert.IsTrue(Api.TrySeek(this.sesid, this.tableid, SeekGrbit.SeekEQ));
            bool actual = (bool)Api.RetrieveColumnAsBoolean(this.sesid, this.tableid, this.columnid);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Create a temporary table with one indexed column on the given type.
        /// </summary>
        /// <param name="column">Name of the column. Used to look up the column definition</param>
        private void CreateTempTableOnColumn(string column)
        {
            var columns = new[] { SetupHelper.ColumndefDictionary[column] };
            columns[0].grbit |= ColumndefGrbit.TTKey;
            var columnids = new JET_COLUMNID[1];

            Api.JetOpenTempTable(this.sesid, columns, columns.Length, TempTableGrbit.Indexed, out this.tableid, columnids);
            this.columnid = columnids[0];            
        }

        /// <summary>
        /// Creates a record with the column set to the specified value.
        /// The tableid is positioned on the new record.
        /// </summary>
        /// <param name="data">The data to set.</param>
        private void InsertRecord(byte[] data)
        {
            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, data, (null == data) ? 0 : data.Length, SetColumnGrbit.None, null);
            Api.JetUpdate(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);      
        }

        /// <summary>
        /// Creates a record with the column set to zero-length data.
        /// </summary>
        private void InsertZeroLengthData()
        {
            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.JetSetColumn(this.sesid, this.tableid, this.columnid, null, 0, SetColumnGrbit.ZeroLength, null);
            Api.JetUpdate(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }

        /// <summary>
        /// Seek to a record and make sure the expected data is returned.
        /// </summary>
        /// <param name="expectedData">The expected data.</param>
        private void TestSeek(byte[] expectedData)
        {
            Assert.IsTrue(Api.TrySeek(this.sesid, this.tableid, SeekGrbit.SeekEQ));
            byte[] actualData = Api.RetrieveColumn(this.sesid, this.tableid, this.columnid);
            CollectionAssert.AreEqual(expectedData, actualData);
        }

        #endregion Helper methods
    }
}
