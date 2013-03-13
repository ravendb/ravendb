//-----------------------------------------------------------------------
// <copyright file="HelpersTempTableFixtureTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the various Set/RetrieveColumn* methods and
    /// the helper methods that retrieve meta-data.
    /// </summary>
    [TestClass]
    public class HelpersTempTableFixtureTests
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
        /// A dictionary that maps column names to column ids.
        /// </summary>
        private IDictionary<string, JET_COLUMNID> columnidDict;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Fixture setup for HelpersTempTableFixtureTests")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            // turn off logging so initialization is faster
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.PageTempDBMin, SystemParameters.PageTempDBSmallest, null);
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);

            this.columnidDict = SetupHelper.CreateTempTableWithAllColumns(this.sesid, TempTableGrbit.Indexed, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Fixture cleanup for HelpersTempTableFixtureTests")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
            SetupHelper.CheckProcessForInstanceLeaks();
        }

        /// <summary>
        /// Verify that the test class has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that HelpersTempTableFixtureTests.Setup created the fixture properly")]
        public void VerifyFixtureSetup()
        {
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
            Assert.AreNotEqual(JET_TABLEID.Nil, this.tableid);
            Assert.IsNotNull(this.columnidDict);

            Assert.IsTrue(this.columnidDict.ContainsKey("boolean"));
            Assert.IsTrue(this.columnidDict.ContainsKey("byte"));
            Assert.IsTrue(this.columnidDict.ContainsKey("int16"));
            Assert.IsTrue(this.columnidDict.ContainsKey("int32"));
            Assert.IsTrue(this.columnidDict.ContainsKey("int64"));
            Assert.IsTrue(this.columnidDict.ContainsKey("float"));
            Assert.IsTrue(this.columnidDict.ContainsKey("double"));
            Assert.IsTrue(this.columnidDict.ContainsKey("binary"));
            Assert.IsTrue(this.columnidDict.ContainsKey("ascii"));
            Assert.IsTrue(this.columnidDict.ContainsKey("unicode"));
            Assert.IsTrue(this.columnidDict.ContainsKey("guid"));
            Assert.IsTrue(this.columnidDict.ContainsKey("datetime"));
            Assert.IsTrue(this.columnidDict.ContainsKey("uint16"));
            Assert.IsTrue(this.columnidDict.ContainsKey("uint32"));
            Assert.IsTrue(this.columnidDict.ContainsKey("uint64"));

            Assert.IsFalse(this.columnidDict.ContainsKey("nosuchcolumn"));
        }

        #endregion Setup/Teardown

        #region RetrieveColumn tests

        /// <summary>
        /// Verify that retrieving the size of a null column returns null.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that retrieving the size of a null column returns null")]
        public void VerifyNullColumnSizeIsNull()
        {
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            this.UpdateAndGotoBookmark();
            Assert.IsNull(Api.RetrieveColumnSize(this.sesid, this.tableid, this.columnidDict["Int32"]));
        }

        /// <summary>
        /// Check that retrieving the size of a zero-length column returns the amount of data
        /// in the column.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Check that retrieving the size of a zero-length column returns 0")]
        public void VerifyZeroLengthColumnSizeIsZero()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            this.InsertRecord(columnid, new byte[0]);
            Assert.AreEqual(0, Api.RetrieveColumnSize(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Check that retrieving the size of a column returns the amount of data
        /// in the column.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Check that retrieving the size of a column returns the amount of data in the column")]
        public void VerifyRetrieveColumnSizeReturnsColumnSize()
        {
            JET_COLUMNID columnid = this.columnidDict["Byte"];
            var b = new byte[] { 0x55 };
            this.InsertRecord(columnid, b);
            Assert.AreEqual(1, Api.RetrieveColumnSize(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a column that exceeds the cached buffer size used by RetrieveColumn.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Retrieve a column that exceeds the cached buffer size used by RetrieveColumn")]
        public void RetrieveLargeColumn()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var expected = new byte[16384];

            var random = new Random();
            random.NextBytes(expected);

            this.InsertRecord(columnid, expected);

            byte[] actual = Api.RetrieveColumn(this.sesid, this.tableid, columnid);
            CollectionAssert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Check that retrieving a null column returns null.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Check that retrieving a null column returns null")]
        public void RetrieveNullColumn()
        {
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            this.UpdateAndGotoBookmark();
            Assert.IsNull(Api.RetrieveColumn(this.sesid, this.tableid, this.columnidDict["Int32"]));
        }

        /// <summary>
        /// Retrieve a column as boolean.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as boolean")]
        public void RetrieveAsBoolean()
        {
            JET_COLUMNID columnid = this.columnidDict["Boolean"];
            bool value = Any.Boolean;
            this.InsertRecord(columnid, BitConverter.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsBoolean(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as boolean.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as boolean")]
        public void RetrieveNullAsBoolean()
        {
            this.NullColumnTest<bool>("Boolean", Api.RetrieveColumnAsBoolean);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a boolean column from the copy buffer")]
        public void RetrieveBooleanFromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsBoolean, Any.Boolean);
        }

        /// <summary>
        /// Retrieve a column as a byte.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a byte")]
        public void RetrieveAsByte()
        {
            JET_COLUMNID columnid = this.columnidDict["Byte"];
            var b = new byte[] { 0x55 };
            this.InsertRecord(columnid, b);
            Assert.AreEqual(b[0], Api.RetrieveColumnAsByte(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as byte.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as byte")]
        public void RetrieveNullAsByte()
        {
            this.NullColumnTest<byte>("Byte", Api.RetrieveColumnAsByte);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a byte column from the copy buffer")]
        public void RetrieveByteFromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsByte, Any.Byte);
        }

        /// <summary>
        /// Retrieve a column as a short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a short")]
        public void RetrieveAsInt16()
        {
            JET_COLUMNID columnid = this.columnidDict["Int16"];
            short value = Any.Int16;
            this.InsertRecord(columnid, BitConverter.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsInt16(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as a short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as a short")]
        public void RetrieveNullAsInt16()
        {
            this.NullColumnTest<short>("Int16", Api.RetrieveColumnAsInt16);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve an Int16 column from the copy buffer")]
        public void RetrieveInt16FromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsInt16, Any.Int16);
        }

        /// <summary>
        /// Verify retrieving a column as a short throws an exception when the column
        /// is too short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify retrieving a column as a short throws an exception when the column is too short")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void VerifyRetrieveAsInt16ThrowsExceptionWhenColumnIsTooShort()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var value = new byte[1];
            this.InsertRecord(columnid, value);
            Api.RetrieveColumnAsInt16(this.sesid, this.tableid, columnid);
        }

        /// <summary>
        /// Retrieve a column as a ushort.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a ushort")]
        public void RetrieveAsUInt16()
        {
            JET_COLUMNID columnid = this.columnidDict["UInt16"];
            ushort value = Any.UInt16;
            this.InsertRecord(columnid, BitConverter.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsUInt16(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as a ushort.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as a ushort")]
        public void RetrieveNullAsUInt16()
        {
            this.NullColumnTest<ushort>("UInt16", Api.RetrieveColumnAsUInt16);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a UInt16 column from the copy buffer")]
        public void RetrieveUInt16FromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsUInt16, Any.UInt16);
        }

        /// <summary>
        /// Verify retrieving a column as a ushort throws an exception when the column
        /// is too short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieving a byte as a ushort throws an exception when the column is too short")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void VerifyRetrieveAsUInt16ThrowsExceptionWhenColumnIsTooShort()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var value = new byte[1];
            this.InsertRecord(columnid, value);
            Api.RetrieveColumnAsUInt16(this.sesid, this.tableid, columnid);
        }

        /// <summary>
        /// Retrieve a column as an int.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as an int")]
        public void RetrieveAsInt32()
        {
            JET_COLUMNID columnid = this.columnidDict["Int32"];
            int value = Any.Int32;
            this.InsertRecord(columnid, BitConverter.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsInt32(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as an int.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as an int")]
        public void RetrieveNullAsInt32()
        {
            this.NullColumnTest<int>("Int32", Api.RetrieveColumnAsInt32);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve an Int32 column from the copy buffer")]
        public void RetrieveInt32FromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsInt32, Any.Int32);
        }

        /// <summary>
        /// Verify retrieving a column as an int throws an exception when the column
        /// is too short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify retrieving a column as an int throws an exception when the column is too short")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void VerifyRetrieveAsInt32ThrowsExceptionWhenColumnIsTooShort()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var value = new byte[1];
            this.InsertRecord(columnid, value);
            Api.RetrieveColumnAsInt32(this.sesid, this.tableid, columnid);
        }

        /// <summary>
        /// Retrieve a column as a uint.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a uint")]
        public void RetrieveAsUInt32()
        {
            JET_COLUMNID columnid = this.columnidDict["UInt32"];
            uint value = Any.UInt32;
            this.InsertRecord(columnid, BitConverter.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsUInt32(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as a uint.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as a uint")]
        public void RetrieveNullAsUInt32()
        {
            this.NullColumnTest<uint>("UInt32", Api.RetrieveColumnAsUInt32);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a UInt32 column from the copy buffer")]
        public void RetrieveUInt32FromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsUInt32, Any.UInt32);
        }

        /// <summary>
        /// Verify retrieving a column as a uint throws an exception when the column
        /// is too short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify retrieving a column as a uint throws an exception when the column is too short")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void VerifyRetrieveAsUInt32ThrowsExceptionWhenColumnIsTooShort()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var value = new byte[1];
            this.InsertRecord(columnid, value);
            Api.RetrieveColumnAsUInt32(this.sesid, this.tableid, columnid);
        }

        /// <summary>
        /// Retrieve a column as a long.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a long")]
        public void RetrieveAsInt64()
        {
            JET_COLUMNID columnid = this.columnidDict["Int64"];
            long value = Any.Int64;
            this.InsertRecord(columnid, BitConverter.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsInt64(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as a long.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as a long")]
        public void RetrieveNullAsInt64()
        {
            this.NullColumnTest<long>("Int64", Api.RetrieveColumnAsInt64);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve an Int64 column from the copy buffer")]
        public void RetrieveInt64FromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsInt64, Any.Int64);
        }

        /// <summary>
        /// Verify retrieving a column as a long throws an exception when the column
        /// is too short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify retrieving a column as a long throws an exception when the column is too short")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void VerifyRetrieveAsInt64ThrowsExceptionWhenColumnIsTooShort()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var value = new byte[1];
            this.InsertRecord(columnid, value);
            Api.RetrieveColumnAsInt64(this.sesid, this.tableid, columnid);
        }

        /// <summary>
        /// Retrieve a column as a ulong.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a ulong")]
        public void RetrieveAsUInt64()
        {
            JET_COLUMNID columnid = this.columnidDict["UInt64"];
            ulong value = Any.UInt64;
            this.InsertRecord(columnid, BitConverter.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsUInt64(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as a ulong.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as a ulong")]
        public void RetrieveNullAsUInt64()
        {
            this.NullColumnTest<ulong>("UInt64", Api.RetrieveColumnAsUInt64);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a UInt64 column from the copy buffer")]
        public void RetrieveUInt64FromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsUInt64, Any.UInt64);
        }

        /// <summary>
        /// Verify retrieving a column as a ulong throws an exception when the column
        /// is too short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify retrieving a column as a ulong throws an exception when the column is too short")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void VerifyRetrieveAsUInt64ThrowsExceptionWhenColumnIsTooShort()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var value = new byte[1];
            this.InsertRecord(columnid, value);
            Api.RetrieveColumnAsUInt64(this.sesid, this.tableid, columnid);
        }

        /// <summary>
        /// Retrieve a column as a float.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a float")]
        public void RetrieveAsFloat()
        {
            JET_COLUMNID columnid = this.columnidDict["Float"];
            float value = Any.Float;
            this.InsertRecord(columnid, BitConverter.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsFloat(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as a float.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as a float")]
        public void RetrieveNullAsFloat()
        {
            this.NullColumnTest<float>("float", Api.RetrieveColumnAsFloat);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a Float column from the copy buffer")]
        public void RetrieveFloatFromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsFloat, Any.Float);
        }

        /// <summary>
        /// Verify retrieving a column as a float throws an exception when the column
        /// is too short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify retrieving a column as a float throws an exception when the column is too short")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void VerifyRetrieveAsFloatThrowsExceptionWhenColumnIsTooShort()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var value = new byte[1];
            this.InsertRecord(columnid, value);
            Api.RetrieveColumnAsFloat(this.sesid, this.tableid, columnid);
        }

        /// <summary>
        /// Retrieve a column as a double.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a double")]
        public void RetrieveAsDouble()
        {
            JET_COLUMNID columnid = this.columnidDict["Double"];
            double value = Any.Double;
            this.InsertRecord(columnid, BitConverter.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsDouble(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as a double.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as a double")]
        public void RetrieveNullAsDouble()
        {
            this.NullColumnTest<double>("double", Api.RetrieveColumnAsDouble);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a Double column from the copy buffer")]
        public void RetrieveDoubleFromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsDouble, Any.Double);
        }

        /// <summary>
        /// Verify retrieving a column as a double throws an exception when the column
        /// is too short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify retrieving a column as a double throws an exception when the column is too short")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void VerifyRetrieveAsDoubleThrowsExceptionWhenColumnIsTooShort()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var value = new byte[1];
            this.InsertRecord(columnid, value);
            Api.RetrieveColumnAsDouble(this.sesid, this.tableid, columnid);
        }

        /// <summary>
        /// Retrieve a column as a Guid.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a Guid")]
        public void RetrieveAsGuid()
        {
            JET_COLUMNID columnid = this.columnidDict["Guid"];
            Guid value = Any.Guid;
            this.InsertRecord(columnid, value.ToByteArray());
            Assert.AreEqual(value, Api.RetrieveColumnAsGuid(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as a guid.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as a guid")]
        public void RetrieveNullAsGuid()
        {
            this.NullColumnTest<Guid>("Guid", Api.RetrieveColumnAsGuid);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a Guid column from the copy buffer")]
        public void RetrieveGuidFromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsGuid, Any.Guid);
        }

        /// <summary>
        /// Verify retrieving a column as a guid throws an exception when the column
        /// is too short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify retrieving a column as a guid throws an exception when the column is too short")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void VerifyRetrieveAsGuidThrowsExceptionWhenColumnIsTooShort()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var value = new byte[1];
            this.InsertRecord(columnid, value);
            Api.RetrieveColumnAsGuid(this.sesid, this.tableid, columnid);
        }

        /// <summary>
        /// Retrieve a column as a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a DateTime")]
        public void RetrieveAsDateTime()
        {
            JET_COLUMNID columnid = this.columnidDict["DateTime"];

            // The .NET DateTime class has more precision than ESENT can store so we can't use
            // a general time (e.g. DateTime.Now) here
            var value = new DateTime(2006, 09, 10, 4, 5, 6);
            this.InsertRecord(columnid, BitConverter.GetBytes(value.ToOADate()));
            Assert.AreEqual(value, Api.RetrieveColumnAsDateTime(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a column as a DateTime when the value is invalid (too small).
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a DateTime when the value is invalid (too small)")]
        public void RetrieveAsDateTimeReturnsMinWhenValueIsTooSmall()
        {
            JET_COLUMNID columnid = this.columnidDict["DateTime"];

            // MSDN says that the value must be a value between negative 657435.0 through positive 2958466.0
            this.InsertRecord(columnid, BitConverter.GetBytes(-657436.0));
            Assert.AreEqual(DateTime.MinValue, Api.RetrieveColumnAsDateTime(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a column as a DateTime when the value is invalid (too large).
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a DateTime when the value is invalid (too large)")]
        public void RetrieveAsDateTimeReturnsMaxWhenValueIsTooLarge()
        {
            JET_COLUMNID columnid = this.columnidDict["DateTime"];

            // MSDN says that the value must be a value between negative 657435.0 through positive 2958466.0
            this.InsertRecord(columnid, BitConverter.GetBytes(2958467.0));
            Assert.AreEqual(DateTime.MaxValue, Api.RetrieveColumnAsDateTime(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve a null column as a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as a DateTime")]
        public void RetrieveNullAsDateTime()
        {
            this.NullColumnTest<DateTime>("DateTime", Api.RetrieveColumnAsDateTime);
        }

        /// <summary>
        /// Retrieve a column. This makes sure the grbit passed to the
        /// RetrieveColumn helper is respected.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a DateTime column from the copy buffer")]
        public void RetrieveDateTimeFromCopyBuffer()
        {
            this.RetrieveFromCopyBufferTest(Api.SetColumn, Api.RetrieveColumnAsDateTime, Any.DateTime);
        }

        /// <summary>
        /// Verify retrieving a column as a DateTime throws an exception when the column
        /// is too short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify retrieving a column as a DateTime throws an exception when the column is too short")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void VerifyRetrieveAsDateTimeThrowsExceptionWhenColumnIsTooShort()
        {
            JET_COLUMNID columnid = this.columnidDict["Binary"];
            var value = new byte[1];
            this.InsertRecord(columnid, value);
            Api.RetrieveColumnAsDateTime(this.sesid, this.tableid, columnid);
        }

        /// <summary>
        /// Retrieve a column as ASCII.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as ASCII")]
        public void RetrieveAsAscii()
        {
            JET_COLUMNID columnid = this.columnidDict["ASCII"];
            string value = Any.String;
            this.InsertRecord(columnid, Encoding.ASCII.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.ASCII));
        }

        /// <summary>
        /// Retrieve a null column as ASCII.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as ASCII")]
        public void RetrieveNullAsAscii()
        {
            JET_COLUMNID columnid = this.columnidDict["ASCII"];
            this.InsertRecord(columnid, null);
            Assert.IsNull(Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.ASCII));
        }

        /// <summary>
        /// Retrieve an empty string as ASCII to make sure it is handled differently from a null column.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve an empty string as ASCII to make sure it is handled differently from a null column")]
        public void RetrieveEmptyStringAsAscii()
        {
            JET_COLUMNID columnid = this.columnidDict["ASCII"];
            string value = String.Empty;
            byte[] data = Encoding.ASCII.GetBytes(value);
            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.JetSetColumn(this.sesid, this.tableid, columnid, data, data.Length, SetColumnGrbit.ZeroLength, null);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Assert.AreEqual(value, Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.ASCII));
        }

        /// <summary>
        /// Retrieve an ASCII string that is too large for the cached retrieval buffer.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Retrieve an ASCII string that is too large for the cached retrieval buffer")]
        public void RetrieveExtremelyLargeASciiString()
        {
            JET_COLUMNID columnid = this.columnidDict["Ascii"];
            var value = new string('X', 1024 * 1024);
            this.InsertRecord(columnid, Encoding.ASCII.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.ASCII));
        }

        /// <summary>
        /// Retrieve a column as Unicode.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a Unicode (i.e. non ASCII) string")]
        public void RetrieveAsUnicode()
        {
            JET_COLUMNID columnid = this.columnidDict["Unicode"];

            // These characters include surrogate pairs. String.Length counts the
            // surrogate pairs.
            const string Expected = "☺ś𐐂𐐉𐐯𐑉𝓐𐒀";   
            byte[] data = Encoding.Unicode.GetBytes(Expected);
            this.InsertRecord(columnid, data);
            string actual = Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.Unicode);
            Assert.AreEqual(Expected, actual);
        }

        /// <summary>
        /// Retrieve a null column as Unicode.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a null column as Unicode")]
        public void RetrieveNullAsUnicode()
        {
            JET_COLUMNID columnid = this.columnidDict["Unicode"];
            this.InsertRecord(columnid, null);
            Assert.IsNull(Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.Unicode));
        }

        /// <summary>
        /// Retrieve an empty string as Unicode to make sure it is handled differently from a null column.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve an empty string as Unicode to make sure it is handled differently from a null column")]
        public void RetrieveEmptyStringAsUnicode()
        {
            JET_COLUMNID columnid = this.columnidDict["Unicode"];
            string value = String.Empty;
            byte[] data = Encoding.Unicode.GetBytes(value);
            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.JetSetColumn(this.sesid, this.tableid, columnid, data, data.Length, SetColumnGrbit.ZeroLength, null);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Assert.AreEqual(value, Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.Unicode));
        }

        /// <summary>
        /// Retrieve a string that is the size of the cached retrieval buffer.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Retrieve a string that is the size of the cached retrieval buffer")]
        public void RetrieveLargeString()
        {
            JET_COLUMNID columnid = this.columnidDict["Unicode"];
            var value = new string('X', 512);
            this.InsertRecord(columnid, Encoding.Unicode.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.Unicode));
        }

        /// <summary>
        /// Retrieve a string that is too large for the cached retrieval buffer.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Retrieve a string that is too large for the cached retrieval buffer")]
        public void RetrieveExtremelyLargeString()
        {
            JET_COLUMNID columnid = this.columnidDict["Unicode"];
            string value = "Ẁ𐐂𐐉𐐯𐑉𝓐€" + new string('ᵫ', 1024 * 1024);
            this.InsertRecord(columnid, Encoding.Unicode.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.Unicode));
        }

        /// <summary>
        /// Retrieve a column as a (unicode) string.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a column as a (unicode) string")]
        public void RetrieveAsString()
        {
            JET_COLUMNID columnid = this.columnidDict["Unicode"];
            string value = Any.String;
            this.InsertRecord(columnid, Encoding.Unicode.GetBytes(value));
            Assert.AreEqual(value, Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Retrieve an empty string to make sure it is handled differently from a null column.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Retrieve a string from the copy buffer")]
        public void RetrieveStringFromCopyBuffer()
        {
            // A binary column can hold any type
            JET_COLUMNID columnid = this.columnidDict["binary"];
            string expected = Any.String;

            using (var trx = new Transaction(this.sesid))
            {
                using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
                {
                    update.Save();
                }

                Assert.IsTrue(Api.TryMoveFirst(this.sesid, this.tableid));

                using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
                {
                    Api.SetColumn(this.sesid, this.tableid, columnid, expected, Encoding.Unicode);
                    Assert.AreEqual(null, Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.Unicode, RetrieveColumnGrbit.None));
                    Assert.AreEqual(expected, Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, Encoding.Unicode, RetrieveColumnGrbit.RetrieveCopy));
                    update.Cancel();
                }

                trx.Commit(CommitTransactionGrbit.None);
            }
        }

        #endregion RetrieveColumnAs tests

        #region SetColumn Tests

        /// <summary>
        /// Test setting a unicode column from a string.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a unicode column from a string")]
        public void SetUnicodeString()
        {
            JET_COLUMNID columnid = this.columnidDict["unicode"];
            string expected = Any.String;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected, Encoding.Unicode);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            
            string actual = Encoding.Unicode.GetString(Api.RetrieveColumn(this.sesid, this.tableid, columnid));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a unicode column from a string, using a grbit.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a unicode column from a string, using a grbit")]
        public void SetUnicodeStringGrbit()
        {
            JET_COLUMNID columnid = this.columnidDict["unicode"];
            string data = Any.String;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, data, Encoding.Unicode, SetColumnGrbit.None);
            Api.SetColumn(this.sesid, this.tableid, columnid, data, Encoding.Unicode, SetColumnGrbit.AppendLV);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            string actual = Encoding.Unicode.GetString(Api.RetrieveColumn(this.sesid, this.tableid, columnid));
            Assert.AreEqual(data + data, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a string.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a string")]
        public void SetColumnsWithString()
        {
            JET_COLUMNID columnid = this.columnidDict["unicode"];
            var value = Any.String;
            this.InsertRecordWithSetColumns(columnid, new StringColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting an ASCII column from a string.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting an ASCII column from a string")]
        public void SetASCIIString()
        {
            JET_COLUMNID columnid = this.columnidDict["ascii"];
            string expected = Any.String;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected, Encoding.ASCII);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            string actual = Encoding.ASCII.GetString(Api.RetrieveColumn(this.sesid, this.tableid, columnid));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a long ASCII column from a string.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test setting a long ASCII column from a string")]
        public void SetLongAsciiString()
        {
            JET_COLUMNID columnid = this.columnidDict["ascii"];
            string expected = Any.StringOfLength(1024 * 1024);

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected, Encoding.ASCII);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            string actual = Encoding.ASCII.GetString(Api.RetrieveColumn(this.sesid, this.tableid, columnid));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting and retrieving strings.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test setting and retrieving strings with boundary condition lengths")]
        public void SetAndRetrieveboundaryStrings()
        {
            // This is the size of the internal buffer we use to cache data on
            // set/retrieve. Use this to create boundary condition length strings.
            const int InternalBufferSize = 128 * 1024;

            this.SetAndRetrieveString("ascii", InternalBufferSize - 1, Encoding.ASCII);
            this.SetAndRetrieveString("ascii", InternalBufferSize, Encoding.ASCII);
            this.SetAndRetrieveString("ascii", InternalBufferSize + 1, Encoding.ASCII);

            this.SetAndRetrieveString("unicode", (InternalBufferSize / sizeof(char)) - 1, Encoding.Unicode);
            this.SetAndRetrieveString("unicode", InternalBufferSize / sizeof(char), Encoding.Unicode);
            this.SetAndRetrieveString("unicode", (InternalBufferSize / sizeof(char)) + 1, Encoding.Unicode);
        }

        /// <summary>
        /// Test setting and retrieving strings with a custom Unicode encoding.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving strings with custom unicode encoding")]
        public void SetAndRetrieveStringsCustomUnicode()
        {
            this.SetAndRetrieveString("unicode", 16, new UnicodeEncoding());
        }

        /// <summary>
        /// Test setting and retrieving strings with a custom ASCII encoding.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving strings with custom ASCII encoding")]
        public void SetAndRetrieveStringsCustomAscii()
        {
            this.SetAndRetrieveString("ASCII", 16, new ASCIIEncoding());
        }

        /// <summary>
        /// Verify using an encoding which is neither ASCII nor Unicode throws an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify using an encoding which is neither ASCII nor Unicode throws an exception")]
        public void VerifySetStringWithInvalidEncodingThrowsException()
        {
            JET_COLUMNID columnid = this.columnidDict["unicode"];

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);

            try
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Any.String, Encoding.UTF8);
                Assert.Fail("Expected an ESENT exception");
            }
            catch (ArgumentOutOfRangeException)
            {
            }
        }

        /// <summary>
        /// Test setting a column from an empty string.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from an empty string")]
        public void SetEmptyString()
        {
            JET_COLUMNID columnid = this.columnidDict["unicode"];
            string expected = string.Empty;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected, Encoding.Unicode);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            string actual = Encoding.Unicode.GetString(Api.RetrieveColumn(this.sesid, this.tableid, columnid));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a column from a null string.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from a null string")]
        public void SetNullString()
        {
            JET_COLUMNID columnid = this.columnidDict["unicode"];

            this.InsertRecord(columnid, Encoding.Unicode.GetBytes(Any.String));

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Replace);
            Api.SetColumn(this.sesid, this.tableid, columnid, null, Encoding.Unicode);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Assert.IsNull(Api.RetrieveColumn(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from true.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from true")]
        public void SetBooleanTrue()
        {
            JET_COLUMNID columnid = this.columnidDict["boolean"];
            bool expected = true;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            bool actual = BitConverter.ToBoolean(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a column from false.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from false")]
        public void SetBooleanFalse()
        {
            JET_COLUMNID columnid = this.columnidDict["boolean"];
            bool expected = false;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            bool actual = BitConverter.ToBoolean(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a boolean.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a boolean")]
        public void SetColumnsWithBoolean()
        {
            JET_COLUMNID columnid = this.columnidDict["Boolean"];
            bool value = Any.Boolean;
            this.InsertRecordWithSetColumns(columnid, new BoolColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsBoolean(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test SetColumn with a byte.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test SetColumn with a byte")]
        public void SetByte()
        {
            JET_COLUMNID columnid = this.columnidDict["byte"];
            byte expected = Any.Byte;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            byte actual = Api.RetrieveColumn(this.sesid, this.tableid, columnid)[0];
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a byte.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a byte")]
        public void SetColumnsWithByte()
        {
            JET_COLUMNID columnid = this.columnidDict["byte"];
            var value = Any.Byte;
            this.InsertRecordWithSetColumns(columnid, new ByteColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsByte(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from a short.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from a short")]
        public void SetInt16()
        {
            JET_COLUMNID columnid = this.columnidDict["int16"];
            short expected = Any.Int16;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            short actual = BitConverter.ToInt16(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with an Int16.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with an Int16")]
        public void SetColumnsWithInt16()
        {
            JET_COLUMNID columnid = this.columnidDict["int16"];
            var value = Any.Int16;
            this.InsertRecordWithSetColumns(columnid, new Int16ColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsInt16(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from an int.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from an int")]
        public void SetInt32()
        {
            JET_COLUMNID columnid = this.columnidDict["int32"];
            int expected = Any.Int32;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            int actual = BitConverter.ToInt32(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with an Int32.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with an Int32")]
        public void SetColumnsWithInt32()
        {
            JET_COLUMNID columnid = this.columnidDict["int32"];
            var value = Any.Int32;
            this.InsertRecordWithSetColumns(columnid, new Int32ColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsInt32(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from a long.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from a long")]
        public void SetInt64()
        {
            JET_COLUMNID columnid = this.columnidDict["int64"];
            long expected = Any.Int64;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            long actual = BitConverter.ToInt64(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with an Int64.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with an Int64")]
        public void SetColumnsWithInt64()
        {
            JET_COLUMNID columnid = this.columnidDict["int64"];
            var value = Any.Int64;
            this.InsertRecordWithSetColumns(columnid, new Int64ColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsInt64(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from a ushort.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from a ushort")]
        public void SetUInt16()
        {
            JET_COLUMNID columnid = this.columnidDict["uint16"];
            ushort expected = Any.UInt16;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            ushort actual = BitConverter.ToUInt16(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a UInt16.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a UInt16")]
        public void SetColumnsWithUInt16()
        {
            JET_COLUMNID columnid = this.columnidDict["uint16"];
            var value = Any.UInt16;
            this.InsertRecordWithSetColumns(columnid, new UInt16ColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsUInt16(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from a uint.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from a uint")]
        public void SetUInt32()
        {
            JET_COLUMNID columnid = this.columnidDict["uint32"];
            uint expected = Any.UInt32;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            uint actual = BitConverter.ToUInt32(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a UInt32.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a UInt32")]
        public void SetColumnsWithUInt32()
        {
            JET_COLUMNID columnid = this.columnidDict["uint32"];
            var value = Any.UInt32;
            this.InsertRecordWithSetColumns(columnid, new UInt32ColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsUInt32(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from a ulong.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from a ulong")]
        public void SetUInt64()
        {
            JET_COLUMNID columnid = this.columnidDict["uint64"];
            ulong expected = Any.UInt64;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            ulong actual = BitConverter.ToUInt64(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a UInt64.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a UInt64")]
        public void SetColumnsWithUInt64()
        {
            JET_COLUMNID columnid = this.columnidDict["uint64"];
            var value = Any.UInt64;
            this.InsertRecordWithSetColumns(columnid, new UInt64ColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsUInt64(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from a float.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from a float")]
        public void SetFloat()
        {
            JET_COLUMNID columnid = this.columnidDict["float"];
            float expected = Any.Float;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            float actual = BitConverter.ToSingle(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a Float.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a Float")]
        public void SetColumnsWithFloat()
        {
            JET_COLUMNID columnid = this.columnidDict["Float"];
            var value = Any.Float;
            this.InsertRecordWithSetColumns(columnid, new FloatColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsFloat(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from a double.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from a double")]
        public void SetDouble()
        {
            JET_COLUMNID columnid = this.columnidDict["double"];
            double expected = Any.Double;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            double actual = BitConverter.ToDouble(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a Double.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a Double")]
        public void SetColumnsWithDouble()
        {
            JET_COLUMNID columnid = this.columnidDict["Double"];
            var value = Any.Double;
            this.InsertRecordWithSetColumns(columnid, new DoubleColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsDouble(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from a guid.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from a guid")]
        public void SetGuid()
        {
            JET_COLUMNID columnid = this.columnidDict["guid"];
            Guid expected = Any.Guid;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            var actual = new Guid(Api.RetrieveColumn(this.sesid, this.tableid, columnid));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a Guid.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a Guid")]
        public void SetColumnsWithGuid()
        {
            JET_COLUMNID columnid = this.columnidDict["Guid"];
            var value = Any.Guid;
            this.InsertRecordWithSetColumns(columnid, new GuidColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsGuid(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from a DateTime")]
        public void SetDateTime()
        {
            JET_COLUMNID columnid = this.columnidDict["DateTime"];
            var expected = new DateTime(1956, 01, 02, 13, 2, 59);

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            DateTime actual = DateTime.FromOADate(BitConverter.ToDouble(Api.RetrieveColumn(this.sesid, this.tableid, columnid), 0));
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a DateTime")]
        public void SetColumnsWithDateTime()
        {
            JET_COLUMNID columnid = this.columnidDict["DateTime"];
            var value = Any.DateTime;
            this.InsertRecordWithSetColumns(columnid, new DateTimeColumnValue { Columnid = columnid, Value = value });
            Assert.AreEqual(value, Api.RetrieveColumnAsDateTime(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a column from an array of bytes using a grbit.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from an array of bytes using a grbit")]
        public void SetBytesWithGrbit()
        {
            JET_COLUMNID columnid = this.columnidDict["binary"];
            byte[] data = Any.Bytes;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, data, SetColumnGrbit.None);
            Api.SetColumn(this.sesid, this.tableid, columnid, data, SetColumnGrbit.AppendLV);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            byte[] expected = new byte[data.Length * 2];
            Array.Copy(data, 0, expected, 0, data.Length);
            Array.Copy(data, 0, expected, data.Length, data.Length);
            byte[] actual = Api.RetrieveColumn(this.sesid, this.tableid, columnid);
            CollectionAssert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a column from an array of bytes.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a column from an array of bytes")]
        public void SetBytes()
        {
            JET_COLUMNID columnid = this.columnidDict["binary"];
            byte[] expected = Any.Bytes;

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            byte[] actual = Api.RetrieveColumn(this.sesid, this.tableid, columnid);
            CollectionAssert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test setting a ColumnValue with a byte array.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a ColumnValue with a byte array")]
        public void SetColumnsWithBytes()
        {
            JET_COLUMNID columnid = this.columnidDict["binary"];
            var value = Any.Bytes;
            this.InsertRecordWithSetColumns(columnid, new BytesColumnValue { Columnid = columnid, Value = value });
            CollectionAssert.AreEqual(value, Api.RetrieveColumn(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a binary column from a zero-length array.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a binary column from a zero-length array")]
        public void SetZeroLengthBytes()
        {
            JET_COLUMNID columnid = this.columnidDict["binary"];

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, new byte[0]);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Assert.AreEqual(0, Api.RetrieveColumn(this.sesid, this.tableid, columnid).Length);
        }

        /// <summary>
        /// Test setting a binary column from a zero-length array with grbits.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a binary column from a zero-length array using a grbit")]
        public void SetZeroLengthBytesWithGrbit()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                Assert.Inconclusive("Incorrectly returns null");
            }

            JET_COLUMNID columnid = this.columnidDict["binary"];

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, new byte[0], SetColumnGrbit.IntrinsicLV);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Assert.AreEqual(0, Api.RetrieveColumn(this.sesid, this.tableid, columnid).Length);
        }

        /// <summary>
        /// Test setting a binary column from a null object.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a binary column from a null object")]
        public void SetNullBytes()
        {
            JET_COLUMNID columnid = this.columnidDict["binary"];

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, null);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);

            Assert.IsNull(Api.RetrieveColumn(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the min value of a Byte.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the min value of a Byte")]
        public void SetAndRetrieveByteMin()
        {
            var columnid = this.columnidDict["Byte"];
            const byte Expected = Byte.MinValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsByte(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the max value of a Byte.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the max value of a Byte")]
        public void SetAndRetrieveByteMax()
        {
            var columnid = this.columnidDict["Byte"];
            const byte Expected = Byte.MaxValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsByte(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the min value of an Int16.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the min value of an Int16")]
        public void SetAndRetrieveInt16Min()
        {
            var columnid = this.columnidDict["Int16"];
            const short Expected = Int16.MinValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsInt16(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the max value of an Int16.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the max value of an Int16")]
        public void SetAndRetrieveInt16Max()
        {
            var columnid = this.columnidDict["Int16"];
            const short Expected = Int16.MaxValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsInt16(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the min value of a UInt16.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the min value of a UInt16")]
        public void SetAndRetrieveUInt16Min()
        {
            var columnid = this.columnidDict["Binary"];
            const ushort Expected = UInt16.MinValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsUInt16(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the max value of a UInt16.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the max value of a UInt16")]
        public void SetAndRetrieveUInt16Max()
        {
            var columnid = this.columnidDict["Binary"];
            const ushort Expected = UInt16.MaxValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsUInt16(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the min value of an Int32.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the min value of an Int32")]
        public void SetAndRetrieveInt32Min()
        {
            var columnid = this.columnidDict["Int32"];
            const int Expected = Int32.MinValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsInt32(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the max value of an Int32.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the max value of an Int32")]
        public void SetAndRetrieveInt32Max()
        {
            var columnid = this.columnidDict["Int32"];
            const int Expected = Int32.MaxValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsInt32(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the min value of a UInt32.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the min value of a UInt32")]
        public void SetAndRetrieveUInt32Min()
        {
            var columnid = this.columnidDict["Binary"];
            const uint Expected = UInt32.MinValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsUInt32(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the max value of a UInt32.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the max value of a UInt32")]
        public void SetAndRetrieveUInt32Max()
        {
            var columnid = this.columnidDict["Binary"];
            const uint Expected = UInt32.MaxValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsUInt32(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the min value of an Int64.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the min value of an Int64")]
        public void SetAndRetrieveInt64Min()
        {
            var columnid = this.columnidDict["Int64"];
            const long Expected = Int64.MinValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsInt64(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the max value of an Int64.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the max value of an Int64")]
        public void SetAndRetrieveInt64Max()
        {
            var columnid = this.columnidDict["Int64"];
            const long Expected = Int64.MaxValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsInt64(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the min value of a UInt64.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the min value of a UInt64")]
        public void SetAndRetrieveUInt64Min()
        {
            var columnid = this.columnidDict["Binary"];
            const ulong Expected = UInt64.MinValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsUInt64(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the max value of a UInt64.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the max value of a UInt64")]
        public void SetAndRetrieveUInt64Max()
        {
            var columnid = this.columnidDict["Binary"];
            const ulong Expected = UInt64.MaxValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsUInt64(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the min value of a Float.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the min value of a Float")]
        public void SetAndRetrieveFloatMin()
        {
            var columnid = this.columnidDict["Float"];
            const float Expected = Single.MinValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsFloat(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the max value of a Float.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the max value of a Float")]
        public void SetAndRetrieveFloatMax()
        {
            var columnid = this.columnidDict["Float"];
            const float Expected = Single.MaxValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsFloat(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the min value of a Double.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the min value of a Double")]
        public void SetAndRetrieveDoubleMin()
        {
            var columnid = this.columnidDict["Double"];
            const double Expected = Double.MinValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsDouble(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the max value of a Double.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the max value of a Double")]
        public void SetAndRetrieveDoubleMax()
        {
            var columnid = this.columnidDict["Double"];
            const double Expected = Double.MaxValue;

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, Expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(Expected, Api.RetrieveColumnAsDouble(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting a date that is too small. Dates before Jan 1, year 100 can't
        /// be represented in ESENT (which uses the OLE Automation Date format), so this
        /// generates an exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting a date that is too small")]
        [ExpectedException(typeof(OverflowException))]
        public void SetDateTimeThrowsExceptionWhenDateIsTooSmall()
        {
            // Can't represent Dec 31, 99
            var invalid = new DateTime(99, 12, 31, 23, 59, 59);
            var columnid = this.columnidDict["Binary"];

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, invalid);
            }
        }

        /// <summary>
        /// This is a bit bizarre. DateTime.MinValue.ToOADate gives the OLE Automation
        /// base date, not the smallest possible OLE Automation Date or an overflow
        /// exception (either of which would seem to be more sensible).
        /// This test is documentating, but not endorsing, this behaviour.
        /// The MSDN documentation says that "An uninitialized DateTime, that is,
        /// an instance with a tick value of 0, is converted to the equivalent
        /// uninitialized OLE Automation Date, that is, a date with a value of
        /// 0.0 which represents midnight, 30 December 1899"
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting DateTime.Min")]
        public void SetDateTimeMinGivesOleAutomationBaseTime()
        {
            // The base OLE Automation Date is midnight, 30 December 1899
            var expected = new DateTime(1899, 12, 30, 0, 0, 0);

            var columnid = this.columnidDict["Binary"];

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, DateTime.MinValue);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(expected, Api.RetrieveColumnAsDateTime(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the min value of a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the min value of a DateTime")]
        public void SetAndRetrieveDateTimeOleAutomationMin()
        {
            // The earliest OLE Automation Date is Jan 1, year 100
            var expected = new DateTime(100, 1, 1, 00, 00, 1);

            var columnid = this.columnidDict["Binary"];

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(expected, Api.RetrieveColumnAsDateTime(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Test setting and retrieving the max value of a DateTime.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test setting and retrieving the max value of a DateTime")]
        public void SetAndRetrieveDateTimeMax()
        {
            // MSDN says the maximum OLE Automation Date is the same as
            // DateTime.MaxValue, the last moment of 31 December 9999.
            var expected = new DateTime(9999, 12, 31, 23, 59, 59);

            var columnid = this.columnidDict["Binary"];

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, columnid, expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.AreEqual(expected, Api.RetrieveColumnAsDateTime(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Serialize and deserialize null.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Serialize and deserialize null")]
        public void SerializeAndDeserializeNull()
        {
            var columnid = this.columnidDict["Binary"];

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SerializeObjectToColumn(this.sesid, this.tableid, columnid, null);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Assert.IsNull(Api.DeserializeObjectFromColumn(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Serialize and deserialize an object.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Serialize and deserialize an object")]
        public void SerializeAndDeserializeObject()
        {
            var columnid = this.columnidDict["Binary"];
            var expected = new List<double> { Math.PI, Math.E, Double.PositiveInfinity, Double.NegativeInfinity, Double.Epsilon };

            using (var trx = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SerializeObjectToColumn(this.sesid, this.tableid, columnid, expected);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            var actual = Api.DeserializeObjectFromColumn(this.sesid, this.tableid, columnid) as List<double>;
            CollectionAssert.AreEqual(expected, actual);
        }

        #endregion SetColumn Tests

        #region Helper methods

        /// <summary>
        /// Creates a record with the given column set to the specified value.
        /// The tableid is positioned on the new record.
        /// </summary>
        /// <param name="columnid">The column to set.</param>
        /// <param name="data">The data to set.</param>
        private void InsertRecord(JET_COLUMNID columnid, byte[] data)
        {
            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            SetColumnGrbit grbit = (null != data && 0 == data.Length) ? SetColumnGrbit.ZeroLength : SetColumnGrbit.None;
            Api.JetSetColumn(this.sesid, this.tableid, columnid, data, (null == data) ? 0 : data.Length, grbit, null);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);      
        }

        /// <summary>
        /// Creates a record with the given column set to the specified value.
        /// The tableid is positioned on the new record.
        /// </summary>
        /// <param name="columnid">The column to set.</param>
        /// <param name="values">The data to set.</param>
        private void InsertRecordWithSetColumns(JET_COLUMNID columnid, params ColumnValue[] values)
        {
            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumns(this.sesid, this.tableid, values);
            this.UpdateAndGotoBookmark();
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
        }

        /// <summary>
        /// Test setting and retrieving a null column.
        /// </summary>
        /// <typeparam name="T">The struct type that is being returned.</typeparam>
        /// <param name="column">The name of the column to set.</param>
        /// <param name="retrieveFunc">The function to use when retrieving the column.</param>
        private void NullColumnTest<T>(string column, Func<JET_SESID, JET_TABLEID, JET_COLUMNID, T?> retrieveFunc) where T : struct
        {
            JET_COLUMNID columnid = this.columnidDict[column];
            this.InsertRecord(columnid, null);
            Assert.IsNull(retrieveFunc(this.sesid, this.tableid, columnid));
        }

        /// <summary>
        /// Set a column and retrieve it from the copy buffer. This makes sure the grbit is respected.
        /// </summary>
        /// <typeparam name="T">The (struct) value to set and get.</typeparam>
        /// <param name="setcolumn">A method to set the column.</param>
        /// <param name="retrievecolumn">A method to retrieve the column.</param>
        /// <param name="value">The value to set.</param>
        private void RetrieveFromCopyBufferTest<T>(
            Action<JET_SESID, JET_TABLEID, JET_COLUMNID, T> setcolumn,
            Func<JET_SESID, JET_TABLEID, JET_COLUMNID, RetrieveColumnGrbit, T?> retrievecolumn,
            T value) where T : struct
        {
            // A binary column can hold any type
            JET_COLUMNID columnid = this.columnidDict["binary"];

            using (var trx = new Transaction(this.sesid))
            {
                using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
                {
                    update.Save();
                }

                Assert.IsTrue(Api.TryMoveFirst(this.sesid, this.tableid));

                using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
                {
                    setcolumn(this.sesid, this.tableid, columnid, value);
                    Assert.AreEqual(null, retrievecolumn(this.sesid, this.tableid, columnid, RetrieveColumnGrbit.None));
                    Assert.AreEqual(value, retrievecolumn(this.sesid, this.tableid, columnid, RetrieveColumnGrbit.RetrieveCopy));
                    update.Cancel();
                }

                trx.Commit(CommitTransactionGrbit.None);
            }
        }

        /// <summary>
        /// Update the cursor and goto the returned bookmark.
        /// </summary>
        private void UpdateAndGotoBookmark()
        {
            var bookmark = new byte[256];
            int bookmarkSize;
            Api.JetUpdate(this.sesid, this.tableid, bookmark, bookmark.Length, out bookmarkSize);
            Api.JetGotoBookmark(this.sesid, this.tableid, bookmark, bookmarkSize);
        }

        /// <summary>
        /// Set a string and the retrieve it.
        /// </summary>
        /// <param name="columnName">The name of the column to set.</param>
        /// <param name="numChars">The number of characters in the string to set.</param>
        /// <param name="encoding">The encoding to use when setting/retrieving the string.</param>
        private void SetAndRetrieveString(string columnName, int numChars, Encoding encoding)
        {
            JET_COLUMNID columnid = this.columnidDict[columnName];
            string expected = Any.StringOfLength(numChars);

            Api.JetBeginTransaction(this.sesid);
            Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
            Api.SetColumn(this.sesid, this.tableid, columnid, expected, encoding);
            this.UpdateAndGotoBookmark();

            string actual = Api.RetrieveColumnAsString(this.sesid, this.tableid, columnid, encoding);
            Assert.AreEqual(expected, actual, "RetrieveColumnAsString");
            actual = encoding.GetString(Api.RetrieveColumn(this.sesid, this.tableid, columnid));
            Assert.AreEqual(expected, actual, "RetrieveColumn");

            Api.JetRollback(this.sesid, RollbackTransactionGrbit.None);
        }

        #endregion Helper methods
    }
}
