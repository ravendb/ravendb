//-----------------------------------------------------------------------
// <copyright file="TempTableFixture.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Server2003;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests that use the temporary table fixture, which is very quick to setup.
    /// </summary>
    [TestClass]
    public class TempTableFixture
    {
        /// <summary>
        /// The instance used by the test.
        /// </summary>
        private Instance instance;

        /// <summary>
        /// The session used by the test.
        /// </summary>
        private Session session;

        /// <summary>
        /// The tableid being used by the test.
        /// </summary>
        private JET_TABLEID tableid;

        /// <summary>
        /// A dictionary that maps column type names to column ids.
        /// </summary>
        private IDictionary<string, JET_COLUMNID> columnDict;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup the TempTableFixture fixture")]
        public void Setup()
        {
            this.instance = new Instance(Guid.NewGuid().ToString(), "TempTableFixture");
            this.instance.Parameters.Recovery = false;
            this.instance.Parameters.PageTempDBMin = SystemParameters.PageTempDBSmallest;
            this.instance.Parameters.NoInformationEvent = true;
            this.instance.Init();

            this.session = new Session(this.instance);
            this.columnDict = SetupHelper.CreateTempTableWithAllColumns(this.session, TempTableGrbit.None, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the TempTableFixture fixture")]
        public void Teardown()
        {
            this.instance.Term();
            SetupHelper.CheckProcessForInstanceLeaks();
        }

        #endregion Setup/Teardown

        /// <summary>
        /// Test JetUpdate2.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test JetUpdate2")]
        public void TestJetUpdate2()
        {
            if (!EsentVersion.SupportsServer2003Features)
            {
                return;
            }

            JET_COLUMNID columnid = this.columnDict["Int32"];
            int value = Any.Int32;

            using (var trx = new Transaction(this.session))
            {
                Api.JetPrepareUpdate(this.session, this.tableid, JET_prep.Insert);
                Api.SetColumn(this.session, this.tableid, columnid, value);
                int ignored;
                Server2003Api.JetUpdate2(this.session, this.tableid, null, 0, out ignored, UpdateGrbit.None);
                trx.Commit(CommitTransactionGrbit.None);
            }

            Api.TryMoveFirst(this.session, this.tableid);
            Assert.AreEqual(value, Api.RetrieveColumnAsInt32(this.session, this.tableid, columnid));
        }

        /// <summary>
        /// Test GetBookmark
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test GetBookmark")]
        public void TestGetBookmark()
        {
            JET_COLUMNID columnid = this.columnDict["Key"];
            int value = Any.Int32;

            var bookmark = new byte[SystemParameters.BookmarkMost];
            int bookmarkSize;
            using (var trx = new Transaction(this.session))
            {
                Api.JetPrepareUpdate(this.session, this.tableid, JET_prep.Insert);
                Api.SetColumn(this.session, this.tableid, columnid, value);
                Api.JetUpdate(this.session, this.tableid, bookmark, bookmark.Length, out bookmarkSize);
                trx.Commit(CommitTransactionGrbit.None);
            }

            Api.TryMoveFirst(this.session, this.tableid);
            var expectedBookmark = new byte[bookmarkSize];
            Array.Copy(bookmark, expectedBookmark, bookmarkSize);
            byte[] actualBookmark = Api.GetBookmark(this.session, this.tableid);
            Assert.AreEqual(bookmarkSize, actualBookmark.Length);
            CollectionAssert.AreEqual(expectedBookmark, actualBookmark);
        }

        #region Set and Retrieve Columns

        /// <summary>
        /// Test JetSetColumns.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test JetSetColumns")]
        public void JetSetColumns()
        {
            byte b = Any.Byte;
            short s = Any.Int16;
            int i = Any.Int32;
            long l = Any.Int64;
            string str = Any.StringOfLength(256);
            float f = Any.Float;
            double d = Any.Double;
            byte[] data = Any.BytesOfLength(1023);

            var setcolumns = new[]
            {
                new JET_SETCOLUMN { cbData = sizeof(byte), columnid = this.columnDict["Byte"], pvData = new[] { (byte)0x0, b, (byte)0x1 }, ibData = 1 },
                new JET_SETCOLUMN { cbData = sizeof(short), columnid = this.columnDict["Int16"], pvData = BitConverter.GetBytes(s) },
                new JET_SETCOLUMN { cbData = sizeof(int), columnid = this.columnDict["Int32"], pvData = BitConverter.GetBytes(i) },
                new JET_SETCOLUMN { cbData = sizeof(long), columnid = this.columnDict["Int64"], pvData = BitConverter.GetBytes(l) },
                new JET_SETCOLUMN { cbData = sizeof(float), columnid = this.columnDict["Float"], pvData = BitConverter.GetBytes(f) },
                new JET_SETCOLUMN { cbData = sizeof(double), columnid = this.columnDict["Double"], pvData = BitConverter.GetBytes(d) },
                new JET_SETCOLUMN { cbData = (str.Length - 1) * sizeof(char), columnid = this.columnDict["Unicode"], pvData = Encoding.Unicode.GetBytes(str), ibData = sizeof(char) },
                new JET_SETCOLUMN { cbData = data.Length, columnid = this.columnDict["Binary"], pvData = data },
            };

            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.JetSetColumns(this.session, this.tableid, setcolumns, setcolumns.Length);
                update.Save();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Api.TryMoveFirst(this.session, this.tableid);

            Assert.AreEqual(b, Api.RetrieveColumnAsByte(this.session, this.tableid, this.columnDict["Byte"]));
            Assert.AreEqual(s, Api.RetrieveColumnAsInt16(this.session, this.tableid, this.columnDict["Int16"]));
            Assert.AreEqual(i, Api.RetrieveColumnAsInt32(this.session, this.tableid, this.columnDict["Int32"]));
            Assert.AreEqual(l, Api.RetrieveColumnAsInt64(this.session, this.tableid, this.columnDict["Int64"]));
            Assert.AreEqual(f, Api.RetrieveColumnAsFloat(this.session, this.tableid, this.columnDict["Float"]));
            Assert.AreEqual(d, Api.RetrieveColumnAsDouble(this.session, this.tableid, this.columnDict["Double"]));
            Assert.AreEqual(str.Substring(1), Api.RetrieveColumnAsString(this.session, this.tableid, this.columnDict["Unicode"]));
            CollectionAssert.AreEqual(data, Api.RetrieveColumn(this.session, this.tableid, this.columnDict["Binary"]));
        }

        /// <summary>
        /// Test JetRetrieveColumns.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test JetRetrieveColumns")]
        public void JetRetrieveColumns()
        {
            short s = Any.Int16;
            string str = Any.String;
            double d = Any.Double;

            var setcolumns = new[]
            {
                new JET_SETCOLUMN { cbData = sizeof(short), columnid = this.columnDict["Int16"], pvData = BitConverter.GetBytes(s) },
                new JET_SETCOLUMN { cbData = sizeof(double), columnid = this.columnDict["Double"], pvData = BitConverter.GetBytes(d) },
                new JET_SETCOLUMN { cbData = str.Length * sizeof(char), columnid = this.columnDict["Unicode"], pvData = Encoding.Unicode.GetBytes(str) },
                new JET_SETCOLUMN { cbData = 0, columnid = this.columnDict["Binary"], pvData = null },
            };

            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.JetSetColumns(this.session, this.tableid, setcolumns, setcolumns.Length);
                update.Save();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Api.TryMoveFirst(this.session, this.tableid);

            var retrievecolumns = new[]
            {
                new JET_RETRIEVECOLUMN { cbData = sizeof(short), columnid = this.columnDict["Int16"], pvData = new byte[sizeof(short) + 3], ibData = 3 },
                new JET_RETRIEVECOLUMN { cbData = sizeof(double), columnid = this.columnDict["Double"], pvData = new byte[sizeof(double) + 2], ibData = 1 },
                new JET_RETRIEVECOLUMN { cbData = str.Length * sizeof(char) * 2, columnid = this.columnDict["Unicode"], pvData = new byte[str.Length * sizeof(char) * 2] },
                new JET_RETRIEVECOLUMN { cbData = 10, columnid = this.columnDict["Binary"], pvData = new byte[10] },
            };

            for (int i = 0; i < retrievecolumns.Length; ++i)
            {
                retrievecolumns[i].itagSequence = 1;    
            }

            Api.JetRetrieveColumns(this.session, this.tableid, retrievecolumns, retrievecolumns.Length);

            // retrievecolumns[0] = short
            Assert.AreEqual(sizeof(short), retrievecolumns[0].cbActual);
            Assert.AreEqual(JET_wrn.Success, retrievecolumns[0].err);
            Assert.AreEqual(s, BitConverter.ToInt16(retrievecolumns[0].pvData, retrievecolumns[0].ibData));

            // retrievecolumns[1] = double
            Assert.AreEqual(sizeof(double), retrievecolumns[1].cbActual);
            Assert.AreEqual(JET_wrn.Success, retrievecolumns[1].err);
            Assert.AreEqual(d, BitConverter.ToDouble(retrievecolumns[1].pvData, retrievecolumns[1].ibData));

            // retrievecolumns[2] = string
            Assert.AreEqual(str.Length * sizeof(char), retrievecolumns[2].cbActual);
            Assert.AreEqual(JET_wrn.Success, retrievecolumns[2].err);
            Assert.AreEqual(str, Encoding.Unicode.GetString(retrievecolumns[2].pvData, retrievecolumns[2].ibData, retrievecolumns[2].cbActual));

            // retrievecolumns[3] = null
            Assert.AreEqual(0, retrievecolumns[3].cbActual);
            Assert.AreEqual(JET_wrn.ColumnNull, retrievecolumns[3].err);
        }

        /// <summary>
        /// Test JetRetrieveColumns, retrieving into the same buffer.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test JetRetrieveColumns with the same buffer")]
        public void JetRetrieveColumnsSameBuffer()
        {
            short s = Any.Int16;
            string str = Any.String;
            double d = Any.Double;

            var setcolumns = new[]
            {
                new JET_SETCOLUMN { cbData = sizeof(short), columnid = this.columnDict["Int16"], pvData = BitConverter.GetBytes(s) },
                new JET_SETCOLUMN { cbData = sizeof(double), columnid = this.columnDict["Double"], pvData = BitConverter.GetBytes(d) },
                new JET_SETCOLUMN { cbData = str.Length * sizeof(char), columnid = this.columnDict["Unicode"], pvData = Encoding.Unicode.GetBytes(str) },
                new JET_SETCOLUMN { cbData = 0, columnid = this.columnDict["Binary"], pvData = null },
            };

            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.JetSetColumns(this.session, this.tableid, setcolumns, setcolumns.Length);
                update.Save();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Api.TryMoveFirst(this.session, this.tableid);

            byte[] buffer = new byte[1024];
            var retrievecolumns = new[]
            {
                new JET_RETRIEVECOLUMN { cbData = sizeof(short), columnid = this.columnDict["Int16"], pvData = buffer, ibData = 100 },
                new JET_RETRIEVECOLUMN { cbData = sizeof(double), columnid = this.columnDict["Double"], pvData = buffer, ibData = 200 },
                new JET_RETRIEVECOLUMN { cbData = str.Length * sizeof(char) * 2, columnid = this.columnDict["Unicode"], pvData = new byte[str.Length * sizeof(char) * 2] },
                new JET_RETRIEVECOLUMN { cbData = 10, columnid = this.columnDict["Binary"], pvData = buffer },
            };

            for (int i = 0; i < retrievecolumns.Length; ++i)
            {
                retrievecolumns[i].itagSequence = 1;
            }

            Api.JetRetrieveColumns(this.session, this.tableid, retrievecolumns, retrievecolumns.Length);

            // retrievecolumns[0] = short
            Assert.AreEqual(sizeof(short), retrievecolumns[0].cbActual);
            Assert.AreEqual(JET_wrn.Success, retrievecolumns[0].err);
            Assert.AreEqual(s, BitConverter.ToInt16(retrievecolumns[0].pvData, retrievecolumns[0].ibData));

            // retrievecolumns[1] = double
            Assert.AreEqual(sizeof(double), retrievecolumns[1].cbActual);
            Assert.AreEqual(JET_wrn.Success, retrievecolumns[1].err);
            Assert.AreEqual(d, BitConverter.ToDouble(retrievecolumns[1].pvData, retrievecolumns[1].ibData));

            // retrievecolumns[2] = string
            Assert.AreEqual(str.Length * sizeof(char), retrievecolumns[2].cbActual);
            Assert.AreEqual(JET_wrn.Success, retrievecolumns[2].err);
            Assert.AreEqual(str, Encoding.Unicode.GetString(retrievecolumns[2].pvData, retrievecolumns[2].ibData, retrievecolumns[2].cbActual));

            // retrievecolumns[3] = null
            Assert.AreEqual(0, retrievecolumns[3].cbActual);
            Assert.AreEqual(JET_wrn.ColumnNull, retrievecolumns[3].err);
        }

        /// <summary>
        /// Test JetRetrieveColumns with a null buffer.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test JetRetrieveColumns with a null buffer")]
        public void JetRetrieveColumnsNullBuffer()
        {
            byte[] data = Any.Bytes;

            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.session, this.tableid, this.columnDict["Binary"], data);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            var retrievecolumns = new[]
            {
                new JET_RETRIEVECOLUMN { columnid = this.columnDict["Binary"], itagSequence = 1 },
            };

            Assert.AreEqual(
                JET_wrn.BufferTruncated,
                Api.JetRetrieveColumns(this.session, this.tableid, retrievecolumns, retrievecolumns.Length));

            Assert.AreEqual(data.Length, retrievecolumns[0].cbActual);
            Assert.AreEqual(JET_wrn.BufferTruncated, retrievecolumns[0].err);
        }

        /// <summary>
        /// Test SetColumns.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test SetColumns")]
        public void SetColumns()
        {
            bool bit = true;
            byte b = Any.Byte;
            short i16 = Any.Int16;
            ushort ui16 = Any.UInt16;
            int i32 = Any.Int32;
            uint ui32 = Any.UInt32;
            long i64 = Any.Int64;
            ulong ui64 = Any.UInt64;
            float f = Any.Float;
            double d = Any.Double;
            DateTime date = Any.DateTime;
            Guid guid = Any.Guid;
            string s = Any.String;
            byte[] bytes = Any.BytesOfLength(1023);

            var columnValues = new ColumnValue[]
            {
                new BoolColumnValue { Columnid = this.columnDict["Boolean"], Value = bit },
                new ByteColumnValue { Columnid = this.columnDict["Byte"], Value = b },
                new Int16ColumnValue { Columnid = this.columnDict["Int16"], Value = i16 },
                new UInt16ColumnValue { Columnid = this.columnDict["UInt16"], Value = ui16 },
                new Int32ColumnValue { Columnid = this.columnDict["Int32"], Value = i32 },
                new UInt32ColumnValue { Columnid = this.columnDict["UInt32"], Value = ui32 },
                new Int64ColumnValue { Columnid = this.columnDict["Int64"], Value = i64 },
                new UInt64ColumnValue { Columnid = this.columnDict["UInt64"], Value = ui64 },
                new FloatColumnValue { Columnid = this.columnDict["Float"], Value = f },
                new DoubleColumnValue { Columnid = this.columnDict["Double"], Value = d },
                new DateTimeColumnValue { Columnid = this.columnDict["DateTime"], Value = date },
                new GuidColumnValue() { Columnid = this.columnDict["Guid"], Value = guid },
                new StringColumnValue { Columnid = this.columnDict["Unicode"], Value = s },
                new BytesColumnValue { Columnid = this.columnDict["Binary"], Value = bytes },
            };

            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumns(this.session, this.tableid, columnValues);
                update.Save();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Api.TryMoveFirst(this.session, this.tableid);

            Assert.AreEqual(bit, Api.RetrieveColumnAsBoolean(this.session, this.tableid, this.columnDict["Boolean"]));
            Assert.AreEqual(b, Api.RetrieveColumnAsByte(this.session, this.tableid, this.columnDict["Byte"]));
            Assert.AreEqual(i16, Api.RetrieveColumnAsInt16(this.session, this.tableid, this.columnDict["Int16"]));
            Assert.AreEqual(ui16, Api.RetrieveColumnAsUInt16(this.session, this.tableid, this.columnDict["UInt16"]));
            Assert.AreEqual(i32, Api.RetrieveColumnAsInt32(this.session, this.tableid, this.columnDict["Int32"]));
            Assert.AreEqual(ui32, Api.RetrieveColumnAsUInt32(this.session, this.tableid, this.columnDict["UInt32"]));
            Assert.AreEqual(i64, Api.RetrieveColumnAsInt64(this.session, this.tableid, this.columnDict["Int64"]));
            Assert.AreEqual(ui64, Api.RetrieveColumnAsUInt64(this.session, this.tableid, this.columnDict["UInt64"]));
            Assert.AreEqual(f, Api.RetrieveColumnAsFloat(this.session, this.tableid, this.columnDict["Float"]));
            Assert.AreEqual(d, Api.RetrieveColumnAsDouble(this.session, this.tableid, this.columnDict["Double"]));
            Assert.AreEqual(date, Api.RetrieveColumnAsDateTime(this.session, this.tableid, this.columnDict["DateTime"]));
            Assert.AreEqual(guid, Api.RetrieveColumnAsGuid(this.session, this.tableid, this.columnDict["Guid"]));
            Assert.AreEqual(s, Api.RetrieveColumnAsString(this.session, this.tableid, this.columnDict["Unicode"]));
            CollectionAssert.AreEqual(bytes, Api.RetrieveColumn(this.session, this.tableid, this.columnDict["Binary"]));
        }

        /// <summary>
        /// Test SetColumns with null data.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test SetColumns with null data")]
        public void SetColumnsWithNull()
        {
            var columnValues = new ColumnValue[]
            {
                new BoolColumnValue { Columnid = this.columnDict["Boolean"], Value = null },
                new ByteColumnValue { Columnid = this.columnDict["Byte"], Value = null },
                new Int16ColumnValue { Columnid = this.columnDict["Int16"], Value = null },
                new Int32ColumnValue { Columnid = this.columnDict["Int32"], Value = null },
                new Int64ColumnValue { Columnid = this.columnDict["Int64"], Value = null },
                new FloatColumnValue { Columnid = this.columnDict["Float"], Value = null },
                new DoubleColumnValue { Columnid = this.columnDict["Double"], Value = null },
                new DateTimeColumnValue { Columnid = this.columnDict["DateTime"], Value = null },
                new StringColumnValue { Columnid = this.columnDict["Unicode"], Value = null },
                new BytesColumnValue { Columnid = this.columnDict["Binary"], Value = null },
            };

            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumns(this.session, this.tableid, columnValues);
                update.Save();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Api.TryMoveFirst(this.session, this.tableid);

            Assert.IsNull(Api.RetrieveColumnAsBoolean(this.session, this.tableid, this.columnDict["Boolean"]));
            Assert.IsNull(Api.RetrieveColumnAsByte(this.session, this.tableid, this.columnDict["Byte"]));
            Assert.IsNull(Api.RetrieveColumnAsInt16(this.session, this.tableid, this.columnDict["Int16"]));
            Assert.IsNull(Api.RetrieveColumnAsInt32(this.session, this.tableid, this.columnDict["Int32"]));
            Assert.IsNull(Api.RetrieveColumnAsInt64(this.session, this.tableid, this.columnDict["Int64"]));
            Assert.IsNull(Api.RetrieveColumnAsFloat(this.session, this.tableid, this.columnDict["Float"]));
            Assert.IsNull(Api.RetrieveColumnAsDouble(this.session, this.tableid, this.columnDict["Double"]));
            Assert.IsNull(Api.RetrieveColumnAsDateTime(this.session, this.tableid, this.columnDict["DateTime"]));
            Assert.IsNull(Api.RetrieveColumnAsString(this.session, this.tableid, this.columnDict["Unicode"]));
            Assert.IsNull(Api.RetrieveColumn(this.session, this.tableid, this.columnDict["Binary"]));
        }

        /// <summary>
        /// Test SetColumns with zero length data.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test SetColumns with zero length data")]
        public void SetColumnsWithZeroLength()
        {
            var columnValues = new ColumnValue[]
            {
                new StringColumnValue { Columnid = this.columnDict["Unicode"], Value = String.Empty },
                new BytesColumnValue { Columnid = this.columnDict["Binary"], Value = new byte[0] },
            };

            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumns(this.session, this.tableid, columnValues);
                update.Save();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Api.TryMoveFirst(this.session, this.tableid);

            Assert.AreEqual(String.Empty, Api.RetrieveColumnAsString(this.session, this.tableid, this.columnDict["Unicode"]));
            CollectionAssert.AreEqual(new byte[0], Api.RetrieveColumn(this.session, this.tableid, this.columnDict["Binary"]));
        }

        /// <summary>
        /// Test SetColumns with multivalues.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test SetColumns with multivalues")]
        public void SetColumnsWithMultivalues()
        {
            short expected = Any.Int16;

            var columnValues = new ColumnValue[]
            {
                new Int16ColumnValue() { Columnid = this.columnDict["Int16"], Value = 0 },
                new Int16ColumnValue() { Columnid = this.columnDict["Int16"], Value = expected, ItagSequence = 2 },
            };

            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumns(this.session, this.tableid, columnValues);
                update.Save();
                trx.Commit(CommitTransactionGrbit.None);
            }

            Api.TryMoveFirst(this.session, this.tableid);

            short actual = BitConverter.ToInt16(
                Api.RetrieveColumn(
                    this.session,
                    this.tableid,
                    this.columnDict["int16"],
                    RetrieveColumnGrbit.None,
                    new JET_RETINFO { itagSequence = 2 }),
                0);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Test RetrieveColumns.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test RetrieveColumns")]
        public void RetrieveColumns()
        {
            bool bit = Any.Boolean;
            byte b = Any.Byte;
            short i16 = Any.Int16;
            ushort ui16 = Any.UInt16;
            int i32 = Any.Int32;
            uint ui32 = Any.UInt32;
            long i64 = Any.Int64;
            ulong ui64 = Any.UInt64;
            float f = Any.Float;
            double d = Any.Double;
            DateTime date = Any.DateTime;
            Guid guid = Any.Guid;
            string str = Any.String;
            byte[] bytes = Any.BytesOfLength(1023);

            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.session, this.tableid, this.columnDict["Boolean"], bit);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Byte"], b);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Int16"], i16);
                Api.SetColumn(this.session, this.tableid, this.columnDict["UInt16"], ui16);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Int32"], i32);
                Api.SetColumn(this.session, this.tableid, this.columnDict["UInt32"], ui32);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Int64"], i64);
                Api.SetColumn(this.session, this.tableid, this.columnDict["UInt64"], ui64);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Float"], f);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Double"], d);
                Api.SetColumn(this.session, this.tableid, this.columnDict["DateTime"], date);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Guid"], guid);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Unicode"], str, Encoding.Unicode);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Binary"], bytes);

                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            var columnValues = new ColumnValue[]
            {
                new BoolColumnValue { Columnid = this.columnDict["Boolean"] },
                new ByteColumnValue { Columnid = this.columnDict["Byte"] },
                new Int16ColumnValue { Columnid = this.columnDict["Int16"] },
                new UInt16ColumnValue { Columnid = this.columnDict["UInt16"] },
                new Int32ColumnValue { Columnid = this.columnDict["Int32"] },
                new UInt32ColumnValue { Columnid = this.columnDict["UInt32"] },
                new Int64ColumnValue { Columnid = this.columnDict["Int64"] },
                new UInt64ColumnValue { Columnid = this.columnDict["UInt64"] },
                new FloatColumnValue { Columnid = this.columnDict["Float"] },
                new DoubleColumnValue { Columnid = this.columnDict["Double"] },
                new DateTimeColumnValue { Columnid = this.columnDict["DateTime"] },
                new GuidColumnValue { Columnid = this.columnDict["Guid"] },
                new StringColumnValue { Columnid = this.columnDict["Unicode"] },
                new BytesColumnValue { Columnid = this.columnDict["Binary"] },
            };

            Api.RetrieveColumns(this.session, this.tableid, columnValues);

            Assert.AreEqual(bit, columnValues[0].ValueAsObject);
            Assert.AreEqual(b, columnValues[1].ValueAsObject);
            Assert.AreEqual(i16, columnValues[2].ValueAsObject);
            Assert.AreEqual(ui16, columnValues[3].ValueAsObject);
            Assert.AreEqual(i32, columnValues[4].ValueAsObject);
            Assert.AreEqual(ui32, columnValues[5].ValueAsObject);
            Assert.AreEqual(i64, columnValues[6].ValueAsObject);
            Assert.AreEqual(ui64, columnValues[7].ValueAsObject);
            Assert.AreEqual(f, columnValues[8].ValueAsObject);
            Assert.AreEqual(d, columnValues[9].ValueAsObject);
            Assert.AreEqual(date, columnValues[10].ValueAsObject);
            Assert.AreEqual(guid, columnValues[11].ValueAsObject);
            Assert.AreEqual(str, columnValues[12].ValueAsObject);
            CollectionAssert.AreEqual(bytes, columnValues[13].ValueAsObject as byte[]);
        }

        /// <summary>
        /// Test RetrieveColumns with large values.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test RetrieveColumns with large values")]
        public void RetrieveColumnsWithLargeValues()
        {
            string str = Any.StringOfLength(129 * 1024);
            byte[] bytes = Any.BytesOfLength(127 * 1024);

            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.session, this.tableid, this.columnDict["Unicode"], str, Encoding.Unicode);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Binary"], bytes);

                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            var columnValues = new ColumnValue[]
            {
                new StringColumnValue { Columnid = this.columnDict["Unicode"] },
                new BytesColumnValue { Columnid = this.columnDict["Binary"] },
            };

            Api.RetrieveColumns(this.session, this.tableid, columnValues);

            Assert.AreEqual(str, columnValues[0].ValueAsObject);
            CollectionAssert.AreEqual(bytes, columnValues[1].ValueAsObject as byte[]);
        }

        /// <summary>
        /// Test RetrieveColumns with zero-length columns.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test RetrieveColumns with zero length columns")]
        public void RetrieveColumnsWithZeroLength()
        {
            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.session, this.tableid, this.columnDict["Unicode"], String.Empty, Encoding.Unicode);
                Api.SetColumn(this.session, this.tableid, this.columnDict["Binary"], new byte[0]);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            var columnValues = new ColumnValue[]
            {
                new StringColumnValue { Columnid = this.columnDict["Unicode"] },
                new BytesColumnValue { Columnid = this.columnDict["Binary"] },
            };

            Api.RetrieveColumns(this.session, this.tableid, columnValues);

            Assert.AreEqual(String.Empty, columnValues[0].ValueAsObject);
            CollectionAssert.AreEqual(new byte[0], columnValues[1].ValueAsObject as byte[]);
        }

        /// <summary>
        /// Test RetrieveColumns with null values.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test RetrieveColumns with nulls")]
        public void RetrieveColumnsWithNull()
        {
            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            var columnValues = new ColumnValue[]
            {
                new BoolColumnValue { Columnid = this.columnDict["Boolean"] },
                new ByteColumnValue { Columnid = this.columnDict["Byte"] },
                new Int16ColumnValue { Columnid = this.columnDict["Int16"] },
                new UInt16ColumnValue { Columnid = this.columnDict["UInt16"] },
                new Int32ColumnValue { Columnid = this.columnDict["Int32"] },
                new UInt32ColumnValue { Columnid = this.columnDict["UInt32"] },
                new Int64ColumnValue { Columnid = this.columnDict["Int64"] },
                new UInt64ColumnValue { Columnid = this.columnDict["UInt64"] },
                new FloatColumnValue { Columnid = this.columnDict["Float"] },
                new DoubleColumnValue { Columnid = this.columnDict["Double"] },
                new DateTimeColumnValue { Columnid = this.columnDict["DateTime"] },
                new GuidColumnValue { Columnid = this.columnDict["Guid"] },
                new StringColumnValue { Columnid = this.columnDict["Unicode"] },
                new BytesColumnValue { Columnid = this.columnDict["Binary"] },
            };

            Api.RetrieveColumns(this.session, this.tableid, columnValues);

            foreach (var c in columnValues)
            {
                Assert.IsNull(c.ValueAsObject);
            }
        }

        /// <summary>
        /// Test RetrieveColumns with an invalid column.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test RetrieveColumns with an invalid column")]
        [ExpectedException(typeof(EsentInvalidColumnException))]
        public void RetrieveColumnsWithInvalidColumn()
        {
            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.session, this.tableid, this.columnDict["Int64"], Any.Int64);
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            var columnValues = new ColumnValue[]
            {
                new GuidColumnValue { Columnid = this.columnDict["Int64"] },
            };

            Api.RetrieveColumns(this.session, this.tableid, columnValues);
        }

        /// <summary>
        /// Test RetrieveColumns with multivalues.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Test RetrieveColumns with multivalues")]
        public void RetrieveColumnsWithMultivalues()
        {
            using (var trx = new Transaction(this.session))
            using (var update = new Update(this.session, this.tableid, JET_prep.Insert))
            {
                Api.JetSetColumn(
                    this.session,
                    this.tableid,
                    this.columnDict["uint64"],
                    BitConverter.GetBytes(5UL),
                    sizeof(ulong),
                    SetColumnGrbit.None,
                    new JET_SETINFO { itagSequence = 1 });
                Api.JetSetColumn(
                    this.session,
                    this.tableid,
                    this.columnDict["uint64"],
                    BitConverter.GetBytes(99UL),
                    sizeof(ulong),
                    SetColumnGrbit.None,
                    new JET_SETINFO { itagSequence = 2 });
                update.SaveAndGotoBookmark();
                trx.Commit(CommitTransactionGrbit.None);
            }

            var columnValues = new ColumnValue[]
            {
                new UInt64ColumnValue { Columnid = this.columnDict["UInt64"], ItagSequence = 1 },
                new UInt64ColumnValue { Columnid = this.columnDict["UInt64"], ItagSequence = 2 },
            };

            Api.RetrieveColumns(this.session, this.tableid, columnValues);

            Assert.AreEqual(5UL, columnValues[0].ValueAsObject);
            Assert.AreEqual(99UL, columnValues[1].ValueAsObject);
        }

        #endregion
    }
}