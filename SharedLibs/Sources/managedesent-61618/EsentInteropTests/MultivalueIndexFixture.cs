//-----------------------------------------------------------------------
// <copyright file="MultivalueIndexFixture.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using System.Text;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test fixture that has a multi-valued column and index.
    /// </summary>
    [TestClass]
    public class MultivalueIndexFixture
    {
        /// <summary>
        /// The directory being used for the database and its files.
        /// </summary>
        private string directory;

        /// <summary>
        /// The path to the database being used by the test.
        /// </summary>
        private string database;

        /// <summary>
        /// The name of the table.
        /// </summary>
        private string table;

        /// <summary>
        /// The instance used by the test.
        /// </summary>
        private JET_INSTANCE instance;

        /// <summary>
        /// The session used by the test.
        /// </summary>
        private JET_SESID sesid;

        /// <summary>
        /// Identifies the database used by the test.
        /// </summary>
        private JET_DBID dbid;

        /// <summary>
        /// The tableid being used by the test.
        /// </summary>
        private JET_TABLEID tableid;

        /// <summary>
        /// Columnid of the key column in the table.
        /// </summary>
        private JET_COLUMNID keyColumn;

        /// <summary>
        /// Columnid of the multi-value column in the table.
        /// </summary>
        private JET_COLUMNID multiValueColumn;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup for MultivalueIndexFixture")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.table = "table";
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.MaxTemporaryTables, 0, null);
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            Api.JetCreateDatabase(this.sesid, this.database, String.Empty, out this.dbid, CreateDatabaseGrbit.None);
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTable(this.sesid, this.dbid, this.table, 0, 100, out this.tableid);

            var columndef = new JET_COLUMNDEF()
            {
                coltyp = JET_coltyp.Long,
            };
            Api.JetAddColumn(this.sesid, this.tableid, "keycolumn", columndef, null, 0, out this.keyColumn);

            columndef = new JET_COLUMNDEF()
            {
                cp = JET_CP.Unicode,
                coltyp = JET_coltyp.LongText,
                grbit = ColumndefGrbit.ColumnMultiValued,
            };
            Api.JetAddColumn(this.sesid, this.tableid, "mvcolumn", columndef, null, 0, out this.multiValueColumn);

            const string PrimaryIndexDescription = "+keycolumn\0\0";
            Api.JetCreateIndex(this.sesid, this.tableid, "primary", CreateIndexGrbit.IndexPrimary, PrimaryIndexDescription, PrimaryIndexDescription.Length, 0);

            const string MultiValueIndexDescription = "+mvcolumn\0\0";
            Api.JetCreateIndex(this.sesid, this.tableid, "index", CreateIndexGrbit.None, MultiValueIndexDescription, MultiValueIndexDescription.Length, 0);

            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup for MultivalueIndexFixture")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
            SetupHelper.CheckProcessForInstanceLeaks();
        }

        /// <summary>
        /// Verify that MultivalueIndexFixture has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify that MultivalueIndexFixture has setup the test fixture properly")]
        public void VerifyFixtureSetup()
        {
            Assert.IsNotNull(this.table);
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
            Assert.AreNotEqual(JET_DBID.Nil, this.dbid);
            Assert.AreNotEqual(JET_TABLEID.Nil, this.tableid);
            Assert.AreNotEqual(JET_COLUMNID.Nil, this.multiValueColumn);

            JET_COLUMNDEF columndef;
            Api.JetGetTableColumnInfo(this.sesid, this.tableid, this.multiValueColumn, out columndef);
            Assert.AreEqual(JET_coltyp.LongText, columndef.coltyp);
        }

        #endregion Setup/Teardown

        /// <summary>
        /// Test JetGetSecondaryIndexBookmark.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetGetSecondaryIndexBookmark")]
        public void JetGetSecondaryIndexBookmark()
        {
            Api.JetSetCurrentIndex(this.sesid, this.tableid, "index");

            this.InsertRecord(0, "a", "b");

            var primaryKey = new byte[SystemParameters.KeyMost];
            int actualPrimaryKeySize;
            var secondaryKey = new byte[SystemParameters.KeyMost];
            int actualSecondaryKeySize;
            Api.JetGetSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                secondaryKey,
                secondaryKey.Length,
                out actualSecondaryKeySize,
                primaryKey,
                primaryKey.Length,
                out actualPrimaryKeySize,
                GetSecondaryIndexBookmarkGrbit.None);

            Assert.AreNotEqual(0, actualPrimaryKeySize);
            Assert.AreNotEqual(0, actualSecondaryKeySize);
        }

        /// <summary>
        /// Test JetGotoSecondaryIndexBookmark.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetGotoSecondaryIndexBookmark")]
        public void JetGotoSecondaryIndexBookmark()
        {
            Api.JetSetCurrentIndex(this.sesid, this.tableid, "index");

            this.InsertRecord(0, "a", "b", "c");
            this.InsertRecord(1, "d", "e", "f");
            this.InsertRecord(2, "g", "h", "i");

            Api.MakeKey(this.sesid, this.tableid, "f", Encoding.Unicode, MakeKeyGrbit.NewKey);
            Assert.IsTrue(Api.TrySeek(this.sesid, this.tableid, SeekGrbit.SeekEQ), "Failed to find record");

            var primaryKey = new byte[SystemParameters.KeyMost];
            int actualPrimaryKeySize;
            var secondaryKey = new byte[SystemParameters.KeyMost];
            int actualSecondaryKeySize;
            Api.JetGetSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                secondaryKey,
                secondaryKey.Length,
                out actualSecondaryKeySize,
                primaryKey,
                primaryKey.Length,
                out actualPrimaryKeySize,
                GetSecondaryIndexBookmarkGrbit.None);

            Api.JetGotoSecondaryIndexBookmark(
                this.sesid,
                this.tableid,
                secondaryKey,
                actualSecondaryKeySize,
                primaryKey,
                actualPrimaryKeySize,
                GotoSecondaryIndexBookmarkGrbit.None);

            Assert.AreEqual(
                1, (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn), "landed on wrong record");
            Assert.IsTrue(Api.TryMoveNext(this.sesid, this.tableid), "unable to move to next record");
            Assert.AreEqual(
                2, (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn), "should have been on the next record");
        }

        /// <summary>
        /// Test JetSetCurrentIndex2.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetSetCurrentIndex2")]
        public void JetSetCurrentIndex2()
        {
            Api.JetSetCurrentIndex(this.sesid, this.tableid, null);

            this.InsertRecord(0, "a");
            this.InsertRecord(1, "b");
            this.InsertRecord(2, "c");

            Api.JetSetCurrentIndex2(this.sesid, this.tableid, "index", SetCurrentIndexGrbit.NoMove);

            Assert.AreEqual(
                2, (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn), "should be on the same record");
        }

        /// <summary>
        /// Test JetSetCurrentIndex3.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetSetCurrentIndex3")]
        public void JetSetCurrentIndex3()
        {
            Api.JetSetCurrentIndex(this.sesid, this.tableid, null);

            this.InsertRecord(0, "a", "b", "c");
            this.InsertRecord(1, "x", "y", "z");
            this.InsertRecord(2, "i", "j", "k");

            // This should position us on the last itag of record 2 ([2:k]). Moving next should take
            // us to the next record ([1:x]).
            Api.JetSetCurrentIndex3(this.sesid, this.tableid, "index", SetCurrentIndexGrbit.NoMove, 3);

            Assert.AreEqual(
                2, (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn), "should be on the same record");
            Assert.IsTrue(Api.TryMoveNext(this.sesid, this.tableid), "unable to move to next record");
            Assert.AreEqual(
                1, (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn), "should have been on the next record");
        }

        /// <summary>
        /// Test JetSetCurrentIndex4.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetSetCurrentIndex4")]
        public void JetSetCurrentIndex4()
        {
            JET_INDEXID indexid;
            Api.JetGetTableIndexInfo(this.sesid, this.tableid, "index", out indexid, JET_IdxInfo.IndexId);
            Api.JetSetCurrentIndex(this.sesid, this.tableid, null);

            this.InsertRecord(0, "a", "b", "c");
            this.InsertRecord(1, "x", "y", "z");
            this.InsertRecord(2, "i", "j", "k");

            // This should position us on the last itag of record 2 ([2:k]). Moving next should take
            // us to the next record ([1:x]).
            Api.JetSetCurrentIndex4(this.sesid, this.tableid, "index", indexid, SetCurrentIndexGrbit.NoMove, 3);

            Assert.AreEqual(
                2, (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn), "should be on the same record");
            Assert.IsTrue(Api.TryMoveNext(this.sesid, this.tableid), "unable to move to next record");
            Assert.AreEqual(
                1, (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn), "should have been on the next record");
        }

        /// <summary>
        /// Test TryMove.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test TryMove")]
        public void TestTryMove()
        {
            this.InsertRecord(0, "a", "b", "c");
            this.InsertRecord(1, "a", "b", "c");
            this.InsertRecord(2, "a", "b", "c");

            Api.JetSetCurrentIndex3(this.sesid, this.tableid, "index", SetCurrentIndexGrbit.NoMove, 3);
            Assert.IsTrue(Api.TryMoveFirst(this.sesid, this.tableid));

            Assert.AreEqual(
                0, (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn), "should be on the first record");
            Assert.IsTrue(Api.TryMove(this.sesid, this.tableid, JET_Move.Next, MoveGrbit.MoveKeyNE), "unable to move to next record");
            Assert.AreEqual(
                0, (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn), "should be on the first record");
            Assert.IsTrue(Api.TryMove(this.sesid, this.tableid, JET_Move.Next, MoveGrbit.MoveKeyNE), "unable to move to next record");
            Assert.AreEqual(
                0, (int)Api.RetrieveColumnAsInt32(this.sesid, this.tableid, this.keyColumn), "should be on the first record");
            Assert.IsFalse(Api.TryMove(this.sesid, this.tableid, JET_Move.Next, MoveGrbit.MoveKeyNE), "able to move to next record");
        }

        #region Helper Methods

        /// <summary>
        /// Insert a record with the given column values. After the insert the cursor is 
        /// positioned on the record.
        /// </summary>
        /// <param name="key">
        /// The key of the record.
        /// </param>
        /// <param name="values">
        /// The column values to insert.
        /// </param>
        private void InsertRecord(int key, params string[] values)
        {
            using (var transaction = new Transaction(this.sesid))
            using (var update = new Update(this.sesid, this.tableid, JET_prep.Insert))
            {
                Api.SetColumn(this.sesid, this.tableid, this.keyColumn, key);
                foreach (string value in values)
                {
                    var setinfo = new JET_SETINFO
                    {
                        ibLongValue = 0,
                        itagSequence = 0,
                    };
                    byte[] data = Encoding.Unicode.GetBytes(value);
                    Api.JetSetColumn(
                        this.sesid, this.tableid, this.multiValueColumn, data, data.Length, SetColumnGrbit.None, setinfo);
                }

                update.SaveAndGotoBookmark();
                transaction.Commit(CommitTransactionGrbit.None);
            }
        }

        #endregion HelperMethods
    }
}
