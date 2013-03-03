//-----------------------------------------------------------------------
// <copyright file="NavigationTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test for navigation on a table. This is used to test methods which don't
    /// work on temp tables. See <see cref="TempTableNavigationTests"/> for tests
    /// that run on a temporary table.
    /// </summary>
    [TestClass]
    public class NavigationTests
    {
        /// <summary>
        /// Number of records inserted in the table.
        /// </summary>
        private int numRecords;

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
        /// Columnid of the Long column in the table.
        /// </summary>
        private JET_COLUMNID columnidLong;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        [Description("Setup the NavigationTests fixture")]
        public void Setup()
        {
            var random = new Random();
            this.numRecords = random.Next(5, 20);

            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.table = "table";
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            // turn off logging so initialization is faster
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.Recovery, 0, "off");
            Api.JetSetSystemParameter(this.instance, JET_SESID.Nil, JET_param.MaxTemporaryTables, 0, null);
            Api.JetInit(ref this.instance);
            Api.JetBeginSession(this.instance, out this.sesid, String.Empty, String.Empty);
            Api.JetCreateDatabase(this.sesid, this.database, String.Empty, out this.dbid, CreateDatabaseGrbit.None);
            Api.JetBeginTransaction(this.sesid);
            Api.JetCreateTable(this.sesid, this.dbid, this.table, 0, 100, out this.tableid);

            var columndef = new JET_COLUMNDEF() { coltyp = JET_coltyp.Long };
            Api.JetAddColumn(this.sesid, this.tableid, "Long", columndef, null, 0, out this.columnidLong);

            string indexDef = "+long\0\0";
            Api.JetCreateIndex(this.sesid, this.tableid, "primary", CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length, 100);

            for (int i = 0; i < this.numRecords; ++i)
            {
                Api.JetPrepareUpdate(this.sesid, this.tableid, JET_prep.Insert);
                Api.JetSetColumn(this.sesid, this.tableid, this.columnidLong, BitConverter.GetBytes(i), 4, SetColumnGrbit.None, null);
                int ignored;
                Api.JetUpdate(this.sesid, this.tableid, null, 0, out ignored);
            }

            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetCommitTransaction(this.sesid, CommitTransactionGrbit.LazyFlush);
            Api.JetOpenTable(this.sesid, this.dbid, this.table, null, 0, OpenTableGrbit.None, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the NavigationTests fixture")]
        public void Teardown()
        {
            Api.JetCloseTable(this.sesid, this.tableid);
            Api.JetEndSession(this.sesid, EndSessionGrbit.None);
            Api.JetTerm(this.instance);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        /// <summary>
        /// Verify that the test class has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify the NavigationTests fixture is setup properly")]
        public void VerifyFixtureSetup()
        {
            Assert.AreNotEqual(JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(JET_SESID.Nil, this.sesid);
            Assert.IsTrue(this.numRecords > 0);
            Assert.AreNotEqual(JET_COLUMNID.Nil, this.columnidLong);

            JET_COLUMNDEF columndef;
            Api.JetGetTableColumnInfo(this.sesid, this.tableid, this.columnidLong, out columndef);
            Assert.AreEqual(JET_coltyp.Long, columndef.coltyp);
        }

        #endregion Setup/Teardown

        #region JetGotoPosition Tests

        /// <summary>
        /// Test using JetGotoPosition to go to the first record.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test using JetGotoPosition to go to the first record.")]
        public void GotoFirstPosition()
        {
            var recpos = new JET_RECPOS() { centriesLT = 0, centriesTotal = 10 };
            Api.JetGotoPosition(this.sesid, this.tableid, recpos);
            int actual = this.GetLongColumn();
            Assert.AreEqual(0, actual);
        }

        /// <summary>
        /// Test using JetGotoPosition to go to the last record.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test using JetGotoPosition to go to the last record.")]
        public void GotoLastPosition()
        {
            var recpos = new JET_RECPOS() { centriesLT = 4, centriesTotal = 4 };
            Api.JetGotoPosition(this.sesid, this.tableid, recpos);
            int actual = this.GetLongColumn();
            Assert.AreEqual(this.numRecords - 1, actual);
        }

        #endregion JetGotoPosition Tests

        #region JetIndexRecordCount Tests

        /// <summary>
        /// Count the records in the table with JetIndexRecordCount.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Count the records in the table with JetIndexRecordCount.")]
        public void GetIndexRecordCount()
        {
            int countedRecords;
            Api.JetIndexRecordCount(this.sesid, this.tableid, out countedRecords, 0);
            Assert.AreEqual(this.numRecords, countedRecords);
        }

        /// <summary>
        /// Count the records in the table with JetIndexRecordCount, with
        /// the maximum number of records constrained.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Count the records in the table with JetIndexRecordCount, with the maximum number of records constrained.")]
        public void GetIndexRecordCountConstrained()
        {
            int countedRecords;
            Api.JetIndexRecordCount(this.sesid, this.tableid, out countedRecords, this.numRecords - 1);
            Assert.AreEqual(this.numRecords - 1, countedRecords);
        }

        /// <summary>
        /// Count the records in an index range with JetGetIndexRecordCount.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Count the records in an index range with JetGetIndexRecordCount.")]
        public void CountIndexRangeRecords()
        {
            int first = 2;
            int last = this.numRecords - 2;

            this.MakeKeyForRecord(first);
            Api.JetSeek(this.sesid, this.tableid, SeekGrbit.SeekEQ);

            this.MakeKeyForRecord(last);
            Api.JetSetIndexRange(this.sesid, this.tableid, SetIndexRangeGrbit.RangeUpperLimit);

            int countedRecords;
            Api.JetIndexRecordCount(this.sesid, this.tableid, out countedRecords, 0);
            Assert.AreEqual(last - first, countedRecords);
        }

        #endregion

        #region JetGetRecordPosition

        /// <summary>
        /// Test using JetGetRecord position.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Test JetGetRecordPosition")]
        public void GetRecordPosition()
        {
            Api.JetMove(this.sesid, this.tableid, JET_Move.Last, MoveGrbit.None);
            JET_RECPOS recpos;
            Api.JetGetRecordPosition(this.sesid, this.tableid, out recpos);
            Assert.AreEqual(recpos.centriesLT, recpos.centriesTotal - 1);
        }

        #endregion

        /// <summary>
        /// Sequentially scan all the records in the table.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Sequentially scan all the records in the table")]
        public void ScanRecords()
        {
            Api.JetMove(this.sesid, this.tableid, JET_Move.First, MoveGrbit.None);
            Api.JetSetTableSequential(this.sesid, this.tableid, SetTableSequentialGrbit.None);
            for (int i = 0; i < this.numRecords; ++i)
            {
                int actual = this.GetLongColumn();
                Assert.AreEqual(i, actual);
                if (this.numRecords - 1 != i)
                {
                    Api.JetMove(this.sesid, this.tableid, JET_Move.Next, MoveGrbit.None);
                }
            }

            Api.JetResetTableSequential(this.sesid, this.tableid, ResetTableSequentialGrbit.None);
        }

        #region Helper Methods

        /// <summary>
        /// Assert that we are currently positioned on the given record.
        /// </summary>
        /// <param name="recordId">The expected record ID.</param>
        private void AssertOnRecord(int recordId)
        {
            int actualId = this.GetLongColumn();
            Assert.AreEqual(recordId, actualId);
        }

        /// <summary>
        /// Return the value of the columnidLong of the current record.
        /// </summary>
        /// <returns>The value of the columnid, converted to an int.</returns>
        private int GetLongColumn()
        {
            var data = new byte[4];
            int actualDataSize;
            Api.JetRetrieveColumn(this.sesid, this.tableid, this.columnidLong, data, data.Length, out actualDataSize, RetrieveColumnGrbit.None, null);
            Assert.AreEqual(data.Length, actualDataSize);
            return BitConverter.ToInt32(data, 0);
        }

        /// <summary>
        /// Make a key for a record with the given ID
        /// </summary>
        /// <param name="id">The id of the record.</param>
        private void MakeKeyForRecord(int id)
        {
            byte[] data = BitConverter.GetBytes(id);
            Api.JetMakeKey(this.sesid, this.tableid, data, data.Length, MakeKeyGrbit.NewKey);
        }

        #endregion Helper Methods
    }
}
