//-----------------------------------------------------------------------
// <copyright file="TransactionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using Microsoft.Isam.Esent;
using Microsoft.Isam.Esent.Interop;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Transaction=Microsoft.Isam.Esent.Transaction;

namespace PixieTests
{
    /// <summary>
    /// Test the EsentTransaction class
    /// </summary>
    [TestClass]
    public class TransactionTests
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
        private string tablename;

        /// <summary>
        /// The instance used by the test.
        /// </summary>
        private Microsoft.Isam.Esent.Interop.JET_INSTANCE instance;

        /// <summary>
        /// The session used by the test.
        /// </summary>
        private Session session;

        /// <summary>
        /// Identifies the database used by the test.
        /// </summary>
        private Microsoft.Isam.Esent.Interop.JET_DBID dbid;

        /// <summary>
        /// The tableid of the table.
        /// </summary>
        private Microsoft.Isam.Esent.Interop.JET_TABLEID tableid;

        /// <summary>
        /// Columnid of the Long column in the table.
        /// </summary>
        private Microsoft.Isam.Esent.Interop.JET_COLUMNID columnid;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Called once when the tests are started.
        /// All DDL should be done in this method.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            Microsoft.Isam.Esent.Interop.JET_TABLEID tableid;

            this.directory = SetupHelper.CreateRandomDirectory();
            this.database = Path.Combine(this.directory, "database.edb");
            this.tablename = "table";
            this.instance = SetupHelper.CreateNewInstance(this.directory);

            // turn off logging so initialization is faster
            Microsoft.Isam.Esent.Interop.Api.JetSetSystemParameter(this.instance, Microsoft.Isam.Esent.Interop.JET_SESID.Nil, Microsoft.Isam.Esent.Interop.JET_param.Recovery, 0, "off");
            Microsoft.Isam.Esent.Interop.Api.JetSetSystemParameter(this.instance, Microsoft.Isam.Esent.Interop.JET_SESID.Nil, Microsoft.Isam.Esent.Interop.JET_param.MaxTemporaryTables, 0, null);
            Microsoft.Isam.Esent.Interop.Api.JetInit(ref this.instance);
            this.session = new Session(this.instance);
            Microsoft.Isam.Esent.Interop.Api.JetCreateDatabase(this.session, this.database, String.Empty, out this.dbid, Microsoft.Isam.Esent.Interop.CreateDatabaseGrbit.None);
            Microsoft.Isam.Esent.Interop.Api.JetBeginTransaction(this.session);
            Microsoft.Isam.Esent.Interop.Api.JetCreateTable(this.session, this.dbid, this.tablename, 0, 100, out tableid);

            var columndef = new Microsoft.Isam.Esent.Interop.JET_COLUMNDEF() { coltyp = Microsoft.Isam.Esent.Interop.JET_coltyp.Long };
            Microsoft.Isam.Esent.Interop.Api.JetAddColumn(this.session, tableid, "Long", columndef, null, 0, out this.columnid);

            string indexDef = "+long\0\0";
            Microsoft.Isam.Esent.Interop.Api.JetCreateIndex(this.session, tableid, "primary", Microsoft.Isam.Esent.Interop.CreateIndexGrbit.IndexPrimary, indexDef, indexDef.Length, 100);

            Microsoft.Isam.Esent.Interop.Api.JetCloseTable(this.session, tableid);
            Microsoft.Isam.Esent.Interop.Api.JetCommitTransaction(this.session, Microsoft.Isam.Esent.Interop.CommitTransactionGrbit.LazyFlush);

            Microsoft.Isam.Esent.Interop.Api.JetOpenTable(this.session, this.dbid, this.tablename, null, 0, Microsoft.Isam.Esent.Interop.OpenTableGrbit.None, out this.tableid);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            Microsoft.Isam.Esent.Interop.Api.JetEndSession(this.session, Microsoft.Isam.Esent.Interop.EndSessionGrbit.None);
            Microsoft.Isam.Esent.Interop.Api.JetTerm(this.instance);
            Directory.Delete(this.directory, true);
        }

        /// <summary>
        /// Verify that the test class has setup the test fixture properly.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void VerifyFixtureSetup()
        {
            Assert.AreNotEqual(Microsoft.Isam.Esent.Interop.JET_INSTANCE.Nil, this.instance);
            Assert.AreNotEqual(Microsoft.Isam.Esent.Interop.JET_SESID.Nil, this.session);
            Assert.AreNotEqual(Microsoft.Isam.Esent.Interop.JET_COLUMNID.Nil, this.columnid);
            Assert.AreNotEqual(Microsoft.Isam.Esent.Interop.JET_TABLEID.Nil, this.tableid);
        }

        #endregion Setup/Teardown

        [TestMethod]
        [Priority(2)]
        public void TestHelperMethods()
        {
            this.InsertRecord(1);
            Assert.IsTrue(this.ContainsRecord(1));
        }

        [TestMethod]
        [Priority(2)]
        public void VerifyCommittedEventIsCalledOnCommit()
        {
            bool eventCalled = false;

            Transaction transaction = new EsentTransaction(this.session, "test", null);
            transaction.Committed += () => eventCalled = true;
            transaction.Commit();

            Assert.IsTrue(eventCalled);
        }

        [TestMethod]
        [Priority(2)]
        public void VerifyRolledBackEventIsCalledOnRollback()
        {
            bool eventCalled = false;

            Transaction transaction = new EsentTransaction(this.session, "test", null);
            transaction.RolledBack += () => eventCalled = true;
            transaction.Rollback();

            Assert.IsTrue(eventCalled);
        }

        [TestMethod]
        [Priority(2)]
        public void VerifyCommitOfOuterTransactionCommitsInnerTransaction()
        {
            bool eventCalled = false;

            var outerTransaction = new EsentTransaction(this.session, "test", null);
            Transaction innerTransaction = new EsentTransaction(this.session, "test", outerTransaction);

            innerTransaction.Committed += () => eventCalled = true;
            outerTransaction.Commit();

            Assert.IsTrue(eventCalled);
        }

        [TestMethod]
        [Priority(2)]
        public void VerifyRollbackOfOuterTransactionRollsbackInnerTransaction()
        {
            bool eventCalled = false;

            var outerTransaction = new EsentTransaction(this.session, "test", null);
            Transaction innerTransaction = new EsentTransaction(this.session, "test", outerTransaction);

            innerTransaction.RolledBack += () => eventCalled = true;
            outerTransaction.Rollback();

            Assert.IsTrue(eventCalled);
        }

        [TestMethod]
        [Priority(2)]
        public void VerifyRolledBackEventMigratesToOuterTransactionOnCommit()
        {
            bool eventCalled = false;

            var outerTransaction = new EsentTransaction(this.session, "test", null);
            Transaction innerTransaction = new EsentTransaction(this.session, "test", outerTransaction);

            innerTransaction.RolledBack += () => eventCalled = true;
            innerTransaction.Commit();
            outerTransaction.Rollback();

            Assert.IsTrue(eventCalled);
        }

        [TestMethod]
        [Priority(2)]
        public void VerifyGetNewestTransactionReturnsCurrentTransactionIfNoSubtransactions()
        {
            var transaction = new EsentTransaction(this.session, "test", null);
            Assert.AreEqual(transaction, transaction.GetNewestTransaction());
        }

        [TestMethod]
        [Priority(2)]
        public void VerifyGetNewestTransactionReturnsNewestTransaction()
        {
            var level0transaction = new EsentTransaction(this.session, "test", null);
            var innerTransaction = new EsentTransaction(this.session, "test", level0transaction);
            var innermostTransaction = new EsentTransaction(this.session, "test", innerTransaction);

            Assert.AreEqual(innermostTransaction, level0transaction.GetNewestTransaction());
        }

        [TestMethod]
        [Priority(2)]
        public void VerifyCommitPersistsChanges()
        {
            Transaction transaction = new EsentTransaction(this.session, "test", null);
            this.InsertRecord(1);
            transaction.Commit();

            Assert.IsTrue(this.ContainsRecord(1));
        }

        [TestMethod]
        [Priority(2)]
        public void VerifyRollbackUndoesChanges()
        {
            Transaction transaction = new EsentTransaction(this.session, "test", null);
            this.InsertRecord(1);
            transaction.Rollback();

            Assert.IsFalse(this.ContainsRecord(1));
        }

        [TestMethod]
        [Priority(2)]
        public void VerifyCommitThenRollbackUndoesChanges()
        {
            var outerTransaction = new EsentTransaction(this.session, "test", null);
            Transaction innerTransaction = new EsentTransaction(this.session, "test", outerTransaction);

            this.InsertRecord(1);
            innerTransaction.Commit();
            Assert.IsTrue(this.ContainsRecord(1));

            outerTransaction.Rollback();
            Assert.IsFalse(this.ContainsRecord(1));
        }

        #region Helper Methods

        /// <summary>
        /// Insert a record with the given key.
        /// </summary>
        /// <param name="key">The key of the record.</param>
        private void InsertRecord(int key)
        {
            Microsoft.Isam.Esent.Interop.Api.JetPrepareUpdate(this.session, this.tableid, Microsoft.Isam.Esent.Interop.JET_prep.Insert);
            Microsoft.Isam.Esent.Interop.Api.SetColumn(this.session, this.tableid, this.columnid, key);
            int ignored;
            Microsoft.Isam.Esent.Interop.Api.JetUpdate(this.session, this.tableid, null, 0, out ignored);
        }

        /// <summary>
        /// Determines whether the table contains a record with the key.
        /// </summary>
        /// <param name="key">The key of the record to search for.</param>
        /// <returns>True if the record is in the table, false otherwise.</returns>
        private bool ContainsRecord(int key)
        {
            Microsoft.Isam.Esent.Interop.Api.MakeKey(this.session, this.tableid, key, Microsoft.Isam.Esent.Interop.MakeKeyGrbit.NewKey);
            return Microsoft.Isam.Esent.Interop.Api.TrySeek(this.session, this.tableid, Microsoft.Isam.Esent.Interop.SeekGrbit.SeekEQ);
        }

        #endregion Helper Methods
    }
}
