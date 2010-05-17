//-----------------------------------------------------------------------
// <copyright file="HowDoI.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//      Example code for the ManagedEsent project.
// </summary>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using System.Text;
    using System.Threading;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests that demonstrate how to do common tasks with ManagedEsent.
    /// </summary>
    [TestClass]
    public class HowDoI
    {
        /// <summary>
        /// The directory that will contain the database.
        /// </summary>
        private const string TestDirectory = "HowDoI";

        /// <summary>
        /// The name of the database.
        /// </summary>
        private const string TestDatabase = @"HowDoI\Demo.edb";

        /// <summary>
        /// The instance we are using.
        /// </summary>
        private Instance testInstance;

        /// <summary>
        /// The session we are using.
        /// </summary>
        private Session testSession;

        /// <summary>
        /// The test database.
        /// </summary>
        private JET_DBID testDbid;

        #region Setup/Teardown

        /// <summary>
        /// Initialization method. Creates the directory, but not the database.
        /// </summary>
        [TestInitialize]
        [Description("Setup for HowDoI examples")]
        public void Setup()
        {
            Cleanup.DeleteDirectoryWithRetry(TestDirectory);
            Directory.CreateDirectory(TestDirectory);
            this.testInstance = new Instance("HowDoI");
            SetupHelper.SetLightweightConfiguration(this.testInstance);
            this.testInstance.Init();
            this.testSession = new Session(this.testInstance);
            Api.JetCreateDatabase(this.testSession, TestDatabase, null, out this.testDbid, CreateDatabaseGrbit.OverwriteExisting);
        }

        /// <summary>
        /// Cleanup after all tests have run.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup for HowDoIExamples")]
        public void Teardown()
        {
            this.testInstance.Term();
            Cleanup.DeleteDirectoryWithRetry(TestDirectory);
        }

        #endregion

        /// <summary>
        /// Demonstrate how to build a key over multiple columns.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Demonstrate how to build a key over multiple columns")]
        public void HowDoIMakeAMultiColumnKey()
        {
            JET_SESID sesid = this.testSession;
            JET_DBID dbid = this.testDbid;

            JET_TABLEID tableid;
            JET_COLUMNDEF columndef = new JET_COLUMNDEF();
            JET_COLUMNID boolColumn;
            JET_COLUMNID int32Column;
            JET_COLUMNID stringColumn;
            JET_COLUMNID dataColumn;

            // First create the table. There will be three key columns, a boolean
            // an Int32 and a String. There will be one data column.
            Api.JetCreateTable(sesid, dbid, "table", 0, 100, out tableid);
            columndef.coltyp = JET_coltyp.Bit;
            Api.JetAddColumn(sesid, tableid, "bool", columndef, null, 0, out boolColumn);
            columndef.coltyp = JET_coltyp.Long;
            Api.JetAddColumn(sesid, tableid, "int32", columndef, null, 0, out int32Column);
            columndef.coltyp = JET_coltyp.LongText;
            columndef.cp = JET_CP.Unicode;
            Api.JetAddColumn(sesid, tableid, "string", columndef, null, 0, out stringColumn);
            Api.JetAddColumn(sesid, tableid, "data", columndef, null, 0, out dataColumn);

            const string KeyDescription = "+bool\0+int32\0+string\0\0";
            Api.JetCreateIndex(
                sesid,
                tableid,
                "index",
                CreateIndexGrbit.IndexPrimary,
                KeyDescription,
                KeyDescription.Length,
                100);

            // Insert a record.
            using (var transaction = new Transaction(sesid))
            using (var update = new Update(sesid, tableid, JET_prep.Insert))
            {
                Api.SetColumn(sesid, tableid, boolColumn, true);
                Api.SetColumn(sesid, tableid, int32Column, 8);
                Api.SetColumn(sesid, tableid, stringColumn, "foo", Encoding.Unicode);
                Api.SetColumn(sesid, tableid, dataColumn, "hello world", Encoding.Unicode);
                update.Save();
                transaction.Commit(CommitTransactionGrbit.LazyFlush);
            }

            // Build a key on the index. The index has 3 segments, a Bool
            // (JET_coltyp.Bit), an Int32 (JET_coltyp.Long) and  String
            // (JET_coltyp.LongText). To build a multi-column key we make
            // one call to JetMakeKey for each column. The first call passes
            // in MakeKeyGrbit.NewKey.
            Api.MakeKey(sesid, tableid, true, MakeKeyGrbit.NewKey);
            Api.MakeKey(sesid, tableid, 8, MakeKeyGrbit.None);
            Api.MakeKey(sesid, tableid, "foo", Encoding.Unicode, MakeKeyGrbit.None);
            Api.JetSeek(sesid, tableid, SeekGrbit.SeekEQ);
            Assert.AreEqual("hello world", Api.RetrieveColumnAsString(sesid, tableid, dataColumn));

            // Here we seek on a key prefix. The index has 3 segments but
            // we only build a key on 2 of them. In this case we use SeekGrbit.SeekGE
            // to find the first record that matches the key prefix.
            Api.MakeKey(sesid, tableid, true, MakeKeyGrbit.NewKey);
            Api.MakeKey(sesid, tableid, 8, MakeKeyGrbit.None);
            Api.JetSeek(sesid, tableid, SeekGrbit.SeekGE);
            Assert.AreEqual("hello world", Api.RetrieveColumnAsString(sesid, tableid, dataColumn));
        }

        /// <summary>
        /// Demonstrate how to seek by string prefix.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Demonstrate how to seek by string prefix")]
        public void HowDoISeekByStringPrefix()
        {
            JET_SESID sesid = this.testSession;
            JET_DBID dbid = this.testDbid;

            JET_TABLEID tableid;
            JET_COLUMNDEF columndef = new JET_COLUMNDEF();
            JET_COLUMNID keyColumn;
            JET_COLUMNID dataColumn;

            // First create the table. The key is a string and the data
            // is an Int32.
            Api.JetCreateTable(sesid, dbid, "table", 0, 100, out tableid);
            columndef.coltyp = JET_coltyp.LongText;
            columndef.cp = JET_CP.Unicode;
            Api.JetAddColumn(sesid, tableid, "key", columndef, null, 0, out keyColumn);
            columndef.coltyp = JET_coltyp.Long;
            Api.JetAddColumn(sesid, tableid, "data", columndef, null, 0, out dataColumn);

            const string KeyDescription = "+key\0\0";
            Api.JetCreateIndex(
                sesid,
                tableid,
                "index",
                CreateIndexGrbit.IndexPrimary,
                KeyDescription,
                KeyDescription.Length,
                100);

            // Insert some records
            using (var transaction = new Transaction(sesid))
            {
                int i = 0;
                foreach (string k in new[] { "foo", "bar", "baz" })
                {
                    using (var update = new Update(sesid, tableid, JET_prep.Insert))
                    {
                        Api.SetColumn(sesid, tableid, keyColumn, k, Encoding.Unicode);
                        Api.SetColumn(sesid, tableid, dataColumn, i++);
                        update.Save();
                    }                    
                }

                transaction.Commit(CommitTransactionGrbit.LazyFlush);
            }

            // Seek for any record that is >= "ba". If any record has "ba" as a prefix
            // this will land on the first such record. This can also land on records
            // that don't have the prefix, e.g. "qux".
            Api.MakeKey(sesid, tableid, "ba", Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.JetSeek(sesid, tableid, SeekGrbit.SeekGE);
            Assert.AreEqual(1, Api.RetrieveColumnAsInt32(sesid, tableid, dataColumn));

            // If we want to find just records that have "ba" as a prefix we need to 
            // set up an index range. We are already positioned on the first record
            // so now we need to set up the other end of the index range. Here we use
            // the PartialColumnEndLimit to build a key which matches "ba*".
            Api.MakeKey(sesid, tableid, "ba", Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.PartialColumnEndLimit);
            Assert.IsTrue(Api.TrySetIndexRange(sesid, tableid, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit));
            int numRecords;
            Api.JetIndexRecordCount(sesid, tableid, out numRecords, 0);
            Assert.AreEqual(2, numRecords, "Should match 'bar' and 'baz' but not 'foo'");
        }

        /// <summary>
        /// Demonstrate how to iterate over records.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Demonstrate how to iterate over records")]
        public void HowDoIScanRecords()
        {
            JET_SESID sesid = this.testSession;
            JET_DBID dbid = this.testDbid;

            JET_TABLEID tableid;
            JET_COLUMNDEF columndef = new JET_COLUMNDEF();
            JET_COLUMNID dataColumn;

            // First create the table. There in one Int32 column.
            Api.JetCreateTable(sesid, dbid, "table", 0, 100, out tableid);
            columndef.coltyp = JET_coltyp.Long;
            Api.JetAddColumn(sesid, tableid, "data", columndef, null, 0, out dataColumn);

            // Insert some records
            using (var transaction = new Transaction(sesid))
            {
                for (int i = 0; i < 10; ++i)
                {
                    using (var update = new Update(sesid, tableid, JET_prep.Insert))
                    {
                        Api.SetColumn(sesid, tableid, dataColumn, i);
                        update.Save();
                    }
                }

                transaction.Commit(CommitTransactionGrbit.LazyFlush);
            }

            // This pattern can be used to enumerate all records in a table/index.
            int? sum = 0;
            Api.MoveBeforeFirst(sesid, tableid);
            while (Api.TryMoveNext(sesid, tableid))
            {
                sum += Api.RetrieveColumnAsInt32(sesid, tableid, dataColumn);
            }

            Assert.AreEqual(45, (int)sum);
        }

        /// <summary>
        /// Demonstrate how to retrieve the value of an autoincrement column.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Demonstrate how to retrieve the value of an autoincrement column")]
        public void HowDoIRetrieveAnAutoInc()
        {
            JET_SESID sesid = this.testSession;
            JET_DBID dbid = this.testDbid;

            JET_TABLEID tableid;
            JET_COLUMNDEF columndef = new JET_COLUMNDEF();
            JET_COLUMNID autoincColumn;

            // First create the table. There is one autoinc column.
            Api.JetCreateTable(sesid, dbid, "table", 0, 100, out tableid);
            columndef.coltyp = JET_coltyp.Long;
            columndef.grbit = ColumndefGrbit.ColumnAutoincrement;
            Api.JetAddColumn(sesid, tableid, "data", columndef, null, 0, out autoincColumn);

            // Once the update is prepared the autoinc column can be retrieved. This
            // requires the RetrieveCopy option, which gets a value from the record 
            // currently under construction.
            for (int i = 0; i < 10; i++)
            {
                using (var update = new Update(sesid, tableid, JET_prep.Insert))
                {
                    int? autoinc = Api.RetrieveColumnAsInt32(
                        sesid,
                        tableid,
                        autoincColumn,
                        RetrieveColumnGrbit.RetrieveCopy);
                    Console.WriteLine("{0}", autoinc);
                    update.Save();
                }
            }
        }

        /// <summary>
        /// Demonstrate how to periodically commit a transaction.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Demonstrate how to periodically commit a transaction")]
        public void HowDoIPulseMyTransaction()
        {
            JET_SESID sesid = this.testSession;
            JET_DBID dbid = this.testDbid;

            // First create the table
            JET_TABLEID tableid;
            Api.JetCreateTable(sesid, dbid, "table", 0, 100, out tableid);
            Api.JetCloseTable(sesid, tableid);
            Api.JetOpenTable(sesid, dbid, "table", null, 0, OpenTableGrbit.None, out tableid);

            // Insert a selection of records
            const int BatchSize = 100;
            Api.JetBeginTransaction(sesid);
            for (int i = 0; i < 256; i++)
            {
                Api.JetPrepareUpdate(sesid, tableid, JET_prep.Insert);
                Api.JetUpdate(sesid, tableid);

                if (i % BatchSize == (BatchSize - 1))
                {
                    // Long-running transactions will consume lots of version store space in ESENT.
                    // Periodically commit the transaction to avoid running out of version store.
                    Api.JetCommitTransaction(sesid, CommitTransactionGrbit.LazyFlush);
                    Api.JetBeginTransaction(sesid);
                }
            }

            Api.JetCommitTransaction(sesid, CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Demonstrate how to lock records.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Demonstrate how to lock records")]
        public void HowDoILockRecords()
        {
            JET_SESID sesid = this.testSession;
            JET_DBID dbid = this.testDbid;

            // First create the table
            JET_TABLEID tableid;
            Api.JetCreateTable(sesid, dbid, "table", 0, 100, out tableid);
            Api.JetCloseTable(sesid, tableid);
            Api.JetOpenTable(sesid, dbid, "table", null, 0, OpenTableGrbit.None, out tableid);

            // Insert a selection of records
            for (int i = 0; i < 30; i++)
            {
                using (var update = new Update(sesid, tableid, JET_prep.Insert))
                {
                    update.Save();
                }
            }

            // Create workers
            var workers = new Worker[2];
            var threads = new Thread[workers.Length];
            for (int i = 0; i < workers.Length; ++i)
            {
                JET_SESID workerSesid;
                Api.JetDupSession(sesid, out workerSesid);
                JET_DBID workerDbid;
                Api.JetOpenDatabase(workerSesid, TestDatabase, null, out workerDbid, OpenDatabaseGrbit.None);
                JET_TABLEID workerTableid;
                Api.JetOpenTable(workerSesid, workerDbid, "table", null, 0, OpenTableGrbit.None, out workerTableid);
                workers[i] = new Worker(workerSesid, workerTableid);
                threads[i] = new Thread(workers[i].DoWork);
            }

            // Run the workers then wait for them
            foreach (Thread t in threads)
            {
                t.Start();
            }

            foreach (Thread t in threads)
            {
                t.Join();
            }

            for (int i = 0; i < workers.Length; ++i)
            {
                Console.WriteLine("Worker {0} processed {1} records", i, workers[i].RecordsProcessed);
            }
        }

        /// <summary>
        /// A worker class that processes records in a table.
        /// </summary>
        private class Worker
        {
            /// <summary>
            /// The session being used.
            /// </summary>
            private readonly JET_SESID sesid;
            
            /// <summary>
            /// The table being scanned.
            /// </summary>
            private readonly JET_TABLEID tableid;
            
            /// <summary>
            /// Initializes a new instance of the Worker class.
            /// </summary>
            /// <param name="sesid">The session to use.</param>
            /// <param name="tableid">The table to operate on.</param>
            public Worker(JET_SESID sesid, JET_TABLEID tableid)
            {
                this.sesid = sesid;
                this.tableid = tableid;
            }

            /// <summary>
            /// Gets the number of records processed by this worker.
            /// </summary>
            public int RecordsProcessed { get; private set; }

            /// <summary>
            /// Process the records sequentially. This method tries
            /// to lock a record and moves to the next record if
            /// it fails to get the lock.
            /// </summary>
            public void DoWork()
            {
                // We must be in a transaction for locking to work.
                using (var transaction = new Transaction(this.sesid))
                {
                    if (Api.TryMoveFirst(this.sesid, this.tableid))
                    {
                        do
                        {
                            // Getting a lock in ESENT is instantaneous -- if
                            // another thread has the record locked or has 
                            // updated this record, this call will fail. There
                            // is no way to wait for the lock to be released.
                            // (because ESENT uses Snapshot Isolation the other
                            // session's lock will always be visible until this
                            // transaction commits).
                            if (Api.TryGetLock(this.sesid, this.tableid, GetLockGrbit.Write))
                            {
                                // [Do something]
                                Thread.Sleep(1);
                                Api.JetDelete(this.sesid, this.tableid);
                                this.RecordsProcessed++;
                            }
                        }
                        while (Api.TryMoveNext(this.sesid, this.tableid));
                    }

                    transaction.Commit(CommitTransactionGrbit.LazyFlush);
                }
            }
        }
    }
}