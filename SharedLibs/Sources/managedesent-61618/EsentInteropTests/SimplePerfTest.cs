//-----------------------------------------------------------------------
// <copyright file="SimplePerfTest.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.InteropServices;
    using System.Threading;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Basic performance tests
    /// </summary>
    [TestClass]
    public class SimplePerfTest
    {
        /// <summary>
        /// Size of the data being inserted in the data column.
        /// </summary>
        private const int DataSize = 32;

        /// <summary>
        /// The name of the table.
        /// </summary>
        private const string TableName = "table";

        /// <summary>
        /// The name of the key column.
        /// </summary>
        private const string KeyColumnName = "Key";

        /// <summary>
        /// The name of the data column.
        /// </summary>
        private const string DataColumnName = "Data";

        /// <summary>
        /// The name of the table's index.
        /// </summary>
        private const string IndexName = "primary";

        /// <summary>
        /// The directory to put the database files in.
        /// </summary>
        private string directory;

        /// <summary>
        /// The path to the database.
        /// </summary>
        private string database;

        /// <summary>
        /// The instance to use.
        /// </summary>
        private Instance instance;
        
        /// <summary>
        /// The session to use.
        /// </summary>
        private Session session;
        
        /// <summary>
        /// Random number generation object.
        /// </summary>
        private Random random;
        
        /// <summary>
        /// Previous minimum cache size. Used to restore the previous setting.
        /// </summary>
        private int cacheSizeMinSaved = 0;

        /// <summary>
        /// Setup for a test -- this creates the database
        /// </summary>
        [TestInitialize]
        [Description("Setup the SimplePerfTest fixture")]
        public void Setup()
        {
            this.directory = SetupHelper.CreateRandomDirectory();

            this.random = new Random();

            JET_DBID dbid;

            string ignored;
            Api.JetGetSystemParameter(
                JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.CacheSizeMin, ref this.cacheSizeMinSaved, out ignored, 0);
            Api.JetSetSystemParameter(JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.CacheSizeMin, 16384, null);

            this.instance = new Instance(Guid.NewGuid().ToString(), "SimplePerfTest");
            this.instance.Parameters.LogFileDirectory = this.directory;
            this.instance.Parameters.SystemDirectory = this.directory;
            this.instance.Parameters.MaxVerPages = 1024;
            this.instance.Parameters.Recovery = false;

            // Create the instance, database and table
            this.instance.Init();
            this.session = new Session(this.instance);
            this.database = Path.Combine(this.directory, "esentperftest.db");
            Api.JetCreateDatabase(this.session, this.database, string.Empty, out dbid, CreateDatabaseGrbit.None);

            // Create a dummy table to force the database to grow
            using (var trx = new Transaction(this.session))
            {
                JET_TABLEID tableid;
                Api.JetCreateTable(this.session, dbid, "dummy_table", 64 * 1024 * 1024 / SystemParameters.DatabasePageSize, 100, out tableid);
                Api.JetCloseTable(this.session, tableid);
                Api.JetDeleteTable(this.session, dbid, "dummy_table");
                trx.Commit(CommitTransactionGrbit.LazyFlush);
            }

            // Create the table
            using (var trx = new Transaction(this.session))
            {
                JET_COLUMNID columnid;
                JET_TABLEID tableid;
                var columndef = new JET_COLUMNDEF();

                Api.JetCreateTable(this.session, dbid, TableName, 0, 100, out tableid);
                columndef.coltyp = JET_coltyp.Currency;
                Api.JetAddColumn(this.session, tableid, KeyColumnName, columndef, null, 0, out columnid);
                columndef.coltyp = JET_coltyp.Binary;
                Api.JetAddColumn(this.session, tableid, DataColumnName, columndef, null, 0, out columnid);
                Api.JetCreateIndex(this.session, tableid, IndexName, CreateIndexGrbit.IndexPrimary, "+key\0\0", 6, 100);
                Api.JetCloseTable(this.session, tableid);
                trx.Commit(CommitTransactionGrbit.None);
            }

            // Reset the key for the worker thread
            PerfTestWorker.NextKey = 0;
        }

        /// <summary>
        /// Cleanup after the test
        /// </summary>
        [TestCleanup]
        [Description("Cleanup the SimplePerfTest fixture")]
        public void Teardown()
        {
            this.session.End();
            this.instance.Term();
            Api.JetSetSystemParameter(JET_INSTANCE.Nil, JET_SESID.Nil, JET_param.CacheSizeMin, this.cacheSizeMinSaved, null);
            Cleanup.DeleteDirectoryWithRetry(this.directory);
        }

        /// <summary>
        /// Test inserting and retrieving records.
        /// </summary>
        [TestMethod]
        [Priority(4)]
        [Description("Run a basic performance test")]
        public void BasicPerfTest()
        {
            CheckMemoryUsage(this.InsertReadSeek);
        }

        /// <summary>
        /// Test inserting and retrieving records with multiple threads.
        /// </summary>
        [TestMethod]
        [Priority(4)]
        [Description("Run a basic multithreaded stress test")]
        public void BasicMultithreadedStressTest()
        {
            CheckMemoryUsage(this.MultithreadedStress);
        }

        /// <summary>
        /// Run garbage collection.
        /// </summary>
        private static void RunGarbageCollection()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        /// <summary>
        /// Perform an action, checking the system's memory usage before and after.
        /// </summary>
        /// <param name="action">The action to perform.</param>
        private static void CheckMemoryUsage(Action action)
        {
            RunGarbageCollection();
            long memoryAtStart = GC.GetTotalMemory(true);
            int collectionCountAtStart = GC.CollectionCount(0);

            action();

            int collectionCountAtEnd = GC.CollectionCount(0);
            RunGarbageCollection();
            long memoryAtEnd = GC.GetTotalMemory(true);
            Console.WriteLine(
                "Memory changed by {0:N} bytes ({1} GC cycles)",
                memoryAtEnd - memoryAtStart,
                collectionCountAtEnd - collectionCountAtStart);
        }

        /// <summary>
        /// Perform and time the given action.
        /// </summary>
        /// <param name="name">The name of the action.</param>
        /// <param name="action">The operation to perform.</param>
        private static void TimeAction(string name, Action action)
        {
            var stopwatch = EsentStopwatch.StartNew();
            action();
            stopwatch.Stop();
            Console.WriteLine("{0}: {1} ({2})", name, stopwatch.Elapsed, stopwatch.ThreadStats);
        }

        /// <summary>
        /// Run multithreaded operations.
        /// </summary>
        /// <param name="worker">The worker to use.</param>
        private static void StressThread(PerfTestWorker worker)
        {
            const int NumRecords = 50000;
            const int NumRetrieves = 100;

            worker.InsertRecordsWithSetColumn(NumRecords);
            worker.RepeatedlyRetrieveOneRecord(NumRetrieves);
            worker.RepeatedlyRetrieveOneRecordWithJetRetrieveColumns(NumRetrieves);
            worker.RepeatedlyRetrieveOneRecordWithRetrieveColumns(NumRetrieves);
            worker.RepeatedlyRetrieveOneRecordWithEnumColumns(NumRetrieves);
            worker.RetrieveAllRecords();

            worker.InsertRecordsWithSetColumns(NumRecords);
            worker.RepeatedlyRetrieveOneRecord(NumRetrieves);
            worker.RepeatedlyRetrieveOneRecordWithJetRetrieveColumns(NumRetrieves);
            worker.RepeatedlyRetrieveOneRecordWithRetrieveColumns(NumRetrieves);
            worker.RepeatedlyRetrieveOneRecordWithEnumColumns(NumRetrieves);
            worker.RetrieveAllRecords();
        }

        /// <summary>
        /// Randomly shuffle an array.
        /// </summary>
        /// <typeparam name="T">The type of the array.</typeparam>
        /// <param name="arrayToShuffle">The array to shuffle.</param>
        private void Shuffle<T>(T[] arrayToShuffle)
        {
            for (int i = 0; i < arrayToShuffle.Length; ++i)
            {
                int swap = this.random.Next(i, arrayToShuffle.Length);
                T temp = arrayToShuffle[i];
                arrayToShuffle[i] = arrayToShuffle[swap];
                arrayToShuffle[swap] = temp;
            }
        }

        /// <summary>
        /// Get keys in the range (0, numKeys] in a random order.
        /// </summary>
        /// <param name="numKeys">The number of keys that are wanted.</param>
        /// <returns>Keys in the range (0, numKeys] in random order.</returns>
        private long[] GetRandomKeys(int numKeys)
        {
            long[] keys = (from x in Enumerable.Range(0, numKeys) select (long)x).ToArray();
            this.Shuffle(keys);
            return keys;
        }

        /// <summary>
        /// Perform a PerfTestWorker action on a separate thread.
        /// </summary>
        /// <param name="action">The action to perform.</param>
        /// <returns>The thread.</returns>
        private Thread StartWorkerThread(Action<PerfTestWorker> action)
        {
            var thread = new Thread(
                () =>
                {
                    using (var worker = new PerfTestWorker(this.instance, this.database))
                    {
                        action(worker);
                    }
                });
            return thread;
        }

        /// <summary>
        /// Insert some records and then retrieve them.
        /// </summary>
        private void InsertReadSeek()
        {
            const int NumRecords = 1000000;

            long[] keys = this.GetRandomKeys(NumRecords);

            using (var worker = new PerfTestWorker(this.instance, this.database))
            {
                TimeAction("Insert records", () => worker.InsertRecordsWithSetColumn(NumRecords / 2));
                TimeAction("Insert records with SetColumns", () => worker.InsertRecordsWithSetColumns(NumRecords / 2));
                TimeAction("Read one record", () => worker.RepeatedlyRetrieveOneRecord(NumRecords));
                TimeAction("Read one record with JetRetrieveColumns", () => worker.RepeatedlyRetrieveOneRecordWithJetRetrieveColumns(NumRecords));
                TimeAction("Read one record with RetrieveColumns", () => worker.RepeatedlyRetrieveOneRecordWithRetrieveColumns(NumRecords));
                TimeAction("Read one record with JetEnumerateColumns", () => worker.RepeatedlyRetrieveOneRecordWithEnumColumns(NumRecords));
                TimeAction("Read all records", worker.RetrieveAllRecords);
                TimeAction("Seek to all records", () => worker.SeekToAllRecords(keys));
            }
        }

        /// <summary>
        /// Insert some records and then retrieve them.
        /// </summary>
        private void MultithreadedStress()
        {
            const int NumThreads = 8;
            Thread[] threads = (from i in Enumerable.Repeat(0, NumThreads) select this.StartWorkerThread(StressThread)).ToArray();
            foreach (Thread thread in threads)
            {
                thread.Start();
            }

            foreach (Thread thread in threads)
            {
                thread.Join();
            }
        }

        /// <summary>
        /// Worker for the performance test.
        /// </summary>
        internal class PerfTestWorker : IDisposable
        {
            /// <summary>
            /// The instance to use.
            /// </summary>
            private readonly JET_INSTANCE instance;

            /// <summary>
            /// The database to use.
            /// </summary>
            private readonly string database;

            /// <summary>
            /// The session to use.
            /// </summary>
            private readonly Session session;

            /// <summary>
            /// The id of the database.
            /// </summary>
            private readonly JET_DBID dbid;

            /// <summary>
            /// The table to use.
            /// </summary>
            private readonly Table table;

            /// <summary>
            /// The columnid of the key column.
            /// </summary>
            private readonly JET_COLUMNID columnidKey;

            /// <summary>
            /// The columnid of the data column.
            /// </summary>
            private readonly JET_COLUMNID columnidData;

            /// <summary>
            /// Data to be inserted into the data column.
            /// </summary>
            private readonly byte[] data;

            /// <summary>
            /// Used to retrieve the data column.
            /// </summary>
            private readonly byte[] dataBuf;

            /// <summary>
            /// The next key value to be inserted. Used to insert records.
            /// </summary>
            private static long nextKey = 0;

            /// <summary>
            /// The key of the last record to be inserted.
            /// </summary>
            private long lastKey;

            /// <summary>
            /// Initializes a new instance of the <see cref="PerfTestWorker"/> class.
            /// </summary>
            /// <param name="instance">
            /// The instance to use.
            /// </param>
            /// <param name="database">
            /// Path to the database. The database should already be created.
            /// </param>
            public PerfTestWorker(JET_INSTANCE instance, string database)
            {
                Thread.BeginThreadAffinity();
                this.instance = instance;
                this.database = database;
                this.session = new Session(this.instance);
                Api.JetOpenDatabase(this.session, this.database, String.Empty, out this.dbid, OpenDatabaseGrbit.None);
                this.table = new Table(this.session, this.dbid, SimplePerfTest.TableName, OpenTableGrbit.None);
                this.columnidKey = Api.GetTableColumnid(this.session, this.table, SimplePerfTest.KeyColumnName);
                this.columnidData = Api.GetTableColumnid(this.session, this.table, SimplePerfTest.DataColumnName);

                this.data = new byte[SimplePerfTest.DataSize];
                this.dataBuf = new byte[SimplePerfTest.DataSize];
            }

            /// <summary>
            /// Finalizes an instance of the <see cref="PerfTestWorker"/> class. 
            /// </summary>
            ~PerfTestWorker()
            {
                this.Dispose(false);
            }

            /// <summary>
            /// Sets the next key value to be inserted. Used to insert records.
            /// </summary>
            public static long NextKey
            {
                set { nextKey = value; }
            }

            /// <summary>
            /// Disposes an instance of the PerfTestWorker class.
            /// </summary>
            public void Dispose()
            {
                this.Dispose(true);
            }

            /// <summary>
            /// Insert multiple records.
            /// </summary>
            /// <param name="numRecords">The number of records to insert.</param>
            public void InsertRecordsWithSetColumn(int numRecords)
            {
                for (int i = 0; i < numRecords; ++i)
                {
                    Api.JetBeginTransaction(this.session);
                    this.InsertRecordWithSetColumn();
                    Api.JetCommitTransaction(this.session, CommitTransactionGrbit.LazyFlush);
                }
            }

            /// <summary>
            /// Insert multiple records with the <see cref="Api.SetColumns"/> API.
            /// </summary>
            /// <param name="numRecords">The number of records to insert.</param>
            public void InsertRecordsWithSetColumns(int numRecords)
            {
                var keyColumn = new Int64ColumnValue { Columnid = this.columnidKey };
                var dataColumn = new BytesColumnValue { Columnid = this.columnidData, Value = this.data };

                var columns = new ColumnValue[] { keyColumn, dataColumn };

                for (int i = 0; i < numRecords; ++i)
                {
                    Api.JetBeginTransaction(this.session);
                    Api.JetPrepareUpdate(this.session, this.table, JET_prep.Insert);
                    keyColumn.Value = this.GetNextKey();
                    Api.SetColumns(this.session, this.table, columns);
                    Api.JetUpdate(this.session, this.table);
                    Api.JetCommitTransaction(this.session, CommitTransactionGrbit.LazyFlush);
                }
            }

            /// <summary>
            /// Retrieve all records in the table.
            /// </summary>
            public void RetrieveAllRecords()
            {
                Api.MoveBeforeFirst(this.session, this.table);
                while (Api.TryMoveNext(this.session, this.table))
                {
                    Api.JetBeginTransaction(this.session);
                    this.RetrieveRecord();
                    Api.JetCommitTransaction(this.session, CommitTransactionGrbit.LazyFlush);
                }
            }

            /// <summary>
            /// Retrieve the current record multiple times.
            /// </summary>
            /// <param name="numRetrieves">The number of times to retrieve the record.</param>
            public void RepeatedlyRetrieveOneRecord(int numRetrieves)
            {
                Api.JetMove(this.session, this.table, JET_Move.First, MoveGrbit.None);
                for (int i = 0; i < numRetrieves; ++i)
                {
                    Api.JetBeginTransaction(this.session);
                    this.RetrieveRecord();
                    Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
                }
            }

            /// <summary>
            /// Repeatedly retrieve one record using <see cref="Api.JetRetrieveColumns"/>.
            /// </summary>
            /// <param name="numRetrieves">The number of times to retrieve the record.</param>
            public void RepeatedlyRetrieveOneRecordWithJetRetrieveColumns(int numRetrieves)
            {
                this.SeekToLastRecordInserted();

                var keyBuffer = new byte[sizeof(long)];
                var retcols = new[]
                {
                    new JET_RETRIEVECOLUMN
                    {
                        columnid = this.columnidKey,
                        pvData = keyBuffer,
                        cbData = keyBuffer.Length,
                        itagSequence = 1,
                    },
                    new JET_RETRIEVECOLUMN
                    {
                        columnid = this.columnidData,
                        pvData = this.dataBuf,
                        cbData = this.dataBuf.Length,
                        itagSequence = 1,
                    },
                };

                for (int i = 0; i < numRetrieves; ++i)
                {
                    Api.JetBeginTransaction(this.session);
                    Api.JetRetrieveColumns(this.session, this.table, retcols, retcols.Length);
                    Assert.AreEqual(this.lastKey, BitConverter.ToInt64(keyBuffer, 0));
                    Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
                }
            }

            /// <summary>
            /// Repeatedly retrieve one record using <see cref="Api.RetrieveColumns"/>.
            /// </summary>
            /// <param name="numRetrieves">The number of times to retrieve the record.</param>
            public void RepeatedlyRetrieveOneRecordWithRetrieveColumns(int numRetrieves)
            {
                this.SeekToLastRecordInserted();

                var columnValues = new ColumnValue[]
                {
                    new Int64ColumnValue { Columnid = this.columnidKey },
                    new BytesColumnValue { Columnid = this.columnidData },
                };

                for (int i = 0; i < numRetrieves; ++i)
                {
                    Api.JetBeginTransaction(this.session);
                    Api.RetrieveColumns(this.session, this.table, columnValues);
                    Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
                }
            }

            /// <summary>
            /// Repeatedly retrieve one record using <see cref="Api.JetEnumerateColumns"/>.
            /// </summary>
            /// <param name="numRetrieves">The number of times to retrieve the record.</param>
            public void RepeatedlyRetrieveOneRecordWithEnumColumns(int numRetrieves)
            {
                this.SeekToLastRecordInserted();

                var columnids = new[]
                {
                    new JET_ENUMCOLUMNID { columnid = this.columnidKey },
                    new JET_ENUMCOLUMNID { columnid = this.columnidData },
                };
                JET_PFNREALLOC allocator = (context, pv, cb) => IntPtr.Zero == pv ? Marshal.AllocHGlobal(new IntPtr(cb)) : Marshal.ReAllocHGlobal(pv, new IntPtr(cb));

                for (int i = 0; i < numRetrieves; ++i)
                {
                    Api.JetBeginTransaction(this.session);
                    int numValues;
                    JET_ENUMCOLUMN[] values;
                    Api.JetEnumerateColumns(
                        this.session,
                        this.table,
                        columnids.Length,
                        columnids,
                        out numValues,
                        out values,
                        allocator,
                        IntPtr.Zero,
                        0,
                        EnumerateColumnsGrbit.EnumerateCompressOutput);
                    Marshal.ReadInt32(values[0].pvData);
                    allocator(IntPtr.Zero, values[0].pvData, 0);
                    allocator(IntPtr.Zero, values[1].pvData, 0);
                    Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
                }
            }

            /// <summary>
            /// Seek to, and retrieve the key column from, the specified records.
            /// </summary>
            /// <param name="keys">The keys of the records to retrieve.</param>
            public void SeekToAllRecords(IEnumerable<long> keys)
            {
                foreach (long key in keys)
                {
                    Api.JetBeginTransaction(this.session);
                    Api.MakeKey(this.session, this.table, key, MakeKeyGrbit.NewKey);
                    Api.JetSeek(this.session, this.table, SeekGrbit.SeekEQ);
                    Assert.AreEqual(key, Api.RetrieveColumnAsInt64(this.session, this.table, this.columnidKey));
                    Api.JetCommitTransaction(this.session, CommitTransactionGrbit.None);
                }
            }

            /// <summary>
            /// Called for the disposer and finalizer.
            /// </summary>
            /// <param name="isDisposing">True if called from dispose.</param>
            protected virtual void Dispose(bool isDisposing)
            {
                if (isDisposing)
                {
                    this.session.Dispose();
                }

                Thread.EndThreadAffinity();
            }

            /// <summary>
            /// Get the key for the next record to be inserted.
            /// </summary>
            /// <returns>The next key to use.</returns>
            private long GetNextKey()
            {
                this.lastKey = Interlocked.Increment(ref nextKey) - 1;
                return this.lastKey;
            }

            /// <summary>
            /// Seek to the last key that was inserted.
            /// </summary>
            private void SeekToLastRecordInserted()
            {
                Api.MakeKey(this.session, this.table, this.lastKey, MakeKeyGrbit.NewKey);
                Api.JetSeek(this.session, this.table, SeekGrbit.SeekEQ);                
            }

            /// <summary>
            /// Insert a record.
            /// </summary>
            private void InsertRecordWithSetColumn()
            {
                long key = this.GetNextKey();
                Api.JetPrepareUpdate(this.session, this.table, JET_prep.Insert);
                Api.SetColumn(this.session, this.table, this.columnidKey, key);
                Api.SetColumn(this.session, this.table, this.columnidData, this.data);
                Api.JetUpdate(this.session, this.table);
            }

            /// <summary>
            /// Retrieve the current record.
            /// </summary>
            private void RetrieveRecord()
            {
                int actualSize;
                Api.RetrieveColumnAsInt64(this.session, this.table, this.columnidKey);
                Api.JetRetrieveColumn(
                    this.session,
                    this.table,
                    this.columnidData,
                    this.dataBuf,
                    this.dataBuf.Length,
                    out actualSize,
                    RetrieveColumnGrbit.None,
                    null);
            }
        }
    }
}
