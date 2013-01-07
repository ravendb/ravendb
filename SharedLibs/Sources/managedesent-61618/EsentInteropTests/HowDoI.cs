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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;
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

            SetupHelper.CheckProcessForInstanceLeaks();
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
        /// Demonstrate how to seek by string wildcards.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Demonstrate how to seek by string wildcard")]
        public void HowDoISeekByStringWildcard()
        {
            JET_SESID sesid = this.testSession;
            JET_DBID dbid = this.testDbid;

            JET_TABLEID tableid;
            JET_COLUMNDEF columndef = new JET_COLUMNDEF();
            JET_COLUMNID keyColumn;

            // First create the table with a string key.
            Api.JetCreateTable(sesid, dbid, "table", 0, 100, out tableid);
            columndef.coltyp = JET_coltyp.LongText;
            columndef.cp = JET_CP.Unicode;
            Api.JetAddColumn(sesid, tableid, "key", columndef, null, 0, out keyColumn);

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
                foreach (string k in new[] { "A", "ANT", "B", "BAT", "C", "CAT", "D", "DOG" })
                {
                    using (var update = new Update(sesid, tableid, JET_prep.Insert))
                    {
                        Api.SetColumn(sesid, tableid, keyColumn, k, Encoding.Unicode);
                        update.Save();
                    }
                }

                transaction.Commit(CommitTransactionGrbit.LazyFlush);
            }

            // We have an index over a string column, which contains 8 records:
            //  [A, ANT, B, BAT, C, CAT, D, DOG]
            // The following code demonstrates how to create index ranges over
            // all string prefix combinations. The three things that are varied are:
            //  1. The use of PartialColumnEndLimit
            //  2. The use of SeekGE or SeekGT
            //  3. The used of RangeInclusive

            // "A*" <= key <= "C*" -> ["A", "ANT", "B", "BAT", "C", "CAT"]
            Api.MakeKey(sesid, tableid, "A", Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.JetSeek(sesid, tableid, SeekGrbit.SeekGE);
            Api.MakeKey(sesid, tableid, "C", Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.PartialColumnEndLimit);
            Api.JetSetIndexRange(sesid, tableid, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
            CheckIndexRange(sesid, tableid, keyColumn, new[] { "A", "ANT", "B", "BAT", "C", "CAT" });

            // "A*" < key <= "C*" -> ["B", "BAT", "C", "CAT"]
            Api.MakeKey(sesid, tableid, "A", Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.PartialColumnEndLimit);
            Api.JetSeek(sesid, tableid, SeekGrbit.SeekGT);
            Api.MakeKey(sesid, tableid, "C", Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.PartialColumnEndLimit);
            Api.JetSetIndexRange(sesid, tableid, SetIndexRangeGrbit.RangeUpperLimit | SetIndexRangeGrbit.RangeInclusive);
            CheckIndexRange(sesid, tableid, keyColumn, new[] { "B", "BAT", "C", "CAT" });

            // "A*" <= key < "C*" -> ["A", "ANT", "B", "BAT"]
            Api.MakeKey(sesid, tableid, "A", Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.JetSeek(sesid, tableid, SeekGrbit.SeekGE);
            Api.MakeKey(sesid, tableid, "C", Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.JetSetIndexRange(sesid, tableid, SetIndexRangeGrbit.RangeUpperLimit);
            CheckIndexRange(sesid, tableid, keyColumn, new[] { "A", "ANT", "B", "BAT" });

            // "A*" < key < "C*" -> ["B", "BAT"]
            Api.MakeKey(sesid, tableid, "A", Encoding.Unicode, MakeKeyGrbit.NewKey | MakeKeyGrbit.PartialColumnEndLimit);
            Api.JetSeek(sesid, tableid, SeekGrbit.SeekGT);
            Api.MakeKey(sesid, tableid, "C", Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.JetSetIndexRange(sesid, tableid, SetIndexRangeGrbit.RangeUpperLimit);
            CheckIndexRange(sesid, tableid, keyColumn, new[] { "B", "BAT" });
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
        /// Demonstrate how to deal with key truncation.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Demonstrate how to deal with key truncation")]
        public void HowDoIDealWithKeyTruncation()
        {
            JET_SESID sesid = this.testSession;
            JET_DBID dbid = this.testDbid;

            JET_TABLEID tableid;
            JET_COLUMNDEF columndef = new JET_COLUMNDEF();
            JET_COLUMNID keyColumn;
            JET_COLUMNID dataColumn;

            // First create the table.
            Api.JetCreateTable(sesid, dbid, "table", 0, 100, out tableid);
            columndef.coltyp = JET_coltyp.LongText;
            columndef.cp = JET_CP.Unicode;
            Api.JetAddColumn(sesid, tableid, "key", columndef, null, 0, out keyColumn);
            columndef.coltyp = JET_coltyp.Long;
            Api.JetAddColumn(sesid, tableid, "data", columndef, null, 0, out dataColumn);

            // Now create a secondary, non-unique column on the string column.
            // ESENT keys are stored in a normalized form, which is typically 
            // larger than the source data, but allow for very fast seeks. By
            // default the maximum normalized key size is 255 bytes. Starting
            // with Windows Vista the maximum key size can be increased. Setting
            // cbKeyMost to SystemParameters.KeyMost will make ManagedEsent
            // create the index with the largest allowable key.
            const string KeyDescription = "+key\0\0";
            JET_INDEXCREATE[] indexcreates = new JET_INDEXCREATE[1];
            indexcreates[0] = new JET_INDEXCREATE
            {
                szIndexName = "secondary",
                szKey = KeyDescription,
                cbKey = KeyDescription.Length,
                cbKeyMost = SystemParameters.KeyMost,
                grbit = CreateIndexGrbit.None,
            };
            Api.JetCreateIndex2(sesid, tableid, indexcreates, indexcreates.Length);

            // Insert some records. The key has the same value for the first
            // 4096 characters and then is different. This string is too large
            // for even the largest key sizes to differentiate.
            // If the index was unique we would get a key duplicate error.
            string prefix = new string('x', 4096);
            using (var transaction = new Transaction(sesid))
            {
                int i = 0;
                foreach (string k in new[] { "a", "b", "c", "d" })
                {
                    using (var update = new Update(sesid, tableid, JET_prep.Insert))
                    {
                        string key = prefix + k;
                        Api.SetColumn(sesid, tableid, keyColumn, key, Encoding.Unicode);
                        Api.SetColumn(sesid, tableid, dataColumn, i++);
                        update.Save();
                    }
                }

                transaction.Commit(CommitTransactionGrbit.LazyFlush);
            }

            // Seek for a record. This demonstrates the problem with key truncation.
            // We seek for the key ending in 'd' but end up on the one ending in 'a'.
            string seekKey = prefix + "d";
            Api.JetSetCurrentIndex(sesid, tableid, "secondary");
            Api.MakeKey(sesid, tableid, prefix + "d", Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.JetSeek(sesid, tableid, SeekGrbit.SeekEQ);
            string actualKey = Api.RetrieveColumnAsString(sesid, tableid, keyColumn);
            Assert.AreNotEqual(seekKey, actualKey);
            Assert.AreEqual(prefix + "a", actualKey);

            // Seek for the record using a key range.
            Assert.IsTrue(TrySeekTruncatedString(sesid, tableid, seekKey, keyColumn));
            Assert.AreEqual(seekKey, Api.RetrieveColumnAsString(sesid, tableid, keyColumn));
            Assert.AreEqual(3, Api.RetrieveColumnAsInt32(sesid, tableid, dataColumn));

            Assert.IsFalse(TrySeekTruncatedString(sesid, tableid, prefix + "z", keyColumn));
            Assert.IsFalse(TrySeekTruncatedString(sesid, tableid, "foo", keyColumn));
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
        /// Demonstrate how to deal with multivalues.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Demonstrate how to deal with multivalues")]
        public void HowDoIDealWithMultivalues()
        {
            JET_SESID sesid = this.testSession;
            JET_DBID dbid = this.testDbid;

            JET_TABLEID tableid;
            JET_COLUMNDEF columndef = new JET_COLUMNDEF();
            JET_COLUMNID tagColumn;

            Api.JetCreateTable(sesid, dbid, "table", 0, 100, out tableid);

            // Create the column. Any column can be multivalued. Using
            // ColumndefGrbit controls how the column is indexed.
            columndef.coltyp = JET_coltyp.LongText;
            columndef.cp = JET_CP.Unicode;
            columndef.grbit = ColumndefGrbit.ColumnMultiValued;
            Api.JetAddColumn(sesid, tableid, "tags", columndef, null, 0, out tagColumn);

            // Create the index. There will be one entry in the index for each
            // instance of the multivalued column.
            const string IndexKey = "+tags\0\0";
            Api.JetCreateIndex(sesid, tableid, "tagsindex", CreateIndexGrbit.None, IndexKey, IndexKey.Length, 100);

            Api.JetBeginTransaction(sesid);

            // Now insert a record. An ESENT column can have multiple instances (multivalues)
            // inside the same record. Each multivalue is identified by an itag, the first itag
            // in a sequence is 1.
            Api.JetPrepareUpdate(sesid, tableid, JET_prep.Insert);

            // With no JET_SETINFO specified, itag 1 will be set.
            byte[] data = Encoding.Unicode.GetBytes("foo");
            Api.JetSetColumn(sesid, tableid, tagColumn, data, data.Length, SetColumnGrbit.None, null);

            // Set a column with a setinfo. The itagSequence in the setinfo will be 0, which 
            // means the value will be added to the collection of values, i.e. the column will
            // have two instances, "foo" and "bar".
            JET_SETINFO setinfo = new JET_SETINFO();
            data = Encoding.Unicode.GetBytes("bar");
            Api.JetSetColumn(sesid, tableid, tagColumn, data, data.Length, SetColumnGrbit.None, setinfo);

            // Add a third instance, explicitly setting the itagSequence
            data = Encoding.Unicode.GetBytes("baz");
            setinfo.itagSequence = 4;
            Api.JetSetColumn(sesid, tableid, tagColumn, data, data.Length, SetColumnGrbit.None, setinfo);

            // Add a duplicate value, checking for uniqueness
            data = Encoding.Unicode.GetBytes("foo");
            setinfo.itagSequence = 0;
            try
            {
                Api.JetSetColumn(sesid, tableid, tagColumn, data, data.Length, SetColumnGrbit.UniqueMultiValues, setinfo);
                Assert.Fail("Expected an EsentErrorException");
            }
            catch (EsentErrorException ex)
            {
                Assert.AreEqual(JET_err.MultiValuedDuplicate, ex.Error);
            }

            Api.JetUpdate(sesid, tableid);

            // Find the record. We can seek for any column instance.
            Api.JetSetCurrentIndex(sesid, tableid, "tagsindex");
            Api.MakeKey(sesid, tableid, "bar", Encoding.Unicode, MakeKeyGrbit.NewKey);
            Assert.IsTrue(Api.TrySeek(sesid, tableid, SeekGrbit.SeekEQ));

            // Retrieve the number of column instances. This can be done with JetRetrieveColumns by setting
            // itagSequence to 0.
            JET_RETRIEVECOLUMN retrievecolumn = new JET_RETRIEVECOLUMN();
            retrievecolumn.columnid = tagColumn;
            retrievecolumn.itagSequence = 0;
            Api.JetRetrieveColumns(sesid, tableid, new[] { retrievecolumn }, 1); 
            Console.WriteLine("{0}", retrievecolumn.itagSequence);
            Assert.AreEqual(3, retrievecolumn.itagSequence);

            // Retrieve all the columns
            for (int itag = 1; itag <= retrievecolumn.itagSequence; ++itag)
            {
                JET_RETINFO retinfo = new JET_RETINFO { itagSequence = itag };
                string s = Encoding.Unicode.GetString(Api.RetrieveColumn(sesid, tableid, tagColumn, RetrieveColumnGrbit.None, retinfo));
                Console.WriteLine("{0}: {1}", itag, s);
            }

            // Update the record
            Api.JetPrepareUpdate(sesid, tableid, JET_prep.Replace);

            // With no JET_SETINFO specified, itag 1 will be set, overwriting the existing value.
            data = Encoding.Unicode.GetBytes("qux");
            Api.JetSetColumn(sesid, tableid, tagColumn, data, data.Length, SetColumnGrbit.None, null);

            // Set an instance to null to delete it.
            setinfo.itagSequence = 2;
            Api.JetSetColumn(sesid, tableid, tagColumn, null, 0, SetColumnGrbit.None, setinfo);

            // Removing itag 2 moved the other itags down (i.e. itag 3 became itag 2).
            // Overwrite itag 2.
            data = Encoding.Unicode.GetBytes("xyzzy");
            setinfo.itagSequence = 2;
            Api.JetSetColumn(sesid, tableid, tagColumn, data, data.Length, SetColumnGrbit.None, setinfo);

            // Now add a new instance by setting itag 0. This instance will go at the end.
            data = Encoding.Unicode.GetBytes("flob");
            setinfo.itagSequence = 0;
            Api.JetSetColumn(sesid, tableid, tagColumn, data, data.Length, SetColumnGrbit.None, setinfo);

            Api.JetUpdate(sesid, tableid);

            // Retrieve the number of column instances again.
            retrievecolumn.itagSequence = 0;
            Api.JetRetrieveColumns(sesid, tableid, new[] { retrievecolumn }, 1);

            // Retrieve all the columns
            for (int itag = 1; itag <= retrievecolumn.itagSequence; ++itag)
            {
                JET_RETINFO retinfo = new JET_RETINFO { itagSequence = itag };
                string s = Encoding.Unicode.GetString(Api.RetrieveColumn(sesid, tableid, tagColumn, RetrieveColumnGrbit.None, retinfo));
                Console.WriteLine("{0}: {1}", itag, s);
            }

            Api.JetCommitTransaction(sesid, CommitTransactionGrbit.LazyFlush);
        }

        /// <summary>
        /// Demonstrate how to get the Vista GUID normalization behaviour on XP.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Demonstrate how to get the Vista GUID normalization behaviour on XP")]
        public void HowDoINormalizeAGuidOnXp()
        {
            if (!EsentVersion.SupportsVistaFeatures)
            {
                return;
            }

            JET_SESID sesid = this.testSession;
            JET_DBID dbid = this.testDbid;

            JET_TABLEID tableid;
            JET_COLUMNDEF columndef = new JET_COLUMNDEF();
            JET_COLUMNID guidColumn, binaryColumn;

            Api.JetCreateTable(sesid, dbid, "table", 0, 100, out tableid);

            columndef.coltyp = VistaColtyp.GUID;
            Api.JetAddColumn(sesid, tableid, "guid", columndef, null, 0, out guidColumn);
            columndef.coltyp = JET_coltyp.Binary;
            Api.JetAddColumn(sesid, tableid, "guid_as_binary", columndef, null, 0, out binaryColumn);

            const string GuidIndexKey = "+guid\0\0";
            Api.JetCreateIndex(sesid, tableid, "guid", CreateIndexGrbit.IndexUnique, GuidIndexKey, GuidIndexKey.Length, 100);
            const string BinaryIndexKey = "+guid_as_binary\0\0";
            Api.JetCreateIndex(sesid, tableid, "binary", CreateIndexGrbit.IndexUnique, BinaryIndexKey, BinaryIndexKey.Length, 100);

            Api.JetBeginTransaction(sesid);
            foreach (Guid g in from x in Enumerable.Range(0, 200) select Guid.NewGuid())
            {
                Api.JetPrepareUpdate(sesid, tableid, JET_prep.Insert);
                Api.SetColumn(sesid, tableid, guidColumn, g);

                byte[] input = g.ToByteArray();
                int[] transform = new[] { 10, 11, 12, 13, 14, 15, 8, 9, 6, 7, 4, 5, 0, 1, 2, 3 };
                byte[] transformed = (from i in transform select input[i]).ToArray();

                Api.SetColumn(sesid, tableid, binaryColumn, transformed);
                Api.JetUpdate(sesid, tableid);
            }

            Api.JetCommitTransaction(sesid, CommitTransactionGrbit.LazyFlush);

            Api.JetSetCurrentIndex(sesid, tableid, "guid");
            Guid[] guidOrder = GetGuids(sesid, tableid, guidColumn).ToArray();
            Api.JetSetCurrentIndex(sesid, tableid, "binary");
            Guid[] binaryOrder = GetGuids(sesid, tableid, guidColumn).ToArray();

            CollectionAssert.AreEqual(guidOrder, binaryOrder);
        }

        /// <summary>
        /// Check that records in an index range have the expected values.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The cursor with the index range setup.</param>
        /// <param name="columnid">The column to retrieve.</param>
        /// <param name="expected">The values we expect.</param>
        private static void CheckIndexRange(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid, ICollection<string> expected)
        {
            ICollection<string> actual = new List<string>();
            do
            {
                actual.Add(Api.RetrieveColumnAsString(sesid, tableid, columnid));
            }
            while (Api.TryMoveNext(sesid, tableid));

            Assert.AreEqual(expected.Count, actual.Count, "Wrong number of records returned");
            Assert.AreEqual(expected.Except(actual).Count(), 0, "Wrong elements returned");
        }

        /// <summary>
        /// Enumerate all records in a table returning the values of the specified column.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to use.</param>
        /// <param name="columnid">The column to retrieve.</param>
        /// <returns>An enumeration of the column values as guids.</returns>
        private static IEnumerable<Guid> GetGuids(JET_SESID sesid, JET_TABLEID tableid, JET_COLUMNID columnid)
        {
            Api.MoveBeforeFirst(sesid, tableid);
            while (Api.TryMoveNext(sesid, tableid))
            {
                yield return Api.RetrieveColumnAsGuid(sesid, tableid, columnid).Value;
            }
        }

        /// <summary>
        /// Seek for a string match on a non-unique secondary index.
        /// </summary>
        /// <param name="sesid">The session to use.</param>
        /// <param name="tableid">The table to seek on.</param>
        /// <param name="key">The key to look for.</param>
        /// <param name="keyColumn">The value of the key column.</param>
        /// <returns>True if a record was found, false otherwise.</returns>
        private static bool TrySeekTruncatedString(JET_SESID sesid, JET_TABLEID tableid, string key, JET_COLUMNID keyColumn)
        {
            // To find the record we want we can set up an index range and iterate
            // through it until we find the record we want. We use the desired key
            // as both the start and end of the range, along with the inclusive flag.
            Api.MakeKey(sesid, tableid, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            if (!Api.TrySeek(sesid, tableid, SeekGrbit.SeekEQ))
            {
                return false;
            }

            Api.MakeKey(sesid, tableid, key, Encoding.Unicode, MakeKeyGrbit.NewKey);
            Api.JetSetIndexRange(sesid, tableid, SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit);

            while (key != Api.RetrieveColumnAsString(sesid, tableid, keyColumn))
            {
                if (!Api.TryMoveNext(sesid, tableid))
                {
                    return false;
                }
            }

            return true;
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
                Thread.BeginThreadAffinity();

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

                Thread.EndThreadAffinity();
            }
        }
    }
}