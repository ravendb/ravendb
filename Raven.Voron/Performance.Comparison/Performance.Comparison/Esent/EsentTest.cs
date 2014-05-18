namespace Performance.Comparison.Esent
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;

    using Microsoft.Isam.Esent.Interop;

    public class EsentTest : StoragePerformanceTestBase
    {
        private readonly string _path;

        private readonly Configurator _configurator;
        private string _database;

        public override string StorageName
        {
            get
            {
                return "Esent";
            }
        }

        public EsentTest(string path, byte[] buffer)
            : base(buffer)
        {
            _path = Path.Combine(path, "esent");
            _configurator = new Configurator();
            _database = Path.Combine(_path, "edbtest.db");
        }

        public Instance CreateInstance(bool delete = true)
        {
            if (delete && Directory.Exists(_path))
                Directory.Delete(_path, true);

            if (!Directory.Exists(_path))
                Directory.CreateDirectory(_path);

            var instance = new Instance(Guid.NewGuid().ToString());

            _configurator.ConfigureInstance(instance, _path);

            instance.Init();

            if (!delete)
                return instance;

            CreateSchema(instance);

            return instance;
        }

        private Session OpenSession(JET_INSTANCE instance, out Table table, out JET_COLUMNID primaryColumnId, out JET_COLUMNID secondaryColumnId)
        {
            var session = new Session(instance);
            Api.JetAttachDatabase2(session, _database, 0, AttachDatabaseGrbit.None);

            JET_DBID dbid;
            Api.JetOpenDatabase(session, _database, null, out dbid, OpenDatabaseGrbit.None);

            table = OpenSchema(session, dbid, out primaryColumnId, out secondaryColumnId);

            return session;
        }

        private Table OpenSchema(JET_SESID sesid, JET_DBID dbid, out JET_COLUMNID primaryColumnId, out JET_COLUMNID secondaryColumnId)
        {
            var table = new Table(sesid, dbid, "table", OpenTableGrbit.None);

            primaryColumnId = Api.GetTableColumnid(sesid, table, "key");
            secondaryColumnId = Api.GetTableColumnid(sesid, table, "data");

            return table;
        }

        private void CreateSchema(Instance instance)
        {
            using (var session = new Session(instance))
            {
                JET_DBID dbid;
                Api.JetCreateDatabase(session, _database, null, out dbid, CreateDatabaseGrbit.OverwriteExisting);

                using (var tx = new Transaction(session))
                {
                    JET_TABLEID tableid;
                    Api.JetCreateTable(session, dbid, "table", 0, 100, out tableid);
                    var primaryColumn = new JET_COLUMNDEF
                    {
                        coltyp = JET_coltyp.Long
                    };

                    var secondaryColumn = new JET_COLUMNDEF
                    {
                        coltyp = JET_coltyp.LongBinary,
                    };

                    JET_COLUMNID primaryColumnId;
                    Api.JetAddColumn(session, tableid, "key", primaryColumn, null, 0, out primaryColumnId);
                    JET_COLUMNID secondaryColumnId;
                    Api.JetAddColumn(session, tableid, "data", secondaryColumn, null, 0, out secondaryColumnId);

                    var index = new JET_INDEXCREATE
                    {
                        szKey = "+key\0\0",
                        szIndexName = "by_key",
                        grbit = CreateIndexGrbit.IndexPrimary,
                        ulDensity = 90
                    };

                    Api.JetCreateIndex(session, tableid, index.szIndexName, index.grbit, index.szKey, index.szKey.Length, index.ulDensity);

                    tx.Commit(CommitTransactionGrbit.None);
                }
            }
        }

        public override List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[Esent] sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override List<PerformanceRecord> WriteParallelSequential(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            return WriteParallel(string.Format("[Esent] parallel sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, numberOfThreads, out elapsedMilliseconds);
        }

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[Esent] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override List<PerformanceRecord> WriteParallelRandom(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            return WriteParallel(string.Format("[Esent] parallel random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, numberOfThreads, out elapsedMilliseconds);
        }

        public override PerformanceRecord ReadSequential(PerfTracker perfTracker)
        {
			var sequentialIds = Enumerable.Range(0, Constants.ReadItems).Select(x => (uint)x); ;

            return Read(string.Format("[Esent] sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads)
        {
			var sequentialIds = Enumerable.Range(0, Constants.ReadItems).Select(x => (uint)x); ;

            return ReadParallel(string.Format("[Esent] parallel sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker, numberOfThreads);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<uint> randomIds, PerfTracker perfTracker)
        {
            return Read(string.Format("[Esent] random read ({0} items)", Constants.ReadItems), randomIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelRandom(IEnumerable<uint> randomIds, PerfTracker perfTracker, int numberOfThreads)
        {
            return ReadParallel(string.Format("[Esent] parallel random read ({0} items)", Constants.ReadItems), randomIds, perfTracker, numberOfThreads);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker)
        {
            using (var instance = CreateInstance())
            {
                var enumerator = data.GetEnumerator();
                return WriteInternal(operation, enumerator, itemsPerTransaction, numberOfTransactions, perfTracker, instance);
            }
        }

        private List<PerformanceRecord> WriteParallel(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            using (var instance = CreateInstance())
            {
                return ExecuteWriteWithParallel(
                data,
                numberOfTransactions,
                itemsPerTransaction,
                numberOfThreads,
                (enumerator, itmsPerTransaction, nmbrOfTransactions) => WriteInternal(operation, enumerator, itmsPerTransaction, nmbrOfTransactions, perfTracker, instance),
                out elapsedMilliseconds);
            }
        }

        private List<PerformanceRecord> WriteInternal(
            string operation,
            IEnumerator<TestData> enumerator,
            long itemsPerTransaction,
            long numberOfTransactions,
            PerfTracker perfTracker,
            Instance instance)
        {
            byte[] valueToWrite = null;
            var records = new List<PerformanceRecord>();
            var sw = new Stopwatch();

            Table table;
            JET_COLUMNID primaryColumnId;
            JET_COLUMNID secondaryColumnId;
            using (var session = OpenSession(instance, out table, out primaryColumnId, out secondaryColumnId))
            {
                for (var transactions = 0; transactions < numberOfTransactions; transactions++)
                {
                    sw.Restart();
                    long v = 0;
                    using (var tx = new Transaction(session))
                    {
                        for (var i = 0; i < itemsPerTransaction; i++)
                        {
                            enumerator.MoveNext();
                            var testData = enumerator.Current;
                            valueToWrite = GetValueToWrite(valueToWrite, testData.ValueSize);
                            v += valueToWrite.Length;
                            Api.JetPrepareUpdate(session, table, JET_prep.Insert);
                            Api.SetColumn(session, table, primaryColumnId, testData.Id);
                            Api.SetColumn(session, table, secondaryColumnId, valueToWrite);
                            Api.JetUpdate(session, table);
                        }

                        tx.Commit(CommitTransactionGrbit.None);
                    }

                    sw.Stop();
                    perfTracker.Record(sw.ElapsedMilliseconds);
                    records.Add(
                        new PerformanceRecord
                            {
                                Bytes = v,
                                Operation = operation,
                                Time = DateTime.Now,
                                Duration = sw.ElapsedMilliseconds,
                                ProcessedItems = itemsPerTransaction,
                            });
                }

                sw.Stop();
            }

            return records;
        }

        private PerformanceRecord Read(string operation, IEnumerable<uint> ids, PerfTracker perfTracker)
        {
            using (var instance = CreateInstance(delete: false))
            {
                var sw = Stopwatch.StartNew();

                var bytes = ReadInternal(ids, perfTracker, instance);

                sw.Stop();

                return new PerformanceRecord
                           {
                               Bytes = bytes,
                               Operation = operation,
                               Time = DateTime.Now,
                               Duration = sw.ElapsedMilliseconds,
                               ProcessedItems = ids.Count()
                           };
            }
        }

        private PerformanceRecord ReadParallel(string operation, IEnumerable<uint> ids, PerfTracker perfTracker, int numberOfThreads)
        {
            using (var instance = CreateInstance(delete: false))
            {
                return ExecuteReadWithParallel(operation, ids, numberOfThreads, () => ReadInternal(ids, perfTracker, instance));
            }
        }

        private long ReadInternal(IEnumerable<uint> ids, PerfTracker perfTracker, Instance instance)
        {
            Table table;
            JET_COLUMNID primaryColumnId;
            JET_COLUMNID secondaryColumnId;
            using (var session = OpenSession(instance, out table, out primaryColumnId, out secondaryColumnId))
            {
                var sw = Stopwatch.StartNew();
                Api.JetSetCurrentIndex(session, table, "by_key");
                long v = 0;
                foreach (var id in ids)
                {
                    Api.MoveBeforeFirst(session, table);
                    Api.MakeKey(session, table, id, MakeKeyGrbit.NewKey);
                    Api.JetSeek(session, table, SeekGrbit.SeekEQ);

                    var value = Api.RetrieveColumn(session, table, secondaryColumnId);

                    v += value.Length;

                    Debug.Assert(value != null);
                }
                perfTracker.Record(sw.ElapsedMilliseconds);
                return v;
            }
        }
    }
}
