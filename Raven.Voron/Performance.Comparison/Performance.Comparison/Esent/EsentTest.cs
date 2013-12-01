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
        }

        ~EsentTest()
        {
            if (Directory.Exists(_path))
                Directory.Delete(_path, true);
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
            Api.JetAttachDatabase2(session, "edbtest.db", 0, AttachDatabaseGrbit.None);

            JET_DBID dbid;
            Api.JetOpenDatabase(session, "edbtest.db", null, out dbid, OpenDatabaseGrbit.None);

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
                Api.JetCreateDatabase(session, "edbtest.db", null, out dbid, CreateDatabaseGrbit.OverwriteExisting);

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
                        coltyp = JET_coltyp.Binary
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

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[Esent] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override PerformanceRecord ReadSequential(PerfTracker perfTracker)
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return Read(string.Format("[Esent] sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads)
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return ReadParallel(string.Format("[Esent] parallel sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker, numberOfThreads);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<int> randomIds, PerfTracker perfTracker)
        {
            return Read(string.Format("[Esent] random read ({0} items)", Constants.ReadItems), randomIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelRandom(IEnumerable<int> randomIds, PerfTracker perfTracker, int numberOfThreads)
        {
            return ReadParallel(string.Format("[Esent] parallel random read ({0} items)", Constants.ReadItems), randomIds, perfTracker, numberOfThreads);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker)
        {
            byte[] valueToWrite = null;
            var records = new List<PerformanceRecord>();

            using (var instance = CreateInstance())
            {
                var sw = new Stopwatch();

                var enumerator = data.GetEnumerator();

                Table table;
                JET_COLUMNID primaryColumnId;
                JET_COLUMNID secondaryColumnId;
                using (var session = OpenSession(instance, out table, out primaryColumnId, out secondaryColumnId))
                {
                    for (var transactions = 0; transactions < numberOfTransactions; transactions++)
                    {
                        sw.Restart();

                        using (var tx = new Transaction(session))
                        {
                            for (var i = 0; i < itemsPerTransaction; i++)
                            {
                                enumerator.MoveNext();

                                valueToWrite = GetValueToWrite(valueToWrite, enumerator.Current.ValueSize);
                                Api.JetPrepareUpdate(session, table, JET_prep.Insert);
                                Api.SetColumn(session, table, primaryColumnId, enumerator.Current.Id);
                                Api.SetColumn(session, table, secondaryColumnId, valueToWrite);
                                Api.JetUpdate(session, table);
                                perfTracker.Increment();

                            }

                            tx.Commit(CommitTransactionGrbit.None);
                        }

                        sw.Stop();

                        records.Add(
                            new PerformanceRecord
                                {
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
        }

        private PerformanceRecord Read(string operation, IEnumerable<int> ids, PerfTracker perfTracker)
        {
            using (var instance = CreateInstance(delete: false))
            {
                var sw = Stopwatch.StartNew();

                ReadInternal(ids, perfTracker, instance);

                sw.Stop();

                return new PerformanceRecord
                           {
                               Operation = operation,
                               Time = DateTime.Now,
                               Duration = sw.ElapsedMilliseconds,
                               ProcessedItems = ids.Count()
                           };
            }
        }

        private PerformanceRecord ReadParallel(string operation, IEnumerable<int> ids, PerfTracker perfTracker, int numberOfThreads)
        {
            var countdownEvent = new CountdownEvent(numberOfThreads);

            using (var instance = CreateInstance(delete: false))
            {
                var sw = Stopwatch.StartNew();

                for (int i = 0; i < numberOfThreads; i++)
                {
                    ThreadPool.QueueUserWorkItem(
                        state =>
                        {
                            ReadInternal(ids, perfTracker, instance);

                            countdownEvent.Signal();
                        });
                }

                countdownEvent.Wait();
                sw.Stop();

                return new PerformanceRecord
                {
                    Operation = operation,
                    Time = DateTime.Now,
                    Duration = sw.ElapsedMilliseconds,
                    ProcessedItems = ids.Count() * numberOfThreads
                };
            }
        }

        private void ReadInternal(IEnumerable<int> ids, PerfTracker perfTracker, Instance instance)
        {
            Table table;
            JET_COLUMNID primaryColumnId;
            JET_COLUMNID secondaryColumnId;
            using (var session = OpenSession(instance, out table, out primaryColumnId, out secondaryColumnId))
            {
                Api.JetSetCurrentIndex(session, table, "by_key");

                foreach (var id in ids)
                {
                    Api.MakeKey(session, table, id, MakeKeyGrbit.NewKey);
                    Api.JetSeek(session, table, SeekGrbit.SeekEQ);

                    var value = Api.RetrieveColumn(session, table, secondaryColumnId);
                    perfTracker.Increment();

                    Debug.Assert(value != null);
                }
            }
        }
    }
}