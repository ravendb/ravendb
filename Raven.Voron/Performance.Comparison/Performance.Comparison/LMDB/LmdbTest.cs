namespace Performance.Comparison.LMDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;

    using LightningDB;

    public class LmdbTest : StoragePerformanceTestBase
    {
        private readonly string _path;

        public override string StorageName
        {
            get
            {
                return "LMDB";
            }
        }

        public LmdbTest(string path, byte[] buffer)
            : base(buffer)
        {
            _path = Path.Combine(path, "lmdb");
        }

        ~LmdbTest()
        {
            if (Directory.Exists(_path))
                Directory.Delete(_path, true);
        }

		public enum rndseq { RND = 0, SEQ = 1 }

        private LightningEnvironment NewEnvironment(out LightningDatabase db, bool delete = true)
        {
            if (delete && Directory.Exists(_path))
                Directory.Delete(_path, true);

            if (!Directory.Exists(_path))
                Directory.CreateDirectory(_path);

            var env = new LightningEnvironment(_path, EnvironmentOpenFlags.None)
                          {
                              MapSize = 1024 * 1024 * 1024 * (long)10
                          };
            env.Open();
            var tx = env.BeginTransaction();
            db = tx.OpenDatabase();
			tx.Commit();

            return env;
        }

        public override List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[LMDB] sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, rndseq.SEQ);
        }

        public override List<PerformanceRecord> WriteParallelSequential(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            return WriteParallel(string.Format("[LMDB] parallel sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, rndseq.RND, numberOfThreads, out elapsedMilliseconds);
        }

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[LMDB] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, rndseq.RND);
        }

        public override List<PerformanceRecord> WriteParallelRandom(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            return WriteParallel(string.Format("[LMDB] parallel random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, rndseq.RND, numberOfThreads, out elapsedMilliseconds);
        }

        public override PerformanceRecord ReadSequential(PerfTracker perfTracker)
        {
			var sequentialIds = Enumerable.Range(0, Constants.ReadItems).Select(x => (uint)x); ;

            return Read(string.Format("[LMDB] sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads)
        {
			var sequentialIds = Enumerable.Range(0, Constants.ReadItems).Select(x => (uint)x); ;

            return ReadParallel(string.Format("[LMDB] parallel sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker, numberOfThreads);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<uint> randomIds, PerfTracker perfTracker)
        {
            return Read(string.Format("[LMDB] random read ({0} items)", Constants.ReadItems), randomIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelRandom(IEnumerable<uint> randomIds, PerfTracker perfTracker, int numberOfThreads)
        {
            return ReadParallel(string.Format("[LMDB] parallel random read ({0} items)", Constants.ReadItems), randomIds, perfTracker, numberOfThreads);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker, rndseq Rflag)
        {
			LightningDatabase db;
            using (var env = NewEnvironment(out db))
            {
                var enumerator = data.GetEnumerator();
                return WriteInternal(operation, enumerator, itemsPerTransaction, numberOfTransactions, perfTracker, Rflag, env, db);
            }
        }

        private List<PerformanceRecord> WriteParallel(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker, rndseq Rflag, int numberOfThreads, out long elapsedMilliseconds)
        {
			LightningDatabase db;
            using (var env = NewEnvironment(out db))
            {
                return ExecuteWriteWithParallel(
                data,
                numberOfTransactions,
                itemsPerTransaction,
                numberOfThreads,
                (enumerator, itmsPerTransaction, nmbrOfTransactions) => WriteInternal(operation, enumerator, itmsPerTransaction, nmbrOfTransactions, perfTracker, Rflag, env, db),
                out elapsedMilliseconds);
            }
        }

        private List<PerformanceRecord> WriteInternal(
            string operation,
            IEnumerator<TestData> enumerator,
            long itemsPerTransaction,
            long numberOfTransactions,
            PerfTracker perfTracker,
			rndseq Rflag,
            LightningEnvironment env,
			LightningDatabase db)
        {
            byte[] valueToWrite = null;
            var records = new List<PerformanceRecord>();
            var sw = new Stopwatch();
			LightningDB.PutOptions putflags = LightningDB.PutOptions.None;

			if (Rflag == rndseq.SEQ)
				putflags = LightningDB.PutOptions.AppendData;

            for (var transactions = 0; transactions < numberOfTransactions; transactions++)
            {
                sw.Restart();

                using (var tx = env.BeginTransaction())
                {
                    for (var i = 0; i < itemsPerTransaction; i++)
                    {
                        enumerator.MoveNext();

                        valueToWrite = GetValueToWrite(valueToWrite, enumerator.Current.ValueSize);

                        tx.Put(db, Encoding.UTF8.GetBytes(enumerator.Current.Id.ToString("0000000000000000")), valueToWrite, putflags);
                    }

                    tx.Commit();
                }

                sw.Stop();
                perfTracker.Record(sw.ElapsedMilliseconds);

                records.Add(new PerformanceRecord
                                {
                                    Operation = operation, 
                                    Time = DateTime.Now, 
                                    Duration = sw.ElapsedMilliseconds, 
                                    ProcessedItems = itemsPerTransaction
                                });
            }

            sw.Stop();

            return records;
        }

        private PerformanceRecord Read(string operation, IEnumerable<uint> ids, PerfTracker perfTracker)
        {
			LightningDatabase db;
            using (var env = NewEnvironment(out db, delete: false))
            {
                var sw = Stopwatch.StartNew();

                ReadInternal(ids, perfTracker, env, db);

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

        private PerformanceRecord ReadParallel(string operation, IEnumerable<uint> ids, PerfTracker perfTracker, int numberOfThreads)
        {
			LightningDatabase db;
            using (var env = NewEnvironment(out db, delete: false))
            {
                return ExecuteReadWithParallel(operation, ids, numberOfThreads, () => ReadInternal(ids, perfTracker, env, db));
            }
        }

        private static long ReadInternal(IEnumerable<uint> ids, PerfTracker perfTracker, LightningEnvironment env,
			LightningDatabase db)
        {
            using (var tx = env.BeginTransaction(LightningDB.TransactionBeginFlags.ReadOnly))
			using (var cursor = new LightningCursor(db, tx))
            {
                long v = 0;
                foreach (var id in ids)
                {
                    var value = cursor.MoveTo(Encoding.UTF8.GetBytes(id.ToString("0000000000000000")));
                    v += value.Value.Length;
                    //Debug.Assert(value != null);
                }
                return v;
            }
        }
    }
}
