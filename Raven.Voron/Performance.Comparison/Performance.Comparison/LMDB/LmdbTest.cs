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

        private LightningEnvironment NewEnvironment(bool delete = true)
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

            return env;
        }

        public override List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[LMDB] sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[LMDB] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override PerformanceRecord ReadSequential(PerfTracker perfTracker)
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return Read(string.Format("[LMDB] sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads)
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return ReadParallel(string.Format("[LMDB] parallel sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker, numberOfThreads);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<int> randomIds, PerfTracker perfTracker)
        {
            return Read(string.Format("[LMDB] random read ({0} items)", Constants.ReadItems), randomIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelRandom(IEnumerable<int> randomIds, PerfTracker perfTracker, int numberOfThreads)
        {
            return ReadParallel(string.Format("[LMDB] parallel random read ({0} items)", Constants.ReadItems), randomIds, perfTracker, numberOfThreads);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker)
        {
            byte[] valueToWrite = null;
            var records = new List<PerformanceRecord>();

            using (var env = NewEnvironment())
            {
                var sw = new Stopwatch();

                var enumerator = data.GetEnumerator();

                for (var transactions = 0; transactions < numberOfTransactions; transactions++)
                {
                    sw.Restart();

                    using (var tx = env.BeginTransaction())
                    using (var db = tx.OpenDatabase())
                    {
                        for (var i = 0; i < itemsPerTransaction; i++)
                        {
                            enumerator.MoveNext();

                            valueToWrite = GetValueToWrite(valueToWrite, enumerator.Current.ValueSize);

                            tx.Put(db, Encoding.UTF8.GetBytes(enumerator.Current.Id.ToString("0000000000000000")), valueToWrite);
                            perfTracker.Increment();
                        }

                        tx.Commit();
                    }

                    sw.Stop();

                    records.Add(new PerformanceRecord
                    {
                        Operation = operation,
                        Time = DateTime.Now,
                        Duration = sw.ElapsedMilliseconds,
                        ProcessedItems = itemsPerTransaction
                    });
                }

                sw.Stop();
            }

            return records;
        }

        private PerformanceRecord Read(string operation, IEnumerable<int> ids, PerfTracker perfTracker)
        {
            using (var env = NewEnvironment(delete: false))
            {
                var sw = Stopwatch.StartNew();

                ReadInternal(ids, perfTracker, env);

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

            using (var env = NewEnvironment(delete: false))
            {
                var sw = Stopwatch.StartNew();

                for (int i = 0; i < numberOfThreads; i++)
                {
                    ThreadPool.QueueUserWorkItem(
                        state =>
                            {
                                ReadInternal(ids, perfTracker, env);

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

        private static void ReadInternal(IEnumerable<int> ids, PerfTracker perfTracker, LightningEnvironment env)
        {
            using (var tx = env.BeginTransaction())
            using (var db = tx.OpenDatabase())
            {
                foreach (var id in ids)
                {
                    var value = tx.Get(db, Encoding.UTF8.GetBytes(id.ToString("0000000000000000")));
                    perfTracker.Increment();
                    Debug.Assert(value != null);
                }
            }
        }
    }
}