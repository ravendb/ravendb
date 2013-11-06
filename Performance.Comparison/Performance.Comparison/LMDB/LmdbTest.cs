namespace Performance.Comparison.LMDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;

    using LightningDB;

    public class LmdbTest : IStoragePerformanceTest
    {
        private readonly string _path;

        public string StorageName
        {
            get
            {
                return "LMDB";
            }
        }

        public LmdbTest(string path)
        {
            _path = Path.Combine(path, "lmdb");
        }

        private LightningEnvironment NewEnvironment(bool delete = true)
        {
            if (delete && Directory.Exists(_path))
                Directory.Delete(_path, true);

            if (!Directory.Exists(_path))
                Directory.CreateDirectory(_path);

            var env = new LightningEnvironment(_path, EnvironmentOpenFlags.None)
                          {
                              MapSize = 500 * 1024 * 1024
                          };
            env.Open();

            return env;
        }

        public List<PerformanceRecord> WriteSequential()
        {
            var sequentialIds = Enumerable.Range(0, Constants.ItemsPerTransaction * Constants.WriteTransactions);

            return Write(string.Format("[LMDB] sequential write ({0} items)", Constants.ItemsPerTransaction), sequentialIds,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions);
        }

        public List<PerformanceRecord> WriteRandom(HashSet<int> randomIds)
        {
            return Write(string.Format("[LMDB] random write ({0} items)", Constants.ItemsPerTransaction), randomIds,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions);
        }

        public PerformanceRecord ReadSequential()
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return Read(string.Format("[LMDB] sequential read ({0} items)", Constants.ReadItems), sequentialIds);
        }

        public PerformanceRecord ReadRandom(HashSet<int> randomIds)
        {
            return Read(string.Format("[LMDB] random read ({0} items)", Constants.ReadItems), randomIds);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<int> ids, int itemsPerTransaction, int numberOfTransactions)
        {
            var value = new byte[128];
            new Random().NextBytes(value);

            var records = new List<PerformanceRecord>();

            using (var env = NewEnvironment())
            {
                var sw = new Stopwatch();

                var enumerator = ids.GetEnumerator();

                for (var transactions = 0; transactions < numberOfTransactions; transactions++)
                {
                    sw.Restart();

                    using (var tx = env.BeginTransaction())
                    using (var db = tx.OpenDatabase())
                    {
                        for (var i = 0; i < itemsPerTransaction; i++)
                        {
                            enumerator.MoveNext();

                            tx.Put(db, Encoding.UTF8.GetBytes(enumerator.Current.ToString("0000000000000000")), value);
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

        private PerformanceRecord Read(string operation, IEnumerable<int> ids)
        {
            using (var env = NewEnvironment(delete: false))
            {
                var sw = Stopwatch.StartNew();

                var processed = 0;
                using (var tx = env.BeginTransaction())
                using (var db = tx.OpenDatabase())
                {
                    foreach (var id in ids)
                    {
                        tx.Get(db, Encoding.UTF8.GetBytes(id.ToString("0000000000000000")));

                        processed++;
                    }
                }

                sw.Stop();

                return new PerformanceRecord
                {
                    Operation = operation,
                    Time = DateTime.Now,
                    Duration = sw.ElapsedMilliseconds,
                    ProcessedItems = processed
                };
            }
        }
    }
}