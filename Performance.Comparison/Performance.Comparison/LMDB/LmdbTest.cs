namespace Performance.Comparison.LMDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;

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

        public override List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data)
        {
            return Write(string.Format("[LMDB] sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions);
        }

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data)
        {
            return Write(string.Format("[LMDB] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions);
        }

        public override PerformanceRecord ReadSequential()
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return Read(string.Format("[LMDB] sequential read ({0} items)", Constants.ReadItems), sequentialIds);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<int> randomIds)
        {
            return Read(string.Format("[LMDB] random read ({0} items)", Constants.ReadItems), randomIds);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions)
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
                        }

                        tx.Commit();
                    }

                    sw.Stop();

                    records.Add(new PerformanceRecord
                    {
                        Operation = operation,
                        Time = DateTime.Now,
                        Duration = sw.ElapsedMilliseconds,
                        ProcessedItems = itemsPerTransaction,
                        Memory = GetMemory()
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
                        var value = tx.Get(db, Encoding.UTF8.GetBytes(id.ToString("0000000000000000")));

                        Debug.Assert(value != null);

                        processed++;
                    }
                }

                sw.Stop();

                return new PerformanceRecord
                {
                    Operation = operation,
                    Time = DateTime.Now,
                    Duration = sw.ElapsedMilliseconds,
                    ProcessedItems = processed,
                    Memory = GetMemory()
                };
            }
        }
    }
}