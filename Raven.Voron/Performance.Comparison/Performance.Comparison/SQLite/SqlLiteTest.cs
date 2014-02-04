// -----------------------------------------------------------------------
//  <copyright file="SqlLiteTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Performance.Comparison.SQLite
{
    using System.Threading;

    public class SqlLiteTest : StoragePerformanceTestBase
    {
        private readonly string path;
        private readonly string connectionString;
        private readonly string dbFileName;

        public SqlLiteTest(string path, byte[] buffer)
            : base(buffer)
        {
            this.path = path;
            dbFileName = Path.Combine(path, "sqlite-perf-test.s3db");
            connectionString = string.Format("Data Source={0}", dbFileName);
        }

        public override string StorageName { get { return "SQLite"; } }

        private void NewDatabase()
        {
            if (File.Exists(dbFileName))
                File.Delete(dbFileName);

            SQLiteConnection.CreateFile(dbFileName);

            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();

                using (var command = new SQLiteCommand("CREATE TABLE Items(Id INTEGER PRIMARY KEY, Value BLOB)", connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        public override List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[SQLite] sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override List<PerformanceRecord> WriteParallelSequential(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            return WriteParallel(string.Format("[SQLite] parallel sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, numberOfThreads, out elapsedMilliseconds);
        }

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[SQLite] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override List<PerformanceRecord> WriteParallelRandom(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            return WriteParallel(string.Format("[SQLite] parallel random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, numberOfThreads, out elapsedMilliseconds);
        }

        public override PerformanceRecord ReadSequential(PerfTracker perfTracker)
        {
			var sequentialIds = Enumerable.Range(0, Constants.ReadItems).Select(x => (uint)x); ;

            return Read(string.Format("[SQLite] sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads)
        {
			var sequentialIds = Enumerable.Range(0, Constants.ReadItems).Select(x => (uint)x); ;

            return ReadParallel(string.Format("[SQLite] parallel sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker, numberOfThreads);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<uint> randomIds, PerfTracker perfTracker)
        {
            return Read(string.Format("[SQLite] random read ({0} items)", Constants.ReadItems), randomIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelRandom(IEnumerable<uint> randomIds, PerfTracker perfTracker, int numberOfThreads)
        {
            return ReadParallel(string.Format("[SQLite] parallel random read ({0} items)", Constants.ReadItems), randomIds, perfTracker, numberOfThreads);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker)
        {
            NewDatabase();

            var enumerator = data.GetEnumerator();
            return WriteInternal(operation, enumerator, itemsPerTransaction, numberOfTransactions, perfTracker);
        }

        private List<PerformanceRecord> WriteParallel(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            NewDatabase();

            return ExecuteWriteWithParallel(
                data,
                numberOfTransactions,
                itemsPerTransaction,
                numberOfThreads,
                (enumerator, itmsPerTransaction, nmbrOfTransactions) => WriteInternal(operation, enumerator, itmsPerTransaction, nmbrOfTransactions, perfTracker),
                out elapsedMilliseconds);
        }

        private List<PerformanceRecord> WriteInternal(string operation, IEnumerator<TestData> enumerator, long itemsPerTransaction, long numberOfTransactions, PerfTracker perfTracker)
        {
            var sw = new Stopwatch();
            byte[] valueToWrite = null;
            var records = new List<PerformanceRecord>();
            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();

                sw.Restart();
                for (var transactions = 0; transactions < numberOfTransactions; transactions++)
                {
                    sw.Restart();
                    using (var tx = connection.BeginTransaction())
                    {
                        for (var i = 0; i < itemsPerTransaction; i++)
                        {
                            enumerator.MoveNext();

                            valueToWrite = GetValueToWrite(valueToWrite, enumerator.Current.ValueSize);

                            using (var command = new SQLiteCommand("INSERT INTO Items (Id, Value) VALUES (@id, @value)", connection))
                            {
                                command.Parameters.Add("@id", DbType.Int32, 4).Value = enumerator.Current.Id;
                                command.Parameters.Add("@value", DbType.Binary, valueToWrite.Length).Value = valueToWrite;

                                var affectedRows = command.ExecuteNonQuery();
                                Debug.Assert(affectedRows == 1);
                            }
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
            }

            return records;
        }

        private PerformanceRecord Read(string operation, IEnumerable<uint> ids, PerfTracker perfTracker)
        {
            var sw = Stopwatch.StartNew();

            using (var connection = new SQLiteConnection(connectionString))
            {
                sw.Restart();

                connection.Open();
                ReadInternal(ids, perfTracker, connection);

                sw.Stop();
            }

            return new PerformanceRecord
                {
                    Operation = operation,
                    Time = DateTime.Now,
                    Duration = sw.ElapsedMilliseconds,
                    ProcessedItems = ids.Count()
                };
        }

        private PerformanceRecord ReadParallel(string operation, IEnumerable<uint> ids, PerfTracker perfTracker, int numberOfThreads)
        {
            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();

                return ExecuteReadWithParallel(operation, ids, numberOfThreads, () => ReadInternal(ids, perfTracker, connection));
            }
        }

        private static long ReadInternal(IEnumerable<uint> ids, PerfTracker perfTracker, SQLiteConnection connection)
        {
            var buffer = new byte[4096];

            using (var tx = connection.BeginTransaction())
            {
                long v = 0;
                var sw = Stopwatch.StartNew();
                foreach (var id in ids)
                {
                    using (var command = new SQLiteCommand("SELECT Value FROM Items WHERE ID = " + id, connection))
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            long bytesRead;
                            long fieldOffset = 0;

                            while ((bytesRead = reader.GetBytes(0, fieldOffset, buffer, 0, buffer.Length)) > 0)
                            {
                                fieldOffset += bytesRead;
                                v += bytesRead;
                            }
                        }
                    }
                }
                perfTracker.Record(sw.ElapsedMilliseconds);
                return v;
            }
        }
    }
}
