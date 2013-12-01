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

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[SQLite] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override PerformanceRecord ReadSequential(PerfTracker perfTracker)
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return Read(string.Format("[SQLite] sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads)
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return ReadParallel(string.Format("[SQLite] parallel sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker, numberOfThreads);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<int> randomIds, PerfTracker perfTracker)
        {
            return Read(string.Format("[SQLite] random read ({0} items)", Constants.ReadItems), randomIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelRandom(IEnumerable<int> randomIds, PerfTracker perfTracker, int numberOfThreads)
        {
            return ReadParallel(string.Format("[SQLite] parallel random read ({0} items)", Constants.ReadItems), randomIds, perfTracker, numberOfThreads);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker)
        {
            byte[] valueToWrite = null;

            NewDatabase();

            var random = new Random();
            var value = new byte[data.Max(x => x.ValueSize)];
            random.NextBytes(value);

            var records = new List<PerformanceRecord>();

            var sw = new Stopwatch();

            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();

                var enumerator = data.GetEnumerator();
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
                                perfTracker.Increment();
                                Debug.Assert(affectedRows == 1);
                            }
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
            var sw = Stopwatch.StartNew();

            ReadInternal(ids, perfTracker, connectionString);

            sw.Stop();

            return new PerformanceRecord
                {
                    Operation = operation,
                    Time = DateTime.Now,
                    Duration = sw.ElapsedMilliseconds,
                    ProcessedItems = ids.Count()
                };
        }

        private PerformanceRecord ReadParallel(string operation, IEnumerable<int> ids, PerfTracker perfTracker, int numberOfThreads)
        {
            var countdownEvent = new CountdownEvent(numberOfThreads);

            var sw = Stopwatch.StartNew();

            for (int i = 0; i < numberOfThreads; i++)
            {
                ThreadPool.QueueUserWorkItem(
                    state =>
                    {
                        ReadInternal(ids, perfTracker, connectionString);

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

        private static void ReadInternal(IEnumerable<int> ids, PerfTracker perfTracker, string connectionString)
        {
            var buffer = new byte[4096];

            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();

                using (var tx = connection.BeginTransaction())
                {
                    foreach (var id in ids)
                    {
                        using (var command = new SQLiteCommand("SELECT Value FROM Items WHERE ID = " + id, connection))
                        {
                            using (var reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    long bytesRead;
                                    long fieldOffset = 0;

                                    while ((bytesRead = reader.GetBytes(0, fieldOffset, buffer, 0, buffer.Length)) > 0)
                                    {
                                        fieldOffset += bytesRead;
                                    }
                                    perfTracker.Increment();
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}