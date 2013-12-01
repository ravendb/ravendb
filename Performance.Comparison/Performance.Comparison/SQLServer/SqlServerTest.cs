// -----------------------------------------------------------------------
//  <copyright file="SqlServerTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace Performance.Comparison.SQLServer
{
    using System.Threading;

    public class SqlServerTest : StoragePerformanceTestBase
    {
        private readonly string connectionString;

        public SqlServerTest(byte[] buffer)
            : base(buffer)
        {
            connectionString = @"Data Source=localhost\LOCAL2012;Initial Catalog=VoronTests;Integrated Security=true";
        }

        public override string StorageName { get { return "SQL Server"; } }

        private void NewDatabase()
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();


                using (var command = new SqlCommand("if object_id('Items') is not null  drop table items", connection))
                {
                    command.ExecuteNonQuery();
                }
                using (var command = new SqlCommand("CREATE TABLE Items(Id INTEGER PRIMARY KEY, Value VARBINARY(MAX))", connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
        public override List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[SQL Server] sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override List<PerformanceRecord> WriteParallelSequential(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            return WriteParallel(string.Format("[SQL Server] parallel sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, numberOfThreads, out elapsedMilliseconds);
        }

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[SQL Server] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override List<PerformanceRecord> WriteParallelRandom(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            return WriteParallel(string.Format("[SQL Server] parallel random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, numberOfThreads, out elapsedMilliseconds);
        }

        public override PerformanceRecord ReadSequential(PerfTracker perfTracker)
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return Read(string.Format("[SQL Server] sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads)
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return ReadParallel(string.Format("[SQL Server] parallel sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker, numberOfThreads);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<int> randomIds, PerfTracker perfTracker)
        {
            return Read(string.Format("[SQL Server] random read ({0} items)", Constants.ReadItems), randomIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelRandom(IEnumerable<int> randomIds, PerfTracker perfTracker, int numberOfThreads)
        {
            return ReadParallel(string.Format("[SQL Server] parallel random read ({0} items)", Constants.ReadItems), randomIds, perfTracker, numberOfThreads);
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

            using (var connection = new SqlConnection(connectionString))
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

                            using (var command = new SqlCommand("INSERT INTO Items (Id, Value) VALUES (@id, @value)", connection))
                            {
                                command.Transaction = tx;
                                command.Parameters.Add("@id", SqlDbType.Int, 4).Value = enumerator.Current.Id;
                                command.Parameters.Add("@value", SqlDbType.Binary, valueToWrite.Length).Value = valueToWrite;

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
            return ExecuteReadWithParallel(operation, ids, numberOfThreads, () => ReadInternal(ids, perfTracker, connectionString));
        }

        private static void ReadInternal(IEnumerable<int> ids, PerfTracker perfTracker, string connectionString)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var buffer = new byte[4096];

                using (var tx = connection.BeginTransaction())
                {
                    foreach (var id in ids)
                    {
                        using (var command = new SqlCommand("SELECT Value FROM Items WHERE ID = " + id, connection))
                        {
                            command.Transaction = tx;
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