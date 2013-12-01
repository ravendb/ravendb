// -----------------------------------------------------------------------
//  <copyright file="VoronTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Voron;
using Voron.Impl;

namespace Performance.Comparison.Voron
{
    public class VoronTest : StoragePerformanceTestBase
    {
        private const string dataDir = "voron-perf-test";
        private readonly string dataPath;

        public VoronTest(string path, byte[] buffer)
            : base(buffer)
        {
            dataPath = Path.Combine(path, dataDir);
        }

        public override string StorageName { get { return "Voron"; } }

        private void NewStorage()
        {
            while (Directory.Exists(dataPath))
            {
                try
                {
                    Directory.Delete(dataPath, true);
                }
                catch (Exception e)
                {
                    Thread.Sleep(100);
                }
            }
        }

        public override List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[Voron] sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override List<PerformanceRecord> WriteParallelSequential(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            return WriteParallel(string.Format("[Voron] parallel sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, numberOfThreads, out elapsedMilliseconds);
        }

        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data, PerfTracker perfTracker)
        {
            return Write(string.Format("[Voron] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker);
        }

        public override List<PerformanceRecord> WriteParallelRandom(IEnumerable<TestData> data, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            return WriteParallel(string.Format("[Voron] parallel random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions, perfTracker, numberOfThreads, out elapsedMilliseconds);
        }

        public override PerformanceRecord ReadSequential(PerfTracker perfTracker)
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return Read(string.Format("[Voron] sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelSequential(PerfTracker perfTracker, int numberOfThreads)
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return ReadParallel(string.Format("[Voron] parallel sequential read ({0} items)", Constants.ReadItems), sequentialIds, perfTracker, numberOfThreads);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<int> randomIds, PerfTracker perfTracker)
        {
            return Read(string.Format("[Voron] random read ({0} items)", Constants.ReadItems), randomIds, perfTracker);
        }

        public override PerformanceRecord ReadParallelRandom(IEnumerable<int> randomIds, PerfTracker perfTracker, int numberOfThreads)
        {
            return ReadParallel(string.Format("[Voron] parallel random read ({0} items)", Constants.ReadItems), randomIds, perfTracker, numberOfThreads);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker)
        {
            NewStorage();

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(dataPath)))
            {
                var enumerator = data.GetEnumerator();
                return WriteInternal(operation, itemsPerTransaction, numberOfTransactions, perfTracker, env, enumerator);
            }
        }

        private List<PerformanceRecord> WriteParallel(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions, PerfTracker perfTracker, int numberOfThreads, out long elapsedMilliseconds)
        {
            NewStorage();

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(dataPath)))
            {
                return ExecuteWriteWithParallel(
                data,
                numberOfTransactions,
                itemsPerTransaction,
                numberOfThreads,
                (enumerator, itmsPerTransaction, nmbrOfTransactions) => WriteInternalBatch(operation, enumerator, itmsPerTransaction, nmbrOfTransactions, perfTracker, env),
                out elapsedMilliseconds);
            }
        }

        private List<PerformanceRecord> WriteInternalBatch(
            string operation,
            IEnumerator<TestData> enumerator,
            long itemsPerBatch,
            long numberOfBatches,
            PerfTracker perfTracker,
            StorageEnvironment env)
        {
            var sw = new Stopwatch();
            byte[] valueToWrite = null;
            var records = new List<PerformanceRecord>();
            for (var b = 0; b < numberOfBatches; b++)
            {
                sw.Restart();
                using (var batch = new WriteBatch())
                {
                    for (var i = 0; i < itemsPerBatch; i++)
                    {
                        enumerator.MoveNext();

                        valueToWrite = GetValueToWrite(valueToWrite, enumerator.Current.ValueSize);

                        batch.Add(enumerator.Current.Id.ToString("0000000000000000"), new MemoryStream(valueToWrite), "Root");
                    }

                    env.Writer.Write(batch);
                    perfTracker.IncrementBy(itemsPerBatch);
                }

                sw.Stop();

                records.Add(new PerformanceRecord
                {
                    Operation = operation,
                    Time = DateTime.Now,
                    Duration = sw.ElapsedMilliseconds,
                    ProcessedItems = itemsPerBatch
                });
            }

            return records;
        }

        private List<PerformanceRecord> WriteInternal(
            string operation,
            int itemsPerTransaction,
            int numberOfTransactions,
            PerfTracker perfTracker,
            StorageEnvironment env,
            IEnumerator<TestData> enumerator)
        {
            var sw = new Stopwatch();
            byte[] valueToWrite = null;
            var records = new List<PerformanceRecord>();
            for (var transactions = 0; transactions < numberOfTransactions; transactions++)
            {
                sw.Restart();
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    for (var i = 0; i < itemsPerTransaction; i++)
                    {
                        enumerator.MoveNext();

                        valueToWrite = GetValueToWrite(valueToWrite, enumerator.Current.ValueSize);

                        tx.State.Root.Add(tx, enumerator.Current.Id.ToString("0000000000000000"), new MemoryStream(valueToWrite));
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

            return records;
        }

        private PerformanceRecord Read(string operation, IEnumerable<int> ids, PerfTracker perfTracker)
        {
            var options = StorageEnvironmentOptions.ForPath(dataPath);
            options.ManualFlushing = true;

            using (var env = new StorageEnvironment(options))
            {
                env.FlushLogToDataFile();

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
            var options = StorageEnvironmentOptions.ForPath(dataPath);
            options.ManualFlushing = true;

            using (var env = new StorageEnvironment(options))
            {
                env.FlushLogToDataFile();

                return ExecuteReadWithParallel(operation, ids, numberOfThreads, () => ReadInternal(ids, perfTracker, env));
            }
        }

        private static void ReadInternal(IEnumerable<int> ids, PerfTracker perfTracker, StorageEnvironment env)
        {
            var ms = new byte[4096];

            using (var tx = env.NewTransaction(TransactionFlags.Read))
            {
                foreach (var id in ids)
                {
                    var key = id.ToString("0000000000000000");
                    using (var stream = tx.State.Root.Read(tx, key).Stream)
                    {
                        while (stream.Read(ms, 0, ms.Length) != 0)
                        {
                        }
                    }
                    perfTracker.Increment();
                }
            }
        }
    }
}