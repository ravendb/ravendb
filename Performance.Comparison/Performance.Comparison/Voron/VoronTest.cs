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
using Voron;
using Voron.Impl;

namespace Performance.Comparison.Voron
{
    public class VoronTest : StoragePerformanceTestBase
    {
        private readonly FlushMode flushMode;
        private const string dataDir = "voron-perf-test";
        private readonly string dataPath;

        public VoronTest(string path, FlushMode flushMode, byte[] buffer)
            : base(buffer)
        {
            this.flushMode = flushMode;
            dataPath = Path.Combine(path, dataDir);
        }

        ~VoronTest()
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, true);
        }

        public override string StorageName { get { return "Voron"; } }

        private void NewStorage()
        {
            if (Directory.Exists(dataPath))
                Directory.Delete(dataPath, true); //TODO do it in more robust way
        }

        public override List<PerformanceRecord> WriteSequential(IEnumerable<TestData> data)
        {
            return Write(string.Format("[Voron] sequential write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions);
        }


        public override List<PerformanceRecord> WriteRandom(IEnumerable<TestData> data)
        {
            return Write(string.Format("[Voron] random write ({0} items)", Constants.ItemsPerTransaction), data,
                         Constants.ItemsPerTransaction, Constants.WriteTransactions);
        }

        public override PerformanceRecord ReadSequential()
        {
            var sequentialIds = Enumerable.Range(0, Constants.ReadItems);

            return Read(string.Format("[Voron] sequential read ({0} items)", Constants.ReadItems), sequentialIds);
        }

        public override PerformanceRecord ReadRandom(IEnumerable<int> randomIds)
        {
            return Read(string.Format("[Voron] random read ({0} items)", Constants.ReadItems), randomIds);
        }

        private List<PerformanceRecord> Write(string operation, IEnumerable<TestData> data, int itemsPerTransaction, int numberOfTransactions)
        {
            byte[] valueToWrite = null;

            NewStorage();

            var records = new List<PerformanceRecord>();

            var sw = new Stopwatch();

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(dataPath, flushMode)))
            {
                var enumerator = data.GetEnumerator();
                sw.Restart();
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
            var options = StorageEnvironmentOptions.ForPath(dataPath);
            options.ManualFlushing = true;

            using (var env = new StorageEnvironment(options))
            {
                env.FlushLogToDataFile();

                var ms = new byte[128];
                var sw = Stopwatch.StartNew();

                var processed = 0;

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