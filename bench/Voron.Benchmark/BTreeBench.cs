using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Voron.Benchmark
{
    public class BTreeBench
    {
        private HashSet<long> _randomNumbers;

        public BTreeBench(HashSet<long> _randomNumbers)
        {
            this._randomNumbers = _randomNumbers;
        }

        public void Execute()
        {
            Console.WriteLine();
            Console.WriteLine("General BTree Benchmarking.");
            Console.WriteLine();

            Benchmark.Time("fill seq", sw => FillSeqOneTransaction(sw));

            Benchmark.Time("fill seq separate tx", sw => FillSeqMultipleTransaction(sw));

            Benchmark.Time("fill rnd", sw => FillRandomOneTransaction(sw));

            Benchmark.Time("fill rnd separate tx", sw => FillRandomMultipleTransaction(sw));

            Benchmark.Time("Data for tests", sw => FillSeqOneTransaction(sw));

            Benchmark.Time("read seq", ReadOneTransaction, delete: false);

            Benchmark.Time("read parallel 1", sw => ReadOneTransaction_Parallel(sw, 1), delete: false);
            Benchmark.Time("read parallel 2", sw => ReadOneTransaction_Parallel(sw, 2), delete: false);
            Benchmark.Time("read parallel 4", sw => ReadOneTransaction_Parallel(sw, 4), delete: false);
            Benchmark.Time("read parallel 8", sw => ReadOneTransaction_Parallel(sw, 8), delete: false);
            Benchmark.Time("read parallel 16", sw => ReadOneTransaction_Parallel(sw, 16), delete: false);


            Benchmark.Time("iterate parallel 1", sw => IterateAllKeysInOneTransaction_Parallel(sw, 1), delete: false);
            Benchmark.Time("iterate parallel 2", sw => IterateAllKeysInOneTransaction_Parallel(sw, 2), delete: false);
            Benchmark.Time("iterate parallel 4", sw => IterateAllKeysInOneTransaction_Parallel(sw, 4), delete: false);
            Benchmark.Time("iterate parallel 8", sw => IterateAllKeysInOneTransaction_Parallel(sw, 8), delete: false);
            Benchmark.Time("iterate parallel 16", sw => IterateAllKeysInOneTransaction_Parallel(sw, 16), delete: false);

            Benchmark.Time("fill seq non then read parallel 4", stopwatch => ReadAndWriteOneTransaction(stopwatch, 4));
            Benchmark.Time("fill seq non then read parallel 8", stopwatch => ReadAndWriteOneTransaction(stopwatch, 8));
            Benchmark.Time("fill seq non then read parallel 16", stopwatch => ReadAndWriteOneTransaction(stopwatch, 16));
        }

        private void FillRandomOneTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            {
                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("test");
                    tx.Commit();
                }

                sw.Start();
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("test");
                    foreach (var l in _randomNumbers)
                    {
                        ms.Position = 0;
                        tree.Add(l.ToString("0000000000000000"), ms);
                    }

                    tx.Commit();
                }
                sw.Stop();
            }
        }


        private void FillSeqOneTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            {
                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    env.Options.DataPager.EnsureContinuous(0, 256 * 1024);
                    tx.CreateTree("test");

                    tx.Commit();
                }


                sw.Start();
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("test");
                    for (long i = 0; i < Configuration.Transactions * Configuration.ItemsPerTransaction; i++)
                    {
                        ms.Position = 0;
                        tree.Add(i.ToString("0000000000000000"), ms);
                    }

                    tx.Commit();
                }
                sw.Stop();
            }
        }

        private void FillRandomMultipleTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            {
                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    env.Options.DataPager.EnsureContinuous(0, 256 * 1024);
                    tx.CreateTree("test");

                    tx.Commit();
                }

                sw.Start();
                var enumerator = _randomNumbers.GetEnumerator();
                for (int x = 0; x < Configuration.Transactions; x++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("test");
                        for (long i = 0; i < Configuration.ItemsPerTransaction; i++)
                        {
                            ms.Position = 0;
                            enumerator.MoveNext();
                            tree.Add((enumerator.Current).ToString("0000000000000000"), ms);
                        }
                        tx.Commit();
                    }
                }
                sw.Stop();
            }
        }

        private void FillSeqMultipleTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            {
                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    env.Options.DataPager.EnsureContinuous(0, 256 * 1024);
                    tx.CreateTree("test");

                    tx.Commit();
                }

                sw.Start();
                int counter = 0;
                for (int x = 0; x < Configuration.Transactions; x++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("test");
                        for (long i = 0; i < Configuration.ItemsPerTransaction; i++)
                        {
                            ms.Position = 0;
                            tree.Add((counter++).ToString("0000000000000000"), ms);
                        }

                        tx.Commit();
                    }
                }
                sw.Stop();
            }
        }


        private void ReadOneTransaction_Parallel(Stopwatch sw, int concurrency)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            {
                var countdownEvent = new CountdownEvent(concurrency);

                sw.Start();
                for (int i = 0; i < concurrency; i++)
                {
                    var currentBase = i;
                    ThreadPool.QueueUserWorkItem(state =>
                    {
                        using (var tx = env.ReadTransaction())
                        {
                            var tree = tx.ReadTree("test");
                            var ms = new byte[100];
                            for (int j = 0; j < ((Configuration.ItemsPerTransaction * Configuration.Transactions) / concurrency); j++)
                            {
                                var current = j * currentBase;
                                var key = current.ToString("0000000000000000");
                                var stream = tree.Read(key).Reader;
                                while (stream.Read(ms, 0, ms.Length) != 0)
                                {
                                }
                            }

                            tx.Commit();
                        }

                        countdownEvent.Signal();
                    });
                }
                countdownEvent.Wait();
                sw.Stop();
            }
        }

        private void IterateAllKeysInOneTransaction_Parallel(Stopwatch sw, int concurrency)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            {
                var countdownEvent = new CountdownEvent(concurrency);

                sw.Start();
                for (int i = 0; i < concurrency; i++)
                {
                    var currentBase = i;
                    ThreadPool.QueueUserWorkItem(state =>
                    {
                        var local = 0;
                        using (var tx = env.ReadTransaction())
                        {
                            var tree = tx.ReadTree("test");
                            using (var it = tree.Iterate())
                            {
                                if (it.Seek(Slice.BeforeAllKeys))
                                {
                                    do
                                    {
                                        local += it.CurrentKey.Size;
                                    } while (it.MoveNext());
                                }
                            }
                            tx.Commit();
                        }

                        countdownEvent.Signal();
                    });
                }
                countdownEvent.Wait();
                sw.Stop();
            }
        }


        private void ReadOneTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            {
                sw.Start();
                using (var tx = env.ReadTransaction())
                {
                    var test = tx.ReadTree("test");
                    var ms = new byte[100];
                    for (int i = 0; i < Configuration.Transactions * Configuration.ItemsPerTransaction; i++)
                    {
                        var key = i.ToString("0000000000000000");
                        var stream = test.Read(key).Reader;
                        {
                            while (stream.Read(ms, 0, ms.Length) != 0)
                            {
                            }
                        }
                    }

                    tx.Commit();
                }
                sw.Stop();
            }
        }

        private void ReadAndWriteOneTransaction(Stopwatch sw, int concurrency)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            {
                var value = new byte[100];
                new Random().NextBytes(value);

                using (var tx = env.WriteTransaction())
                {
                    env.Options.DataPager.EnsureContinuous(0, 256 * 1024);
                    tx.CreateTree("test");

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("test");

                    var ms = new MemoryStream(value);
                    for (long i = 0; i < Configuration.Transactions * Configuration.ItemsPerTransaction; i++)
                    {
                        ms.Position = 0;
                        tree.Add(i.ToString("0000000000000000"), ms);
                    }

                    tx.Commit();
                }

                var countdownEvent = new CountdownEvent(concurrency);

                sw.Start();
                for (int i = 0; i < concurrency; i++)
                {
                    var currentBase = i;
                    ThreadPool.QueueUserWorkItem(state =>
                    {
                        using (var tx = env.ReadTransaction())
                        {
                            var tree = tx.ReadTree("test");
                            var ms = new byte[100];
                            for (int j = 0; j < ((Configuration.ItemsPerTransaction * Configuration.Transactions) / concurrency); j++)
                            {
                                var current = j * currentBase;
                                var key = current.ToString("0000000000000000");
                                var stream = tree.Read(key).Reader;
                                {
                                    while (stream.Read(ms, 0, ms.Length) != 0)
                                    {
                                    }
                                }
                            }

                            tx.Commit();
                        }

                        countdownEvent.Signal();
                    });
                }
                countdownEvent.Wait();
                sw.Stop();
            }
        }
    }
}
