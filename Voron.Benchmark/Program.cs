using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Voron.Debugging;
using Voron.Impl;

namespace Voron.Benchmark
{
    public class Program
    {
        private static HashSet<long> _randomNumbers;
        public const int ItemsPerTransaction = 100;
        private const int Transactions = 500;
        private const string Path = @"e:\data\bench.data";

        public static void Main()
        {
#if DEBUG
            var oldfg = Console.ForegroundColor;
            var oldbg = Console.BackgroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.BackgroundColor = ConsoleColor.Yellow;
            Console.Beep();
            Console.WriteLine("Dude, you are running benchmark in debug mode?!");
            Console.ForegroundColor = oldfg;
            Console.BackgroundColor = oldbg;
#endif
            _randomNumbers = InitRandomNumbers(Transactions * ItemsPerTransaction);

            //Time("fill seq none", sw => FillSeqOneTransaction(sw, FlushMode.None));
            //Time("fill seq buff", sw => FillSeqOneTransaction(sw, FlushMode.Buffers));
            //Time("fill seq sync", sw => FillSeqOneTransaction(sw, FlushMode.Full));

            //Time("fill seq none separate tx", sw => FillSeqMultipleTransaction(sw, FlushMode.None));
            //Time("fill seq buff separate tx", sw => FillSeqMultipleTransaction(sw, FlushMode.Buffers));
            Time("fill seq sync separate tx", sw => FillSeqMultipleTransaction(sw, FlushMode.Full));

            //Time("fill rnd none", sw => FillRandomOneTransaction(sw, FlushMode.None));
            //Time("fill rnd buff", sw => FillRandomOneTransaction(sw, FlushMode.Buffers));
            //Time("fill rnd sync", sw => FillRandomOneTransaction(sw, FlushMode.Full));

            //Time("fill rnd none separate tx", sw => FillRandomMultipleTransaction(sw, FlushMode.None));
            //Time("fill rnd buff separate tx", sw => FillRandomMultipleTransaction(sw, FlushMode.Buffers));
            //Time("fill rnd sync separate tx", sw => FillRandomMultipleTransaction(sw, FlushMode.Full));

            //Time("Data for tests", sw => FillSeqOneTransaction(sw, FlushMode.None));

            //Time("read seq", ReadOneTransaction, delete: false);
            //Time("read parallel 1", sw => ReadOneTransaction_Parallel(sw, 1), delete: false);
            //Time("read parallel 2", sw => ReadOneTransaction_Parallel(sw, 2), delete: false);
            //Time("read parallel 4", sw => ReadOneTransaction_Parallel(sw, 4), delete: false);
            //Time("read parallel 8", sw => ReadOneTransaction_Parallel(sw, 8), delete: false);
            //Time("read parallel 16", sw => ReadOneTransaction_Parallel(sw, 16), delete: false);

            //Time("fill seq non then read parallel 4", stopwatch => ReadAndWriteOneTransaction(stopwatch, 4));

        }

        private static void FlushOsBuffer()
        {
            //const FileOptions fileFlagNoBuffering = (FileOptions)0x20000000;

            //using (new FileStream(Path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite, 4096,
            //                           fileFlagNoBuffering))
            //{

            //}
        }

        private static HashSet<long> InitRandomNumbers(int count)
        {
            var random = new Random(1337 ^ 13);
            var randomNumbers = new HashSet<long>();
            while (randomNumbers.Count < count)
            {
                randomNumbers.Add(random.Next(0, int.MaxValue));
            }
            return randomNumbers;
        }

        private static void Time(string name, Action<Stopwatch> action, bool delete = true)
        {
            if (File.Exists(Path) && delete)
                File.Delete(Path);
            else
                FlushOsBuffer();
            var sp = new Stopwatch();
            Console.Write("{0,-35}: running...", name);
            action(sp);

            Console.WriteLine("\r{0,-35}: {1,10:#,#} ms {2,10:#,#} ops / sec", name, sp.ElapsedMilliseconds, Transactions * ItemsPerTransaction / sp.Elapsed.TotalSeconds);
        }

        private static void FillRandomOneTransaction(Stopwatch sw, FlushMode flushMode)
        {
            var memoryMapPager = new MemoryMapPager(Path, flushMode);
            using (var env = new StorageEnvironment(memoryMapPager))
            {
                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    memoryMapPager.AllocateMorePages(tx, 1024 * 1024 * 768);
                    tx.Commit();
                }

                sw.Start();
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    foreach (var l in _randomNumbers)
                    {
                        ms.Position = 0;
                        env.Root.Add(tx, l.ToString("0000000000000000"), ms);
                    }

                    tx.Commit();
                }
                sw.Stop();
            }
        }


        private static void FillSeqOneTransaction(Stopwatch sw, FlushMode flushMode)
        {
            var memoryMapPager = new MemoryMapPager(Path, flushMode);
            using (var env = new StorageEnvironment(memoryMapPager))
            {
                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    memoryMapPager.AllocateMorePages(tx, 1024 * 1024 * 768);
                    tx.Commit();
                }


                sw.Start();
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    for (long i = 0; i < Transactions * ItemsPerTransaction; i++)
                    {
                        ms.Position = 0;
                        env.Root.Add(tx, i.ToString("0000000000000000"), ms);
                    }

                    tx.Commit();
                }
                sw.Stop();
            }
        }

        private static void FillRandomMultipleTransaction(Stopwatch sw, FlushMode flushMode)
        {
            var memoryMapPager = new MemoryMapPager(Path, flushMode);
            using (var env = new StorageEnvironment(memoryMapPager))
            {
                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    memoryMapPager.AllocateMorePages(tx, 1024 * 1024 * 768);
                    tx.Commit();
                }

                sw.Start();
                var enumerator = _randomNumbers.GetEnumerator();
                for (int x = 0; x < Transactions; x++)
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        for (long i = 0; i < ItemsPerTransaction; i++)
                        {
                            ms.Position = 0;
                            enumerator.MoveNext();
                            env.Root.Add(tx, (enumerator.Current).ToString("0000000000000000"), ms);
                        }
                        tx.Commit();
                    }
                }
                sw.Stop();
            }
        }

        private static void FillSeqMultipleTransaction(Stopwatch sw, FlushMode flushMode)
        {
            var memoryMapPager = new MemoryMapPager(Path, flushMode);
            using (var env = new StorageEnvironment(memoryMapPager))
            {
                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    memoryMapPager.AllocateMorePages(tx, 1024 * 1024 * 768);
                    tx.Commit();
                }

                sw.Start();
                int counter = 0;
                for (int x = 0; x < Transactions; x++)
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        for (long i = 0; i < ItemsPerTransaction; i++)
                        {
                            ms.Position = 0;
                            env.Root.Add(tx, (counter++).ToString("0000000000000000"), ms);
                        }

                        tx.Commit();
                    }
                }
                sw.Stop();
            }
        }


        private static void ReadOneTransaction_Parallel(Stopwatch sw, int concurrency)
        {
            using (var env = new StorageEnvironment(new MemoryMapPager(Path)))
            {
                var countdownEvent = new CountdownEvent(concurrency);

                sw.Start();
                for (int i = 0; i < concurrency; i++)
                {
                    var currentBase = i;
                    ThreadPool.QueueUserWorkItem(state =>
                    {
                        using (var tx = env.NewTransaction(TransactionFlags.Read))
                        {
                            var ms = new byte[100];
                            for (int j = 0; j < ((ItemsPerTransaction * Transactions) / concurrency); j++)
                            {
                                var current = j * currentBase;
                                var key = current.ToString("0000000000000000");
                                using (var stream = env.Root.Read(tx, key).Stream)
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

        private static void ReadOneTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(new MemoryMapPager(Path)))
            {
                sw.Start();
                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var ms = new byte[100];
                    for (int i = 0; i < Transactions * ItemsPerTransaction; i++)
                    {
                        var key = i.ToString("0000000000000000");
                        using (var stream = env.Root.Read(tx, key).Stream)
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

        private static void ReadAndWriteOneTransaction(Stopwatch sw, int concurrency)
        {
            var memoryMapPager = new MemoryMapPager(Path, FlushMode.None);
            using (var env = new StorageEnvironment(memoryMapPager))
            {
                var value = new byte[100];
                new Random().NextBytes(value);

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    memoryMapPager.AllocateMorePages(tx, 1024 * 1024 * 768);
                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var ms = new MemoryStream(value);
                    for (long i = 0; i < Transactions * ItemsPerTransaction; i++)
                    {
                        ms.Position = 0;
                        env.Root.Add(tx, i.ToString("0000000000000000"), ms);
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
                        using (var tx = env.NewTransaction(TransactionFlags.Read))
                        {
                            var ms = new byte[100];
                            for (int j = 0; j < ((ItemsPerTransaction*Transactions)/concurrency); j++)
                            {
                                var current = j * currentBase;
                                var key = current.ToString("0000000000000000");
                                using (var stream = env.Root.Read(tx, key).Stream)
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
