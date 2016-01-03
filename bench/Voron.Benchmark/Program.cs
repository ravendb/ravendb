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
        private const string Path = @"c:\temp\bench.data";

        public static void Main()
        {
#if DEBUG
            var oldfg = Console.ForegroundColor;
            var oldbg = Console.BackgroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.BackgroundColor = ConsoleColor.Yellow;
      
            Console.WriteLine("Dude, you are running benchmark in debug mode?!");
            Console.ForegroundColor = oldfg;
            Console.BackgroundColor = oldbg;
#endif
            _randomNumbers = InitRandomNumbers(Transactions * ItemsPerTransaction);

            Time("fill seq", sw => FillSeqOneTransaction(sw));

            Time("fill seq separate tx", sw => FillSeqMultipleTransaction(sw));

            Time("fill rnd", sw => FillRandomOneTransaction(sw));

            Time("fill rnd separate tx", sw => FillRandomMultipleTransaction(sw));

            Time("Data for tests", sw => FillSeqOneTransaction(sw));

            Time("read seq", ReadOneTransaction, delete: false);

            Time("read parallel 1", sw => ReadOneTransaction_Parallel(sw, 1), delete: false);
            Time("read parallel 2", sw => ReadOneTransaction_Parallel(sw, 2), delete: false);
            Time("read parallel 4", sw => ReadOneTransaction_Parallel(sw, 4), delete: false);
            Time("read parallel 8", sw => ReadOneTransaction_Parallel(sw, 8), delete: false);
            Time("read parallel 16", sw => ReadOneTransaction_Parallel(sw, 16), delete: false);

            Time("fill seq non then read parallel 4", stopwatch => ReadAndWriteOneTransaction(stopwatch, 4));
            Time("fill seq non then read parallel 8", stopwatch => ReadAndWriteOneTransaction(stopwatch, 8));
            Time("fill seq non then read parallel 16", stopwatch => ReadAndWriteOneTransaction(stopwatch, 16));
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
            if (delete)
                DeleteDirectory(Path);

            var sp = new Stopwatch();
            Console.Write("{0,-35}: running...", name);
            action(sp);

            Console.WriteLine("\r{0,-35}: {1,10:#,#} ms {2,10:#,#} ops / sec", name, sp.ElapsedMilliseconds, Transactions * ItemsPerTransaction / sp.Elapsed.TotalSeconds);
        }

        private static void DeleteDirectory(string dir)
        {
            if (Directory.Exists(dir) == false)
                return;

            for (int i = 0; i < 10; i++)
            {
                try
                {
                    Directory.Delete(dir, true);
                    return;
                }
                catch (DirectoryNotFoundException)
                {
                    return;
                }
                catch (Exception)
                {
                    Thread.Sleep(13);
                }
            }

            Directory.Delete(dir, true);
        }

        private static void FillRandomOneTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
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


        private static void FillSeqOneTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
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
                    for (long i = 0; i < Transactions * ItemsPerTransaction; i++)
                    {
                        ms.Position = 0;
                        tree.Add(i.ToString("0000000000000000"), ms);
                    }

                    tx.Commit();
                }
                sw.Stop();
            }
        }

        private static void FillRandomMultipleTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
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
                for (int x = 0; x < Transactions; x++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("test");
                        for (long i = 0; i < ItemsPerTransaction; i++)
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

        private static void FillSeqMultipleTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
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
                for (int x = 0; x < Transactions; x++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("test");
                        for (long i = 0; i < ItemsPerTransaction; i++)
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


        private static void ReadOneTransaction_Parallel(Stopwatch sw, int concurrency)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
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
                            for (int j = 0; j < ((ItemsPerTransaction * Transactions) / concurrency); j++)
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

        private static void ReadOneTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
            {
                sw.Start();
                using (var tx = env.ReadTransaction())
                {
                    var test = tx.ReadTree("test");
                    var ms = new byte[100];
                    for (int i = 0; i < Transactions * ItemsPerTransaction; i++)
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

        private static void ReadAndWriteOneTransaction(Stopwatch sw, int concurrency)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
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
                    for (long i = 0; i < Transactions * ItemsPerTransaction; i++)
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
                            for (int j = 0; j < ((ItemsPerTransaction * Transactions) / concurrency); j++)
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
