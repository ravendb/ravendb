using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Nevar.Debugging;
using Nevar.Impl;

namespace Nevar.Benchmark
{
    unsafe class Program
    {
        public const int Count = 1000 * 1000;
        private const string _path = @"C:\Users\Ayende\Downloads\bench.data";

        static void Main()
        {
            //Time("fill seq none", () => FillSeqOneTransaction(FlushMode.None));
            //Time("fill seq buff", () => FillSeqOneTransaction(FlushMode.Buffers));
            //Time("fill seq sync", () => FillSeqOneTransaction(FlushMode.Full));

            Time("fill seq non 10,000 transactions", () => FillSeqMultipleTransaction(FlushMode.None, 10 * 1000));

            Time("read seq", ReadOneTransaction, delete: false);
            Time("read parallel 1", () => ReadOneTransaction_Parallel(1), delete: false);
            Time("read parallel 2", () => ReadOneTransaction_Parallel(2), delete: false);
            Time("read parallel 4", () => ReadOneTransaction_Parallel(4), delete: false);
            Time("read parallel 8", () => ReadOneTransaction_Parallel(8), delete: false);
            Time("read parallel 16", () => ReadOneTransaction_Parallel(16), delete: false);
        }

        private static void Time(string name, Action action, bool delete = true)
        {
            if (File.Exists(_path) && delete)
                File.Delete(_path);
            var sp = Stopwatch.StartNew();
            action();
            sp.Stop();

            Console.WriteLine("{0} took\t{1:#,#} ms \t {2:#,#} ops / sec", name, sp.ElapsedMilliseconds, Count / sp.Elapsed.TotalSeconds);
        }

        private static void FillSeqOneTransaction(FlushMode flushMode)
        {
            using (var env = new StorageEnvironment(new MemoryMapPager(_path, flushMode)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var value = new byte[100];
                    new Random().NextBytes(value);
                    var ms = new MemoryStream(value);
                    for (long i = 0; i < Count; i++)
                    {
                        ms.Position = 0;
                        env.Root.Add(tx, i.ToString("0000000000000000"), ms);
                    }

                    tx.Commit();
                }
            }
        }

        private static void FillSeqMultipleTransaction(FlushMode flushMode, int parts)
        {
            using (var env = new StorageEnvironment(new MemoryMapPager(_path, flushMode)))
            {
                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value); 
                
                for (int x = 0; x < parts; x++)
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        for (long i = 0; i < Count / parts; i++)
                        {
                            ms.Position = 0;
                            env.Root.Add(tx, (x * i).ToString("0000000000000000"), ms);
                        }

                        tx.Commit();
                    }
                    if (PagerState.Instances.Count != 1)
                    {
                        
                    }
                }
            }
        }


        private static void ReadOneTransaction_Parallel(int parts)
        {
            using (var env = new StorageEnvironment(new MemoryMapPager(_path)))
            {
                var countdownEvent = new CountdownEvent(parts);
                for (int i = 0; i < parts; i++)
                {
                    var currentBase = i;
                    ThreadPool.QueueUserWorkItem(state =>
                    {
                        using (var tx = env.NewTransaction(TransactionFlags.Read))
                        {
                            var ms = new MemoryStream(100);
                            for (int j = 0; j < Count / parts; j++)
                            {
                                var current = j * currentBase;
                                var key = current.ToString("0000000000000000");
                                using (var stream = env.Root.Read(tx, key))
                                {
                                    ms.Position = 0;
                                    stream.CopyTo(ms);
                                }
                            }

                            tx.Commit();
                        }

                        countdownEvent.Signal();
                    });
                }
                countdownEvent.Wait();
            }
        }

        private static void ReadOneTransaction()
        {
            using (var env = new StorageEnvironment(new MemoryMapPager(_path)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var ms = new MemoryStream(100);
                    for (int i = 0; i < Count; i++)
                    {
                        var key = i.ToString("0000000000000000");
                        using (var stream = env.Root.Read(tx, key))
                        {
                            ms.Position = 0;
                            stream.CopyTo(ms);
                        }
                    }

                    tx.Commit();
                }
            }
        }



    }
}
