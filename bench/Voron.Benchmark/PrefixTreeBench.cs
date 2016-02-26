using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Data.Tables;
using Voron.Util.Conversion;

namespace Voron.Benchmark
{
    public unsafe class PrefixTreeBench
    {
        private HashSet<long> _randomNumbers;

        public PrefixTreeBench(HashSet<long> _randomNumbers)
        {
            this._randomNumbers = _randomNumbers;
        }

        protected TableSchema Configure(StorageEnvironment options)
        {
            return new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = 0,
                });
        }

        public unsafe long SetHelper(Table table, params object[] args)
        {
            var handles1 = new List<GCHandle>();

            var builder = new TableValueBuilder();
            foreach (var o in args)
            {
                byte[] buffer;
                GCHandle gcHandle;

                var s = o as string;
                if (s != null)
                {
                    buffer = Encoding.UTF8.GetBytes(s);
                    gcHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                    builder.Add((byte*)gcHandle.AddrOfPinnedObject(), buffer.Length);
                    handles1.Add(gcHandle);
                    continue;
                }

                var slice = o as Slice;
                if (slice != null)
                {
                    if (slice.Array == null)
                        throw new NotSupportedException();

                    gcHandle = GCHandle.Alloc(slice.Array, GCHandleType.Pinned);
                    builder.Add((byte*)gcHandle.AddrOfPinnedObject(), slice.Array.Length);
                    handles1.Add(gcHandle);

                    continue;
                }

                var stream = o as MemoryStream;
                if ( stream != null )
                {
                    buffer = stream.ToArray();
                    gcHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                    builder.Add((byte*)gcHandle.AddrOfPinnedObject(), buffer.Length);
                    handles1.Add(gcHandle);

                    continue;
                }

                var l = (long)o;
                buffer = EndianBitConverter.Big.GetBytes(l);
                gcHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                builder.Add((byte*)gcHandle.AddrOfPinnedObject(), buffer.Length);
                handles1.Add(gcHandle);
            }          

            var handles = handles1;

            long id = table.Set(builder);

            foreach (var gcHandle in handles)
            {
                gcHandle.Free();
            }

            return id;
        }

        public void Execute()
        {
            Console.WriteLine();
            Console.WriteLine("Prefix Tree Benchmarking.");
            Console.WriteLine();

            Benchmark.Time("fill seq", sw => FillSeqOneTransaction(sw));

            //Doesnt work yet. Problems with some data not being written when multiple transactions are involved.
            Benchmark.Time("fill seq separate tx", sw => FillSeqMultipleTransaction(sw));

            Benchmark.Time("fill rnd", sw => FillRandomOneTransaction(sw));

            //Doesnt work yet. Problems with some data not being written when multiple transactions are involved.
            Benchmark.Time("fill rnd separate tx", sw => FillRandomMultipleTransaction(sw));

            Benchmark.Time("Data for tests", sw => FillSeqOneTransaction(sw));

            //Benchmark.Time("read seq", ReadOneTransaction, delete: false);

            //Benchmark.Time("read parallel 1", sw => ReadOneTransaction_Parallel(sw, 1), delete: false);
            //Benchmark.Time("read parallel 2", sw => ReadOneTransaction_Parallel(sw, 2), delete: false);
            //Benchmark.Time("read parallel 4", sw => ReadOneTransaction_Parallel(sw, 4), delete: false);
            //Benchmark.Time("read parallel 8", sw => ReadOneTransaction_Parallel(sw, 8), delete: false);
            //Benchmark.Time("read parallel 16", sw => ReadOneTransaction_Parallel(sw, 16), delete: false);

            //Benchmark.Time("iterate parallel 1", sw => IterateAllKeysInOneTransaction_Parallel(sw, 1), delete: false);
            //Benchmark.Time("iterate parallel 2", sw => IterateAllKeysInOneTransaction_Parallel(sw, 2), delete: false);
            //Benchmark.Time("iterate parallel 4", sw => IterateAllKeysInOneTransaction_Parallel(sw, 4), delete: false);
            //Benchmark.Time("iterate parallel 8", sw => IterateAllKeysInOneTransaction_Parallel(sw, 8), delete: false);
            //Benchmark.Time("iterate parallel 16", sw => IterateAllKeysInOneTransaction_Parallel(sw, 16), delete: false);

            //Benchmark.Time("fill seq non then read parallel 4", stopwatch => ReadAndWriteOneTransaction(stopwatch, 4));
            //Benchmark.Time("fill seq non then read parallel 8", stopwatch => ReadAndWriteOneTransaction(stopwatch, 8));
            //Benchmark.Time("fill seq non then read parallel 16", stopwatch => ReadAndWriteOneTransaction(stopwatch, 16));
        }

        private void FillRandomOneTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            {
                var docsSchema = Configure(env);

                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    docsSchema.Create(tx, "docs");
                    tx.CreatePrefixTree("test");

                    tx.Commit();
                }

                sw.Start();
                using (var tx = env.WriteTransaction())
                {
                    var docs = new Table(docsSchema, "docs", tx);
                    var tree = tx.CreatePrefixTree("test");
                    foreach (var l in _randomNumbers)
                    {
                        ms.Position = 0;

                        var recordId = SetHelper(docs, l.ToString("0000000000000000"), ms);

                        int dataSize;
                        byte* data = docs.DirectRead(recordId, out dataSize);
                        var r = new TableValueReader(data, dataSize);

                        int keySize;
                        var key = r.Read(0, out keySize);
                        tree.Add(new Slice(key, (ushort)keySize), recordId);
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
                var docsSchema = Configure(env);

                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    docsSchema.Create(tx, "docs");
                    tx.CreatePrefixTree("test");

                    tx.Commit();
                }

                sw.Start();
                using (var tx = env.WriteTransaction())
                {
                    var docs = new Table(docsSchema, "docs", tx);
                    var tree = tx.CreatePrefixTree("test");
                    for (long i = 0; i < Configuration.Transactions * Configuration.ItemsPerTransaction; i++)
                    {
                        ms.Position = 0;

                        var recordId = SetHelper(docs, i.ToString("0000000000000000"), ms);

                        int dataSize;
                        byte* data = docs.DirectRead(recordId, out dataSize);
                        var r = new TableValueReader(data, dataSize);

                        int keySize;
                        var key = r.Read(0, out keySize);
                        tree.Add(new Slice(key, (ushort)keySize), recordId);
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
                var docsSchema = Configure(env);

                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    docsSchema.Create(tx, "docs");
                    tx.CreatePrefixTree("test");

                    tx.Commit();
                }

                sw.Start();
                var enumerator = _randomNumbers.GetEnumerator();
                for (int x = 0; x < Configuration.Transactions; x++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var docs = new Table(docsSchema, "docs", tx);
                        var tree = tx.CreatePrefixTree("test");

                        for (long i = 0; i < Configuration.ItemsPerTransaction; i++)
                        {
                            ms.Position = 0;

                            enumerator.MoveNext();

                            var recordId = SetHelper(docs, enumerator.Current.ToString("0000000000000000"), ms);

                            int dataSize;
                            byte* data = docs.DirectRead(recordId, out dataSize);
                            var r = new TableValueReader(data, dataSize);

                            int keySize;
                            var key = r.Read(0, out keySize);
                            tree.Add(new Slice(key, (ushort)keySize), recordId);
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
                var docsSchema = Configure(env);

                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    docsSchema.Create(tx, "docs");
                    tx.CreatePrefixTree("test");

                    tx.Commit();
                }

                sw.Start();
                int counter = 0;
                for (int x = 0; x < Configuration.Transactions; x++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var docs = new Table(docsSchema, "docs", tx);
                        var tree = tx.CreatePrefixTree("test");

                        for (long i = 0; i < Configuration.ItemsPerTransaction; i++)
                        {
                            ms.Position = 0;

                            var recordId = SetHelper(docs, (counter++).ToString("0000000000000000"), ms);

                            int dataSize;
                            byte* data = docs.DirectRead(recordId, out dataSize);
                            var r = new TableValueReader(data, dataSize);

                            int keySize;
                            var key = r.Read(0, out keySize);
                            tree.Add(new Slice(key, (ushort)keySize), recordId);
                        }

                        tx.Commit();
                    }
                }
                sw.Stop();
            }
        }


        private void ReadOneTransaction_Parallel(Stopwatch sw, int concurrency)
        {
            //using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            //{
            //    var countdownEvent = new CountdownEvent(concurrency);

            //    sw.Start();
            //    for (int i = 0; i < concurrency; i++)
            //    {
            //        var currentBase = i;
            //        ThreadPool.QueueUserWorkItem(state =>
            //        {
            //            using (var tx = env.ReadTransaction())
            //            {
            //                var tree = tx.ReadTree("test");
            //                var ms = new byte[100];
            //                for (int j = 0; j < ((Configuration.ItemsPerTransaction * Configuration.Transactions) / concurrency); j++)
            //                {
            //                    var current = j * currentBase;
            //                    var key = current.ToString("0000000000000000");
            //                    var stream = tree.Read(key).Reader;
            //                    while (stream.Read(ms, 0, ms.Length) != 0)
            //                    {
            //                    }
            //                }

            //                tx.Commit();
            //            }

            //            countdownEvent.Signal();
            //        });
            //    }
            //    countdownEvent.Wait();
            //    sw.Stop();
            //}
        }

        private void IterateAllKeysInOneTransaction_Parallel(Stopwatch sw, int concurrency)
        {
            //using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            //{
            //    var countdownEvent = new CountdownEvent(concurrency);

            //    sw.Start();
            //    for (int i = 0; i < concurrency; i++)
            //    {
            //        var currentBase = i;
            //        ThreadPool.QueueUserWorkItem(state =>
            //        {
            //            var local = 0;
            //            using (var tx = env.ReadTransaction())
            //            {
            //                var tree = tx.ReadTree("test");
            //                using (var it = tree.Iterate())
            //                {
            //                    if (it.Seek(Slice.BeforeAllKeys))
            //                    {
            //                        do
            //                        {
            //                            local += it.CurrentKey.Size;
            //                        } while (it.MoveNext());
            //                    }
            //                }
            //                tx.Commit();
            //            }

            //            countdownEvent.Signal();
            //        });
            //    }
            //    countdownEvent.Wait();
            //    sw.Stop();
            //}
        }


        private void ReadOneTransaction(Stopwatch sw)
        {
            //using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            //{
            //    sw.Start();
            //    using (var tx = env.ReadTransaction())
            //    {
            //        var test = tx.ReadTree("test");
            //        var ms = new byte[100];
            //        for (int i = 0; i < Configuration.Transactions * Configuration.ItemsPerTransaction; i++)
            //        {
            //            var key = i.ToString("0000000000000000");
            //            var stream = test.Read(key).Reader;
            //            {
            //                while (stream.Read(ms, 0, ms.Length) != 0)
            //                {
            //                }
            //            }
            //        }

            //        tx.Commit();
            //    }
            //    sw.Stop();
            //}
        }

        private void ReadAndWriteOneTransaction(Stopwatch sw, int concurrency)
        {
            //using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Configuration.Path)))
            //{
            //    var value = new byte[100];
            //    new Random().NextBytes(value);

            //    using (var tx = env.WriteTransaction())
            //    {
            //        env.Options.DataPager.EnsureContinuous(0, 256 * 1024);
            //        tx.CreateTree("test");

            //        tx.Commit();
            //    }

            //    using (var tx = env.WriteTransaction())
            //    {
            //        var tree = tx.CreateTree("test");

            //        var ms = new MemoryStream(value);
            //        for (long i = 0; i < Configuration.Transactions * Configuration.ItemsPerTransaction; i++)
            //        {
            //            ms.Position = 0;
            //            tree.Add(i.ToString("0000000000000000"), ms);
            //        }

            //        tx.Commit();
            //    }

            //    var countdownEvent = new CountdownEvent(concurrency);

            //    sw.Start();
            //    for (int i = 0; i < concurrency; i++)
            //    {
            //        var currentBase = i;
            //        ThreadPool.QueueUserWorkItem(state =>
            //        {
            //            using (var tx = env.ReadTransaction())
            //            {
            //                var tree = tx.ReadTree("test");
            //                var ms = new byte[100];
            //                for (int j = 0; j < ((Configuration.ItemsPerTransaction * Configuration.Transactions) / concurrency); j++)
            //                {
            //                    var current = j * currentBase;
            //                    var key = current.ToString("0000000000000000");
            //                    var stream = tree.Read(key).Reader;
            //                    {
            //                        while (stream.Read(ms, 0, ms.Length) != 0)
            //                        {
            //                        }
            //                    }
            //                }

            //                tx.Commit();
            //            }

            //            countdownEvent.Signal();
            //        });
            //    }
            //    countdownEvent.Wait();
            //    sw.Stop();
            //}
        }
    }
}
