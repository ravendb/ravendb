using Sparrow;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Voron.Data.Tables;
using Voron.Util.Conversion;

namespace Voron.Benchmark
{
    public unsafe class TableBench : IHasStorageLocation
    {
        public string Path 
        {
            get
            {
                string posfix = _indexType == TableIndexType.Compact ? "prefix" : "btree";
                return $"{Configuration.Path}.table.{posfix}";
            }
        }

        private HashSet<long> _randomNumbers;
        private TableIndexType _indexType;

        public TableBench(HashSet<long> _randomNumbers, TableIndexType indexType)
        {
            this._randomNumbers = _randomNumbers;
            this._indexType = indexType;
        }

        protected TableSchema Configure(StorageEnvironment options)
        {
            return new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = 0,
                    Type = _indexType,
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

            string benchmarkType = _indexType == TableIndexType.Compact ? "Prefix Tree" : "BTree";
            Console.WriteLine($"{benchmarkType} Table Benchmarking.");
            Console.WriteLine();

            Benchmark.Time("fill rnd", sw => FillRandomOneTransaction(sw), this);
            Benchmark.Time("fill rnd separate tx", sw => FillRandomMultipleTransaction(sw), this);
            Benchmark.Time("insert rnd separate tx", sw => InsertRandomMultipleTransactionAfterFill(sw), this, delete: false, records: Configuration.ItemsPerTransaction * 100);

            Benchmark.Time("fill seq", sw => FillSeqOneTransaction(sw), this);
            Benchmark.Time("fill seq separate tx", sw => FillSeqMultipleTransaction(sw), this);

            Benchmark.Time("read seq", ReadOneTransaction, this, delete: false);

            Benchmark.Time("read parallel 1", sw => ReadOneTransaction_Parallel(sw, 1), this, delete: false);
            Benchmark.Time("read parallel 2", sw => ReadOneTransaction_Parallel(sw, 2), this, delete: false);
            Benchmark.Time("read parallel 4", sw => ReadOneTransaction_Parallel(sw, 4), this, delete: false);
            Benchmark.Time("read parallel 8", sw => ReadOneTransaction_Parallel(sw, 8), this, delete: false);
            Benchmark.Time("read parallel 16", sw => ReadOneTransaction_Parallel(sw, 16), this, delete: false);

            if (_indexType != TableIndexType.Compact)
            {
                Benchmark.Time("iterate parallel 1", sw => IterateAllKeysInOneTransaction_Parallel(sw, 1), this, delete: false);
                Benchmark.Time("iterate parallel 2", sw => IterateAllKeysInOneTransaction_Parallel(sw, 2), this, delete: false);
                Benchmark.Time("iterate parallel 4", sw => IterateAllKeysInOneTransaction_Parallel(sw, 4), this, delete: false);
                Benchmark.Time("iterate parallel 8", sw => IterateAllKeysInOneTransaction_Parallel(sw, 8), this, delete: false);
                Benchmark.Time("iterate parallel 16", sw => IterateAllKeysInOneTransaction_Parallel(sw, 16), this, delete: false);
            }

            Benchmark.Time("fill seq non then read parallel 4", stopwatch => ReadAndWriteOneTransaction(stopwatch, 4), this);
            Benchmark.Time("fill seq non then read parallel 8", stopwatch => ReadAndWriteOneTransaction(stopwatch, 8), this);
            Benchmark.Time("fill seq non then read parallel 16", stopwatch => ReadAndWriteOneTransaction(stopwatch, 16), this);
        }

        private void InsertRandomMultipleTransactionAfterFill(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
            {
                var docsSchema = Configure(env);

                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                sw.Start();
                var enumerator = _randomNumbers.GetEnumerator();
                for (int x = 0; x < 100; x++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var docs = new Table(docsSchema, "docs", tx);

                        for (long i = 0; i < Configuration.ItemsPerTransaction; i++)
                        {
                            ms.Position = 0;

                            enumerator.MoveNext();

                            SetHelper(docs, enumerator.Current.ToString("0000000000000000"), ms);
                        }

                        tx.Commit();
                    }
                }
                sw.Stop();
            }
        }

        private void FillRandomOneTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
            {
                var docsSchema = Configure(env);

                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    docsSchema.Create(tx, "docs");

                    tx.Commit();
                }

                sw.Start();
                using (var tx = env.WriteTransaction())
                {
                    var docs = new Table(docsSchema, "docs", tx);
                    foreach (var l in _randomNumbers)
                    {
                        ms.Position = 0;

                        SetHelper(docs, l.ToString("0000000000000000"), ms);
                    }

                    tx.Commit();
                }
                sw.Stop();
            }
        }


        private void FillSeqOneTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
            {
                var docsSchema = Configure(env);

                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    docsSchema.Create(tx, "docs");
                    tx.Commit();
                }

                sw.Start();
                using (var tx = env.WriteTransaction())
                {
                    var docs = new Table(docsSchema, "docs", tx);
                    for (long i = 0; i < Configuration.Transactions * Configuration.ItemsPerTransaction; i++)
                    {
                        ms.Position = 0;

                        SetHelper(docs, i.ToString("0000000000000000"), ms);
                    }

                    tx.Commit();
                }
                sw.Stop();
            }
        }

        private void FillRandomMultipleTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
            {
                var docsSchema = Configure(env);

                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    docsSchema.Create(tx, "docs");
                    tx.Commit();
                }

                sw.Start();
                var enumerator = _randomNumbers.GetEnumerator();
                for (int x = 0; x < Configuration.Transactions; x++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var docs = new Table(docsSchema, "docs", tx);

                        for (long i = 0; i < Configuration.ItemsPerTransaction; i++)
                        {
                            ms.Position = 0;

                            enumerator.MoveNext();

                            SetHelper(docs, enumerator.Current.ToString("0000000000000000"), ms);
                        }

                        tx.Commit();
                    }
                }

                sw.Stop();
            }
        }

        private void FillSeqMultipleTransaction(Stopwatch sw)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
            {
                var docsSchema = Configure(env);

                var value = new byte[100];
                new Random().NextBytes(value);
                var ms = new MemoryStream(value);

                using (var tx = env.WriteTransaction())
                {
                    docsSchema.Create(tx, "docs");
                    tx.Commit();
                }

                sw.Start();
                int counter = 0;
                for (int x = 0; x < Configuration.Transactions; x++)
                {
                    var sp = Stopwatch.StartNew();
                    using (var tx = env.WriteTransaction())
                    {
                        var docs = new Table(docsSchema, "docs", tx);

                        for (long i = 0; i < Configuration.ItemsPerTransaction; i++)
                        {
                            ms.Position = 0;

                            SetHelper(docs, (counter++).ToString("0000000000000000"), ms);
                        }

                        tx.Commit();

                    }
                }
                sw.Stop();
            }
        }


        private void ReadOneTransaction_Parallel(Stopwatch sw, int concurrency)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
            {
                var docsSchema = Configure(env);

                Exception exceptionHappened = null;

                sw.Start();

                var tasks = new Task[concurrency];
                for (int i = 0; i < concurrency; i++)
                {
                    var currentBase = i;
                    tasks[i] = Task.Factory.StartNew(()=>                    
                    {
                        using (var tx = env.ReadTransaction())
                        {
                            var docs = new Table(docsSchema, "docs", tx);

                            var ms = new byte[100];
                            for (int j = 0; j < ((Configuration.ItemsPerTransaction * Configuration.Transactions) / concurrency); j++)
                            {
                                var current = j * currentBase;
                                var key = current.ToString("0000000000000000");
                                var tableReader = docs.ReadByKey(key);

                                int size;
                                byte* buffer = tableReader.Read(1, out size);

                                fixed (byte* msPtr = ms)
                                {
                                    Memory.Copy(msPtr, buffer, size);
                                }
                            }

                            tx.Commit();
                        }
                    });
                    tasks[i].ContinueWith(t =>
                    {
                       exceptionHappened = t.Exception.InnerException;
                    }, TaskContinuationOptions.OnlyOnFaulted);                                      
                }

                Task.WaitAll(tasks);

                if (exceptionHappened != null)
                    throw exceptionHappened;

                sw.Stop();
            }
        }

        private void IterateAllKeysInOneTransaction_Parallel(Stopwatch sw, int concurrency)
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
            {
                var docsSchema = Configure(env);

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
                            var docs = new Table(docsSchema, "docs", tx);

                            foreach (var reader in docs.SeekByPrimaryKey(Slice.BeforeAllKeys) )
                            {
                                int size;
                                reader.Read(0, out size);
                                local += size;
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
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
            {
                var docsSchema = Configure(env);

                sw.Start();
                using (var tx = env.ReadTransaction())
                {
                    var docs = new Table(docsSchema, "docs", tx);

                    var ms = new byte[100];
                    for (int i = 0; i < Configuration.Transactions * Configuration.ItemsPerTransaction; i++)
                    {
                        var key = i.ToString("0000000000000000");
                        var tableReader = docs.ReadByKey(key);

                        int size;
                        byte* buffer = tableReader.Read(1, out size);

                        fixed (byte* msPtr = ms)
                        {
                            Memory.Copy(msPtr, buffer, size);
                        }
                    }

                    tx.Commit();
                }
                sw.Stop();
            }
        }

        private void ReadAndWriteOneTransaction(Stopwatch sw, int concurrency)
        {
            //using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(Path)))
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
