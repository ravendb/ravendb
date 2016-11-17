using System.IO;
using Voron.Data;
using Voron.Data.Tables;

namespace Voron.Benchmark.Table
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using BenchmarkDotNet.Attributes;
    using Sparrow;

    public class TableReadAndIterate : StorageBenchmark
    {
        /// <summary>
        /// Ensure we don't have to re-create the Table between benchmarks
        /// </summary>
        public override bool DeleteBeforeEachBenchmark { get; protected set; } = false;

        private static readonly Slice TableNameSlice;
        private static readonly Slice SchemaPKNameSlice;
        private static readonly TableSchema Schema;

        private readonly Dictionary<int, List<Slice>> _keysPerThread = new Dictionary<int, List<Slice>>();
        private readonly Dictionary<int, List<Slice>> _sortedKeysPerThread = new Dictionary<int, List<Slice>>();

        /// <summary>
        /// Length of the keys to be inserted when filling randomly (bytes).
        /// </summary>
        [Params(100)]
        public int KeyLength { get; set; } = 100;

        /// <summary>
        /// Size of tree to create in order to read from (in number of nodes).
        /// This is the TOTAL SIZE after deletions
        /// </summary>
        [Params(Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public int GenerationTableSize { get; set; } = Configuration.RecordsPerTransaction * Configuration.Transactions / 2;

        /// <summary>
        /// Size of batches to divide the insertion into. A lower number will
        /// generate more frequent deletions.
        /// 
        /// Beware of low batch sizes. Even though they generate a good wearing
        /// in the tree, too low of a number here may take a long time to
        /// converge.
        /// </summary>
        [Params(50000)]
        public int GenerationBatchSize { get; set; } = 50000;

        /// <summary>
        /// Probability that a node will be deleted after insertion.
        /// </summary>
        [Params(0.1)]
        public double GenerationDeletionProbability { get; set; } = 0.5;

        /// <summary>
        /// Random seed used to generate values. If -1, uses time for seeding.
        /// TODO: make this nullable. See https://github.com/PerfDotNet/BenchmarkDotNet/issues/271
        /// </summary>
        [Params(-1)]
        public int RandomSeed { get; set; } = -1;

        [Params(1, 2)]
        public int ReadParallelism { get; set; } = 1;

        static TableReadAndIterate()
        {
            Slice.From(Configuration.Allocator, "TestTableRead", ByteStringType.Immutable, out TableNameSlice);
            Slice.From(Configuration.Allocator, "TestSchemaRead", ByteStringType.Immutable, out SchemaPKNameSlice);

            Schema = new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    StartIndex = 0,
                    Count = 0,
                    IsGlobal = false,
                    Name = SchemaPKNameSlice,
                    Type = TableIndexType.BTree
                });
        }

        [Setup]
        public override void Setup()
        {
            base.Setup();

            var tableKeys = Utils.GenerateWornoutTable(
                Env,
                TableNameSlice,
                Schema,
                GenerationTableSize,
                GenerationBatchSize,
                KeyLength,
                GenerationDeletionProbability,
                RandomSeed == -1 ? null : RandomSeed as int?
            );

            // Distribute work amount, each one of the buckets is sorted
            for (var i = 0; i < ReadParallelism; i++)
            {
                _keysPerThread[i] = new List<Slice>();
                _sortedKeysPerThread[i] = new List<Slice>();
            }

            int treeKeyIndex = 0;

            foreach (var key in tableKeys)
            {
                _keysPerThread[treeKeyIndex % ReadParallelism].Add(key);
                _sortedKeysPerThread[treeKeyIndex % ReadParallelism].Add(key);
                treeKeyIndex++;
            }

            // TODO: parallell maybe?
            for (var i = 0; i < ReadParallelism; i++)
            {
                _sortedKeysPerThread[i].Sort(SliceComparer.Instance);
            }
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public void ReadRandomOneTransaction()
        {
            var countdownEvent = new CountdownEvent(ReadParallelism);

            for (int threadIndex = 0; threadIndex < ReadParallelism; threadIndex++)
            {
                ThreadPool.QueueUserWorkItem(state =>
                {
                    var currentThreadIndex = (int)state;

                    using (var tx = Env.ReadTransaction())
                    {
                        var table = tx.OpenTable(Schema, TableNameSlice);

                        foreach (var key in _keysPerThread[currentThreadIndex])
                        {
                            var reader = table.ReadByKey(key);

                            for (var f = 0; f < reader.Count; f++)
                            {
                                unsafe
                                {
                                    int size;
                                    byte* buffer = reader.Read(f, out size);

                                    while (size > 0)
                                        size--;
                                }
                            }
                        }

                        tx.Commit();
                    }

                    countdownEvent.Signal();
                }, threadIndex);
            }

            countdownEvent.Wait();
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public void ReadSeqOneTransaction()
        {
            var countdownEvent = new CountdownEvent(ReadParallelism);

            for (int threadIndex = 0; threadIndex < ReadParallelism; threadIndex++)
            {
                ThreadPool.QueueUserWorkItem(state =>
                {
                    var currentThreadIndex = (int)state;

                    using (var tx = Env.ReadTransaction())
                    {
                        var table = tx.OpenTable(Schema, TableNameSlice);

                        foreach (var key in _sortedKeysPerThread[currentThreadIndex])
                        {
                            var reader = table.ReadByKey(key);

                            for (var f = 0; f < reader.Count; f++)
                            {
                                unsafe
                                {
                                    int size;
                                    byte* buffer = reader.Read(f, out size);

                                    while (size > 0)
                                        size--;
                                }
                            }
                        }

                        tx.Commit();
                    }

                    countdownEvent.Signal();
                }, threadIndex);
            }

            countdownEvent.Wait();
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        // TODO: this is specially bad in this case, since the operations are actually *Parallelism
        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public void IterateAllKeysOneTransaction()
        {
            var countdownEvent = new CountdownEvent(ReadParallelism);

            for (int threadIndex = 0; threadIndex < ReadParallelism; threadIndex++)
            {
                ThreadPool.QueueUserWorkItem(state =>
                {
                    using (var tx = Env.ReadTransaction())
                    {
                        var table = tx.OpenTable(Schema, TableNameSlice);

                        foreach (var reader in table.SeekByPrimaryKey(Slices.BeforeAllKeys))
                        {
                            for (var f = 0; f < reader.Count; f++)
                            {
                                unsafe
                                {
                                    int size;
                                    reader.Read(f, out size);
                                }
                            }
                        }

                        tx.Commit();
                    }

                    countdownEvent.Signal();
                }, threadIndex);
            }

            countdownEvent.Wait();
        }

        // TODO: Fix. See: https://github.com/PerfDotNet/BenchmarkDotNet/issues/258
        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions / 2)]
        public void IterateThreadKeysOneTransaction()
        {
            var countdownEvent = new CountdownEvent(ReadParallelism);

            for (int threadIndex = 0; threadIndex < ReadParallelism; threadIndex++)
            {
                ThreadPool.QueueUserWorkItem(state =>
                {
                    var currentThreadIndex = (int)state;

                    using (var tx = Env.ReadTransaction())
                    {
                        var table = tx.OpenTable(Schema, TableNameSlice);

                        foreach (var reader in table.SeekByPrimaryKey(_sortedKeysPerThread[currentThreadIndex][0]))
                        {
                            for (var f = 0; f < reader.Count; f++)
                            {
                                unsafe
                                {
                                    int size;
                                    reader.Read(f, out size);
                                }
                            }
                        }

                        tx.Commit();
                    }

                    countdownEvent.Signal();
                }, threadIndex);
            }

            countdownEvent.Wait();
        }
    }
}