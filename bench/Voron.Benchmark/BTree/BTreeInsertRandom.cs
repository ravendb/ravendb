using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using System.Linq;
using Sparrow.Server;

namespace Voron.Benchmark.BTree
{
    public class BTreeInsertRandom : StorageBenchmark
    {
        /// <summary>
        /// Ensure we don't have to re-create the BTree between benchmarks
        /// </summary>
        public override bool DeleteBeforeEachBenchmark { get; protected set; } = false;

        private static readonly Slice TreeNameSlice;

        private List<Tuple<Slice, Slice>>[] _pairs;

        [Params(100)]
        public int KeyLength { get; set; } = 100;

        /// <summary>
        /// Size of tree to create in order to write from (in number of nodes).
        /// This is the TOTAL SIZE after deletions
        /// </summary>
        [Params(Configuration.RecordsPerTransaction*Configuration.Transactions/2)]
        public int GenerationTreeSize { get; set; } = Configuration.RecordsPerTransaction*Configuration.Transactions/2;

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
        public double GenerationDeletionProbability { get; set; } = 0.1;

        /// <summary>
        /// Random seed used to generate values. If -1, uses time for seeding.
        /// </summary>
        [Params(-1)]
        public int RandomSeed { get; set; } = -1;

        static BTreeInsertRandom()
        {
            Slice.From(Configuration.Allocator, "TestTreeInsert", ByteStringType.Immutable, out TreeNameSlice);
        }

        [GlobalSetup]
        public override void Setup()
        {
            base.Setup();

            var randomSeed = RandomSeed == -1 ? null : RandomSeed as int?;

            Utils.GenerateWornoutTree(
                Env,
                TreeNameSlice,
                GenerationTreeSize,
                GenerationBatchSize,
                KeyLength,
                GenerationDeletionProbability,
                randomSeed);

            var totalPairs = Utils.GenerateUniqueRandomSlicePairs(
                NumberOfTransactions * NumberOfRecordsPerTransaction,
                KeyLength,
                randomSeed);

            _pairs = new List<Tuple<Slice, Slice>>[NumberOfTransactions];

            for (var i = 0; i < NumberOfTransactions; ++i)
            {
                _pairs[i] = totalPairs.Take(NumberOfRecordsPerTransaction).ToList();
                totalPairs.RemoveRange(0, NumberOfRecordsPerTransaction);
            }
        }

        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions)]
        public void InsertRandomOneTransaction()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(TreeNameSlice);

                for (var i = 0; i < NumberOfTransactions; i++)
                {
                    foreach (var pair in _pairs[i])
                    {
                        tree.Add(pair.Item1, pair.Item2);
                    }
                }

                tx.Commit();
            }
        }

        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions)]
        public void InsertRandomMultipleTransactions()
        {
            for (var i = 0; i < NumberOfTransactions; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree(TreeNameSlice);

                    foreach (var pair in _pairs[i])
                    {
                        tree.Add(pair.Item1, pair.Item2);
                    }

                    tx.Commit();
                }
            }
        }
    }
}
