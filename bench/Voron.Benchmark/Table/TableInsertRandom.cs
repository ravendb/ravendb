using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Sparrow.Server;
using Voron.Data.Tables;

namespace Voron.Benchmark.Table
{
    public class TableInsertRandom : StorageBenchmark
    {
        /// <summary>
        /// Ensure we don't have to re-create the Table between benchmarks
        /// </summary>
        public override bool DeleteBeforeEachBenchmark { get; protected set; } = false;

        private static readonly Slice TableNameSlice;
        private static readonly Slice SchemaPKNameSlice;
        private static readonly TableSchema Schema;

        private List<TableValueBuilder>[] _valueBuilders;

        [Params(100)]
        public int KeyLength { get; set; } = 100;

        /// <summary>
        /// Size of tree to create in order to write from (in number of nodes).
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
        public double GenerationDeletionProbability { get; set; } = 0.1;

        /// <summary>
        /// Random seed used to generate values. If -1, uses time for seeding.
        /// </summary>
        [Params(-1)]
        public int RandomSeed { get; set; } = -1;

        static TableInsertRandom()
        {
            Slice.From(Configuration.Allocator, "TestTable2", ByteStringType.Immutable, out TableNameSlice);
            Slice.From(Configuration.Allocator, "TestSchema2", ByteStringType.Immutable, out SchemaPKNameSlice);

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

        [GlobalSetup]
        public override void Setup()
        {
            base.Setup();

            var randomSeed = RandomSeed == -1 ? null : RandomSeed as int?;

            Utils.GenerateWornoutTable(
                Env,
                TableNameSlice,
                Schema,
                GenerationTableSize,
                GenerationBatchSize,
                KeyLength,
                GenerationDeletionProbability,
                randomSeed);

            var totalPairs = Utils.GenerateUniqueRandomSlicePairs(
                NumberOfTransactions * NumberOfRecordsPerTransaction,
                KeyLength,
                randomSeed);

            _valueBuilders = new List<TableValueBuilder>[NumberOfTransactions];

            for (var i = 0; i < NumberOfTransactions; ++i)
            {
                _valueBuilders[i] = new List<TableValueBuilder>();

                foreach (var pair in totalPairs.Take(NumberOfRecordsPerTransaction))
                {
                    _valueBuilders[i].Add(new TableValueBuilder
                    {
                        pair.Item1,
                        pair.Item2
                    });
                }

                totalPairs.RemoveRange(0, NumberOfRecordsPerTransaction);
            }
        }

        [Benchmark(OperationsPerInvoke = Configuration.RecordsPerTransaction * Configuration.Transactions)]
        public void InsertRandomOneTransaction()
        {
            using (var tx = Env.WriteTransaction())
            {
                var table = tx.OpenTable(Schema, TableNameSlice);

                for (var i = 0; i < NumberOfTransactions; i++)
                {
                    foreach (var value in _valueBuilders[i])
                    {
                        table.Insert(value);
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
                    var table = tx.OpenTable(Schema, TableNameSlice);

                    foreach (var value in _valueBuilders[i])
                    {
                        table.Insert(value);
                    }

                    tx.Commit();
                }
            }
        }
    }
}
