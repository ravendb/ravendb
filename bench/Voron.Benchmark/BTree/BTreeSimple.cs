using BenchmarkDotNet.Attributes;
using Sparrow.Server;
using Voron.Data.BTrees;

namespace Voron.Benchmark.BTree
{
    public class BTreeSimple : StorageBenchmark
    {
        /// <summary>
        /// Ensure we don't have to re-create the BTree between benchmarks
        /// </summary>
        public override bool DeleteBeforeEachBenchmark { get; protected set; } = false;

        private static readonly Slice TreeNameSlice;
        private static readonly Slice KeyValueSlice;

        static BTreeSimple()
        {
            Slice.From(Configuration.Allocator, nameof(BTreeSimple), ByteStringType.Immutable, out TreeNameSlice);
            Slice.From(Configuration.Allocator, nameof(KeyValueSlice), ByteStringType.Immutable, out KeyValueSlice);
        }

        [GlobalSetup]
        public override void Setup()
        {
            base.Setup();

            bool hasTree;
            using (var tx = Env.ReadTransaction())
            {
                Tree tree = tx.ReadTree(TreeNameSlice);
                hasTree = tree != null;
            }

            if (hasTree != false)
                return;

            // Create the tree as is does not exist
            using (var tx = Env.WriteTransaction())
            {
                Tree tree = tx.CreateTree(TreeNameSlice);
                tree.Add(KeyValueSlice, KeyValueSlice);
                tx.Commit();
            }
        }

        private const int OpsCount = 64;

        [Benchmark(OperationsPerInvoke = OpsCount)]
        public int ReadTree()
        {
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree(TreeNameSlice);

                var count = 0;
                for (int i = 0; i < OpsCount; i++)
                {
                    if (tree.TryRead(KeyValueSlice, out _))
                        count++;
                }
                return count;
            }
        }
    }
}
