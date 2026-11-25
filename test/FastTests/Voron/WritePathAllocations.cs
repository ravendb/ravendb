using System;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron
{
    public unsafe class WritePathAllocations(ITestOutputHelper output) : StorageTest(output)
    {
        private const int NumberOfKeys = 100_000;
        private const int Stride = 997; // prime, so consecutive operations land on different leaf pages
        private const int NumberOfOperations = 20_000;

        [RavenFact(RavenTestCategory.Voron)]
        public void Tree_operations()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < NumberOfKeys; i++)
                {
                    using (Slice.From(tx.Allocator, $"k{i:D8}", out Slice key))
                        tree.Add(key, (long)i);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("foo");
                var keys = CreateStridedKeys(tx, "k{0:D8}");

                var i = 0;
                AssertAtMost(8, "Tree.TryRead, scattered", MeasurePerOperation(() => tree.TryRead(keys[i++ % NumberOfKeys], out _)));

                using (var it = tree.Iterate(prefetch: false))
                {
                    var i1 = 0;
                    AssertAtMost(8, "TreeIterator.Seek, scattered", MeasurePerOperation(() => it.Seek(keys[i1++ % NumberOfKeys])));
                }

                var i2 = 0;
                AssertAtMost(8, "Tree iterator + Seek, reused", MeasurePerOperation(() =>
                {
                    using (var it = tree.Iterate(prefetch: false))
                        it.Seek(keys[i2++ % NumberOfKeys]);
                }));
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");
                var keys = CreateStridedKeys(tx, "k{0:D8}");

                for (int j = 0; j < NumberOfKeys; j++)
                    tree.Add(keys[j], (long)j);

                var i1 = 0;
                AssertAtMost(8, "Tree.Add update, scattered", MeasurePerOperation(() => tree.Add(keys[i1++ % NumberOfKeys], (long)i1)));

                var sequential = CreateKeys(tx, "k{0:D8}", stride: 1);
                var i2 = 0;
                AssertAtMost(8, "Tree.Add update, sequential", MeasurePerOperation(() => tree.Add(sequential[i2++ % NumberOfKeys], (long)i2)));

                var appended = CreateKeys(tx, "z{0:D8}", stride: 1);
                var i3 = 0;
                AssertAtMost(16, "Tree.Add new key, appended", MeasurePerOperation(() => tree.Add(appended[i3++ % NumberOfKeys], (long)i3)));

                var i4 = 0;
                AssertAtMost(8, "Tree.Delete, scattered", MeasurePerOperation(() => tree.Delete(keys[i4++ % NumberOfKeys])));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Tree_with_compressed_leaves_operations()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo", flags: TreeFlags.LeafsCompressed);
                for (int i = 0; i < NumberOfKeys; i++)
                {
                    using (Slice.From(tx.Allocator, $"k{i:D8}", out Slice key))
                        tree.Add(key, (long)i);
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");
                var keys = CreateStridedKeys(tx, "k{0:D8}");

                for (int j = 0; j < NumberOfKeys; j++)
                    tree.Add(keys[j], (long)j);

                var i1 = 0;
                AssertAtMost(8, "Compressed Tree.Add update", MeasurePerOperation(() => tree.Add(keys[i1++ % NumberOfKeys], (long)i1)));

            }
        }

        // Before RavenDB-27880 these operations allocated a page object per tree level
        // (and FixedSizeTree operations read their header from the parent tree up to 3 times), 120 - 1,350 bytes per operation.
        private void AssertAtMost(int limit, string name, double bytesPerOperation)
        {
            Output.WriteLine($"{name,-32} {bytesPerOperation,8:F1} bytes/op");
#if !DEBUG // debug-only checks in Voron allocate too
            Assert.True(bytesPerOperation <= limit, $"{name} allocated {bytesPerOperation:F1} bytes/op, expected at most {limit}");
#endif
        }

        private static Slice[] CreateStridedKeys(global::Voron.Impl.Transaction tx, string format) => CreateKeys(tx, format, Stride);

        private static Slice[] CreateKeys(global::Voron.Impl.Transaction tx, string format, int stride)
        {
            var keys = new Slice[NumberOfKeys];
            for (int i = 0; i < NumberOfKeys; i++)
                Slice.From(tx.Allocator, string.Format(format, (long)i * stride % NumberOfKeys), ByteStringType.Immutable, out keys[i]);
            return keys;
        }

        private static double MeasurePerOperation(Action action)
        {
            for (int i = 0; i < 1_000; i++) // warm up so JIT and pool filling are excluded from the measurement
                action();

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < NumberOfOperations; i++)
                action();

            return (double)(GC.GetAllocatedBytesForCurrentThread() - before) / NumberOfOperations;
        }
    }
}
