using System;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron.FixedSize
{
    public class FixedSizeTreeLookupCorrectness(ITestOutputHelper output) : StorageTest(output)
    {
        private static byte[] ValueOf(long key, int valueSize)
        {
            var value = new byte[valueSize];
            for (var i = 0; i < valueSize; i++)
                value[i] = (byte)(key * 31 + i);
            return value;
        }

        private static unsafe bool HasValue(byte* ptr, int size, long key, int valueSize)
        {
            if (ptr == null || size != valueSize)
                return false;

            var expected = ValueOf(key, valueSize);
            for (var i = 0; i < valueSize; i++)
            {
                if (ptr[i] != expected[i])
                    return false;
            }

            return true;
        }

        private void Fill(Slice treeId, int entries, int valueSize, long step)
        {
            using var tx = Env.WriteTransaction();
            var fst = tx.FixedTreeFor(treeId, (ushort)valueSize);
            for (var i = 0; i < entries; i++)
            {
                var key = i * step;
                fst.Add(key, ValueOf(key, valueSize));
            }

            tx.Commit();
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1, 8)]
        [InlineData(10, 8)]
        [InlineData(300, 8)]
        [InlineData(5_000, 8)]
        [InlineData(100_000, 8)]
        [InlineData(1, 256)]
        [InlineData(50, 256)]
        [InlineData(20_000, 256)]
        [InlineData(20_000, 1)]
        public unsafe void EveryStoredKeyIsFoundWithItsValueAndEveryOtherKeyIsMissing(int entries, int valueSize)
        {
            const long step = 3;
            Slice.From(Allocator, "tree", out var treeId);
            Fill(treeId, entries, valueSize, step);

            using var tx = Env.ReadTransaction();
            var fst = tx.FixedTreeFor(treeId, (ushort)valueSize);

            var wrong = 0;
            var falsePositives = 0;
            for (var i = 0; i < entries; i++)
            {
                var key = i * step;
                if (HasValue(fst.ReadPtr(key, out var size), size, key, valueSize) == false)
                    wrong++;

                using (fst.Read(key, out var slice))
                {
                    if (slice.HasValue == false || slice.Size != valueSize)
                        wrong++;
                }

                // between two stored keys, and before the first / after the last
                if (fst.ReadPtr(key + 1, out _) != null || fst.ReadPtr(key + 2, out _) != null || fst.ReadPtr(key - 1, out _) != null)
                    falsePositives++;
                if (fst.Contains(key + 1))
                    falsePositives++;
            }

            Assert.Equal(0, wrong);
            Assert.Equal(0, falsePositives);
            Assert.Null(fst.ReadPtr(long.MaxValue, out var sz));
            Assert.Equal(0, sz);
            Assert.Null(fst.ReadPtr(long.MinValue, out _));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void TreeThatWasNeverCreatedReturnsNothing()
        {
            Slice.From(Allocator, "missing", out var treeId);
            using var tx = Env.ReadTransaction();
            var fst = tx.FixedTreeFor(treeId, 8);

            Assert.Null(fst.ReadPtr(0, out var size));
            Assert.Equal(0, size);
            using (fst.Read(0, out var slice))
                Assert.False(slice.HasValue);
            Assert.False(fst.Contains(0));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void TreeThatWasEmptiedReturnsNothing()
        {
            const int entries = 20_000;
            Slice.From(Allocator, "tree", out var treeId);
            Fill(treeId, entries, 8, 1);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);
                for (var i = 0; i < entries; i++)
                    fst.Delete(i);
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);
                for (var i = 0; i < entries; i += 97)
                    Assert.Null(fst.ReadPtr(i, out _));
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(20, 8)]
        [InlineData(50_000, 8)]
        [InlineData(10_000, 128)]
        public unsafe void DeletedKeysAreMissingAndTheRestIsStillFound(int entries, int valueSize)
        {
            Slice.From(Allocator, "tree", out var treeId);
            Fill(treeId, entries, valueSize, 1);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, (ushort)valueSize);
                for (var i = 0; i < entries; i += 2)
                    fst.Delete(i);
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, (ushort)valueSize);
                var wrong = 0;
                for (var i = 0; i < entries; i++)
                {
                    var ptr = fst.ReadPtr(i, out var size);
                    var ok = i % 2 == 0 ? ptr == null : HasValue(ptr, size, i, valueSize);
                    if (ok == false)
                        wrong++;
                }

                Assert.Equal(0, wrong);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void LookupsInAWriteTransactionSeeUncommittedChangesOfTheSameTransaction()
        {
            Slice.From(Allocator, "tree", out var treeId);
            using var tx = Env.WriteTransaction();
            var fst = tx.FixedTreeFor(treeId, 8);

            Assert.Null(fst.ReadPtr(1, out _));

            // grows from nothing, to embedded, to a multi page tree: the header changes under the lookups
            for (var i = 0; i < 30_000; i++)
            {
                fst.Add(i, ValueOf(i, 8));
                if (i == 0 || i == 5 || i == 400 || i == 29_999)
                {
                    for (var j = 0; j <= i; j += Math.Max(1, i / 50))
                        Assert.True(HasValue(fst.ReadPtr(j, out var size), size, j, 8), $"key {j} after adding {i}");
                    Assert.Null(fst.ReadPtr(i + 1, out _));
                }
            }

            fst.Delete(100);
            Assert.Null(fst.ReadPtr(100, out _));
            Assert.True(HasValue(fst.ReadPtr(101, out var s), s, 101, 8));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void ReadTransactionKeepsItsSnapshotWhileAnotherTransactionModifiesTheTree()
        {
            Slice.From(Allocator, "tree", out var treeId);
            Fill(treeId, 20_000, 8, 1);

            using var oldTx = Env.ReadTransaction();
            var oldTree = oldTx.FixedTreeFor(treeId, 8);
            Assert.True(HasValue(oldTree.ReadPtr(5, out var size), size, 5, 8));

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);
                fst.Delete(5);
                for (var i = 20_000; i < 60_000; i++)
                    fst.Add(i, ValueOf(i, 8));
                tx.Commit();
            }

            // the first read happened before the change: the cached header must not leak the new state into the old snapshot
            Assert.True(HasValue(oldTree.ReadPtr(5, out size), size, 5, 8));
            Assert.Null(oldTree.ReadPtr(30_000, out _));
            Assert.True(HasValue(oldTree.ReadPtr(19_999, out size), size, 19_999, 8));

            using var newTx = Env.ReadTransaction();
            var newTree = newTx.FixedTreeFor(treeId, 8);
            Assert.Null(newTree.ReadPtr(5, out _));
            Assert.True(HasValue(newTree.ReadPtr(30_000, out size), size, 30_000, 8));
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void SeparateTreesInOneTransactionDoNotShareCachedState()
        {
            Slice.From(Allocator, "small", out var small);
            Slice.From(Allocator, "large", out var large);
            Fill(small, 5, 8, 1);
            Fill(large, 40_000, 8, 1);

            using var tx = Env.ReadTransaction();
            var smallTree = tx.FixedTreeFor(small, 8);
            var largeTree = tx.FixedTreeFor(large, 8);

            for (var round = 0; round < 3; round++)
            {
                Assert.True(HasValue(smallTree.ReadPtr(4, out var size), size, 4, 8));
                Assert.Null(smallTree.ReadPtr(5, out _));
                Assert.True(HasValue(largeTree.ReadPtr(39_999, out size), size, 39_999, 8));
                Assert.Null(largeTree.ReadPtr(40_000, out _));
            }
        }
    }
}
