using System;
using System.Collections;
using System.Diagnostics;
using FastTests.Voron;
using SlowTests.Utils;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Fixed;
using Voron.Util.Conversion;
using Xunit;

namespace SlowTests.Voron
{
    public class LargeFixedSizeTrees : StorageTest
    {
        public LargeFixedSizeTrees(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024*256)]
        public void CanAdd_ALot_ForPageSplits(int count)
        {
            var bytes = new byte[48];
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, bytes);
                    Slice read;
                    using (fst.Read(i, out read))
                    {
                        Assert.True(read.HasValue);
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    Assert.True(fst.Contains(i));
                    Slice read;
                    using (fst.Read(i, out read))
                    {
                        read.CopyTo(bytes);
                        Assert.Equal(i, EndianBitConverter.Little.ToInt32(bytes, 0));
                    }
                }
                tx.Commit();
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024*256)]
        public void CanCIterate_ALot_ForPageSplits(int count)
        {
            var bytes = new byte[48];

            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    Slice slice;
                    Slice.From(Allocator, bytes, out slice);
                    fst.Add(i, slice);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);
                var total = 0;
                using (var it = fst.Iterate())
                {
                    Assert.True(it.Seek(long.MinValue));
                    do
                    {
                        Assert.Equal(total++, it.CreateReaderForCurrent().ReadLittleEndianInt64());
                    }
                    while (it.MoveNext());
                }
                Assert.Equal(count, total);
                tx.Commit();
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024 * 256)]
        public void CanRemove_ALot_ForPageSplits(int count)
        {
            var bytes = new byte[48];
            Slice slice;
            Slice.From(Allocator, bytes, out slice);
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, slice);
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    fst.Delete(i);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    Assert.False(fst.Contains(i), i.ToString());
                }
                tx.Commit();
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024 * 256)]
        public void CanDeleteRange(int count)
        {
            var bytes = new byte[48];
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    Assert.Equal(i - 1, fst.NumberOfEntries);

                    Slice slice;
                    Slice.From(Allocator, bytes, out slice);
                    fst.Add(i, slice);
                }

                Assert.Equal(count, fst.NumberOfEntries);
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);
                Assert.Equal(count, fst.NumberOfEntries);
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                var itemsRemoved = fst.DeleteRange(4, count - 3);
                Assert.Equal(count - 6, itemsRemoved.NumberOfEntriesDeleted);
                Assert.Equal(false, itemsRemoved.TreeRemoved);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    if (i >= 4 && i <= count - 3)
                    {
                        Assert.False(fst.Contains(i), i.ToString());
                        Slice read;
                        using (fst.Read(i, out read))
                        {
                            Assert.False(read.HasValue);
                        }
                    }
                    else
                    {
                        Assert.True(fst.Contains(i), i.ToString());
                        Slice read;
                        using (fst.Read(i, out read))
                        {
                            Assert.Equal(i, read.CreateReader().ReadLittleEndianInt64());
                        }
                    }
                }
                tx.Commit();
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024*256)]
        public void CanDeleteAllRange(int count)
        {
            var bytes = new byte[48];

            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    Slice slice;
                    Slice.From(Allocator, bytes, out slice);
                    fst.Add(i, slice);
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                var itemsRemoved = fst.DeleteRange(0, DateTime.MaxValue.Ticks);
                Assert.Equal(count, itemsRemoved.NumberOfEntriesDeleted);
                Assert.Equal(true, itemsRemoved.TreeRemoved);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    Assert.False(fst.Contains(i), i.ToString());
                    Slice read;
                    using (fst.Read(i, out read))
                    {
                        Assert.False(read.HasValue);
                    }
                }
                tx.Commit();
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed(250)]
        [InlineDataWithRandomSeed(1000)]
        public void CanDeleteRange_TryToFindABranchNextToLeaf(int count, int seed)
        {
            var bytes = new byte[48];
     
            var status = new BitArray(count + 1);
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    Slice slice;
                    Slice.From(Allocator, bytes, out slice);
                    fst.Add(i, slice);

                    status[i] = true;
                }

                tx.Commit();
            }

            var random = new Random(seed);
            // del exactly 1 page
            for (var i = 0; i < count/100; i++)
            {
                var start = Math.Floor(random.Next(count)/(decimal) 72)*72;
                start += 1;
                var end = Math.Min(count, start + 71);

                using (var tx = Env.WriteTransaction())
                {
                    var fst = tx.FixedTreeFor(treeId, valSize: 48);
                    for (int j = (int) start; j <= (int) end; j++)
                    {
                        status[j] = false;
                    }
                    fst.DeleteRange((long) start, (long) end);

                    tx.Commit();
                }
            }

            // random size
            for (var i = 0; i < count; i++)
            {
                var start = random.Next(count);
                var end = random.Next(start, count);

                using (var tx = Env.WriteTransaction())
                {
                    var fst = tx.FixedTreeFor(treeId, valSize: 48);
                    if (fst.NumberOfEntries == 0)
                        break;
                    for (int j = start; j <= end; j++)
                    {
                        status.Set(j, false);
                    }
                    fst.DeleteRange(start, end);

                    tx.Commit();
                }
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);
                for (int i = 0; i < count; i++)
                {
                    if (status[i] != fst.Contains(i))
                    {
                        fst.DebugRenderAndShow();
                        Assert.Fail(i.ToString());
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed(1000)]
        [InlineDataWithRandomSeed(100000)]
        [InlineDataWithRandomSeed(500000)]
        [InlineData(100000, 1684385375)]// reproduced a bug, do not remove
        public void CanDeleteRange_RandomRanges(int count, int seed)
        {
            var bytes = new byte[48];
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            var status = new BitArray(count + 1);
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    Slice slice;
                    Slice.From(Allocator, bytes, out slice);
                    fst.Add(i, slice);
                    status[i] = true;
                }

                tx.Commit();
            }

            var random = new Random(seed);
            for (var i = 0; i < count/100; i++)
            {
                var start = random.Next(count);
                var end = random.Next(start, count);

                using (var tx = Env.WriteTransaction())
                {
                    var fst = tx.FixedTreeFor(treeId, valSize: 48);
                    if (fst.NumberOfEntries == 0)
                        break;
                    for (int j = start; j <= end; j++)
                    {
                        status[j] = false;
                    }
                    fst.DeleteRange(start, end);

                    tx.Commit();
                }
            }
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);
                for (int i = 0; i <= count; i++)
                {
                    Assert.Equal(status[i], fst.Contains(i));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed(100)]
        [InlineDataWithRandomSeed(10000)]
        [InlineDataWithRandomSeed(75000)]
        public void CanDeleteRange_RandomRanges_WithGaps(int count, int seed)
        {
            var bytes = new byte[48];
            Slice slice;
            Slice.From(Allocator, bytes, out slice);
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            var status = new BitArray(count * 3);
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);
                for (var i = 1; i < count; i++)
                {
                    fst.Add(i*3, slice);
                    status[i*3] = true;
                }

                tx.Commit();
            }
            var random = new Random(seed);
            for (var i = 0; i < count/10; i++)
            {
                var start = random.Next(status.Length);
                var end = random.Next(start, status.Length);
                using (var tx = Env.WriteTransaction())
                {
                    var fst = tx.FixedTreeFor(treeId, valSize: 48);
                    if (fst.NumberOfEntries == 0)
                        break;
                    for (int j = start; j <= end; j++)
                    {
                        status[j] = false;
                    }
                    fst.DeleteRange(start, end);

                    tx.Commit();
                }
            }
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    Assert.Equal(status[i * 3], fst.Contains(i * 3));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(8)]
        [InlineData(12)]
        [InlineData(16)]
        [InlineData(100)]
        [InlineData(10000)]
        public void SeekToLast_ShouldWork(int count)
        {
            var bytes = new byte[48];
            Slice slice;
            Slice.From(Allocator, bytes, out slice);
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            int lastId = -1;
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);
                for (var i = 1; i < count; i++)
                {
                    fst.Add(i, slice);
                    lastId = i;
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);
                using (var it = fst.Iterate())
                {
                    Assert.True(it.SeekToLast(), "Failed to seek to last");
                    Assert.Equal(lastId, it.CurrentKey);
                }
            }
        }

        #region GetNumberOfEntriesAfter Tests

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(10, EstimationAccuracy.Exact)]
        [InlineData(10, EstimationAccuracy.Estimated)]
        [InlineData(100, EstimationAccuracy.Exact)]
        [InlineData(100, EstimationAccuracy.Estimated)]
        [InlineData(1000, EstimationAccuracy.Exact)]
        [InlineData(1000, EstimationAccuracy.Estimated)]
        public void GetNumberOfEntriesAfter_SmallTree_WithAndWithoutEstimate(int count, EstimationAccuracy accuracy)
        {
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));

                for (int i = 0; i < count; i++)
                {
                    fst.Add(i, new byte[sizeof(long)]);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));

                for (int i = 0; i < count; i++)
                {
                    var actualCount = fst.GetNumberOfEntriesAfter(i, out long totalCount, Stopwatch.StartNew(), accuracy);
                    var expectedCount = CalculateExpectedEntriesAfter(fst, i, out long expectedTotalCount);

                    Assert.Equal(count, expectedTotalCount);
                    Assert.Equal(expectedTotalCount, totalCount);
                    Assert.Equal(expectedCount, actualCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(10000, EstimationAccuracy.Exact)]
        [InlineData(10000, EstimationAccuracy.Estimated)]
        [InlineData(50000, EstimationAccuracy.Exact)]
        [InlineData(50000, EstimationAccuracy.Estimated)]
        [InlineData(100000, EstimationAccuracy.Exact)]
        [InlineData(100000, EstimationAccuracy.Estimated)]
        public void GetNumberOfEntriesAfter_LargeTree_WithAndWithoutEstimate(int count, EstimationAccuracy accuracy)
        {
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));

                for (int i = 0; i < count; i++)
                {
                    fst.Add(i, new byte[sizeof(long)]);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));

                // Test at specific positions to reduce test time for large trees
                int[] testPositions = { 0, count / 4, count / 2, 3 * count / 4, count - 1, count };

                foreach (var position in testPositions)
                {
                    var actualCount = fst.GetNumberOfEntriesAfter(position, out long totalCount, Stopwatch.StartNew(), accuracy);
                    var expectedCount = CalculateExpectedEntriesAfter(fst, position, out long expectedTotalCount);

                    Assert.Equal(count, expectedTotalCount);
                    Assert.Equal(expectedTotalCount, totalCount);
                    Assert.Equal(expectedCount, actualCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(13, 2, EstimationAccuracy.Exact)]
        [InlineData(13, 2, EstimationAccuracy.Estimated)]
        [InlineData(5111, 2, EstimationAccuracy.Exact)]
        [InlineData(5111, 2, EstimationAccuracy.Estimated)]
        [InlineData(5111, 5, EstimationAccuracy.Exact)]
        [InlineData(5111, 5, EstimationAccuracy.Estimated)]
        public void GetNumberOfEntriesAfter_WithGaps_WithAndWithoutEstimate(int total, int mod, EstimationAccuracy accuracy)
        {
            // Insert entries with gaps (only entries where i % mod == 0)
            DoWorkWithGaps(total, mod, modZero: true, accuracy);
            // Insert entries with gaps (only entries where i % mod != 0)
            DoWorkWithGaps(total, mod, modZero: false, accuracy);
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed]
        public void GetNumberOfEntriesAfter_Random_WithoutEstimate(int seed)
        {
            var random = new Random(seed);
            var total = random.Next(10, 10_000);
            var mod = random.Next(2, 100);

            DoWorkWithGaps(total, mod, modZero: true, EstimationAccuracy.Exact);
            DoWorkWithGaps(total, mod, modZero: false, EstimationAccuracy.Exact);
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed]
        public void GetNumberOfEntriesAfter_Random_WithEstimate(int seed)
        {
            var random = new Random(seed);
            var total = random.Next(10, 10_000);
            var mod = random.Next(2, 100);

            DoWorkWithGaps(total, mod, modZero: true, EstimationAccuracy.Estimated);
            DoWorkWithGaps(total, mod, modZero: false, EstimationAccuracy.Estimated);
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(0, EstimationAccuracy.Exact)]
        [InlineData(0, EstimationAccuracy.Estimated)]
        public void GetNumberOfEntriesAfter_EmptyTree_ReturnsZero(int count, EstimationAccuracy accuracy)
        {
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));
                // Don't add any entries

                var actualCount = fst.GetNumberOfEntriesAfter(0, out long totalCount, Stopwatch.StartNew(), accuracy);

                Assert.Equal(0, totalCount);
                Assert.Equal(0, actualCount);
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1, EstimationAccuracy.Exact)]
        [InlineData(1, EstimationAccuracy.Estimated)]
        public void GetNumberOfEntriesAfter_SingleEntry_WithAndWithoutEstimate(int count, EstimationAccuracy accuracy)
        {
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));
                fst.Add(100, new byte[sizeof(long)]);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));

                // Query before the entry
                var beforeCount = fst.GetNumberOfEntriesAfter(50, out long totalCount1, Stopwatch.StartNew(), accuracy);
                Assert.Equal(1, totalCount1);
                Assert.Equal(1, beforeCount);

                // Query at the entry
                var atCount = fst.GetNumberOfEntriesAfter(100, out long totalCount2, Stopwatch.StartNew(), accuracy);
                Assert.Equal(1, totalCount2);
                Assert.Equal(0, atCount);

                // Query after the entry
                var afterCount = fst.GetNumberOfEntriesAfter(150, out long totalCount3, Stopwatch.StartNew(), accuracy);
                Assert.Equal(1, totalCount3);
                Assert.Equal(0, afterCount);
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1000, EstimationAccuracy.Exact)]
        [InlineData(1000, EstimationAccuracy.Estimated)]
        [InlineData(10000, EstimationAccuracy.Exact)]
        [InlineData(10000, EstimationAccuracy.Estimated)]
        public void GetNumberOfEntriesAfter_QueryBeyondLastEntry_ReturnsZero(int count, EstimationAccuracy accuracy)
        {
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));

                for (int i = 0; i < count; i++)
                {
                    fst.Add(i, new byte[sizeof(long)]);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));

                // Query way beyond the last entry
                var actualCount = fst.GetNumberOfEntriesAfter(count + 1000, out long totalCount, Stopwatch.StartNew(), accuracy);

                Assert.Equal(count, totalCount);
                Assert.Equal(0, actualCount);
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1000, EstimationAccuracy.Exact)]
        [InlineData(1000, EstimationAccuracy.Estimated)]
        [InlineData(10000, EstimationAccuracy.Exact)]
        [InlineData(10000, EstimationAccuracy.Estimated)]
        public void GetNumberOfEntriesAfter_QueryBeforeFirstEntry_ReturnsAll(int count, EstimationAccuracy accuracy)
        {
            Slice treeId;
            Slice.From(Allocator, "test", out treeId);

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));

                // Start entries from 100 so we can query before them
                for (int i = 0; i < count; i++)
                {
                    fst.Add(i + 100, new byte[sizeof(long)]);
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: sizeof(long));

                // Query before all entries
                var actualCount = fst.GetNumberOfEntriesAfter(0, out long totalCount, Stopwatch.StartNew(), accuracy);

                Assert.Equal(count, totalCount);
                Assert.Equal(count, actualCount);
            }
        }

        private void DoWorkWithGaps(int total, int mod, bool modZero, EstimationAccuracy accuracy)
        {
            var inserted = 0;
            var treeName = Guid.NewGuid().ToString("N");

            using (var txw = Env.WriteTransaction())
            {
                Slice treeNameSlice;
                Slice.From(Allocator, treeName, ByteStringType.Immutable, out treeNameSlice);

                var tree = txw.GetGlobalFixedSizeTree(treeNameSlice, sizeof(long));

                for (var i = 0; i < total; i++)
                {
                    var modResult = i % mod;

                    if (modZero && modResult == 0)
                        continue;

                    if (modZero == false && modResult != 0)
                        continue;

                    tree.Add(i, new byte[sizeof(long)]);
                    inserted++;
                }

                txw.Commit();
            }

            using (var txr = Env.ReadTransaction())
            {
                var tree = txr.FixedTreeFor(treeName, sizeof(long));

                for (var i = 0; i < total; i++)
                {
                    var count = tree.GetNumberOfEntriesAfter(i, out long totalCount, Stopwatch.StartNew(), accuracy);
                    var expectedCount = CalculateExpectedEntriesAfter(tree, i, out long expectedTotalCount);

                    Assert.Equal(inserted, expectedTotalCount);
                    Assert.Equal(expectedTotalCount, totalCount);
                    Assert.Equal(expectedCount, count);
                }
            }
        }

        private static long CalculateExpectedEntriesAfter(FixedSizeTree fst, long afterValue, out long totalCount)
        {
            totalCount = fst.NumberOfEntries;
            if (totalCount == 0)
                return 0;

            long count = 0;
            using (var it = fst.Iterate())
            {
                if (it.Seek(afterValue) == false)
                    return 0;

                do
                {
                    if (it.CurrentKey == afterValue)
                        continue;

                    count++;
                } while (it.MoveNext());
            }

            return count;
        }

        #endregion
    }
}
