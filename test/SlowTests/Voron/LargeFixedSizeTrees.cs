// -----------------------------------------------------------------------
//  <copyright file="LargeFixedSizeTrees.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections;
using SlowTests.Utils;
using Voron;
using Voron.Util.Conversion;
using Xunit;

namespace SlowTests.Voron
{
    public class LargeFixedSizeTrees : StorageTest
    {
        [Theory]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024*256)]
        public void CanAdd_ALot_ForPageSplits(int count)
        {
            var bytes = new byte[48];
            var treeId = Slice.From(Allocator, "test");

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, bytes);
                    Assert.NotNull(fst.Read(i));
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    Assert.True(fst.Contains(i));
                    var read = fst.Read(i);
                    read.CopyTo(bytes);
                    Assert.Equal(i, EndianBitConverter.Little.ToInt32(bytes, 0));
                }
                tx.Commit();
            }
        }

        [Theory]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024*256)]
        public void CanCIterate_ALot_ForPageSplits(int count)
        {
            var bytes = new byte[48];

            var treeId = Slice.From(Allocator, "test");

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    var slice = Slice.From(Allocator, bytes);
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

        [Theory]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024 * 256)]
        public void CanRemove_ALot_ForPageSplits(int count)
        {
            var bytes = new byte[48];
            var slice = Slice.From(Allocator, bytes);
            var treeId = Slice.From(Allocator, "test");
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

        [Theory]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024 * 256)]
        public void CanDeleteRange(int count)
        {
            var bytes = new byte[48];           
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    Assert.Equal(i - 1, fst.NumberOfEntries);

                    var slice = Slice.From(Allocator, bytes);
                    fst.Add(i, slice);
                }

                tx.Commit();
                Assert.Equal(count, fst.NumberOfEntries);
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
                        Assert.False(fst.Read(i).HasValue);
                    }
                    else
                    {
                        Assert.True(fst.Contains(i), i.ToString());
                        Assert.Equal(i, fst.Read(i).CreateReader().ReadLittleEndianInt64());
                    }
                }
                tx.Commit();
            }
        }

        [Theory]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024*256)]
        public void CanDeleteAllRange(int count)
        {
            var bytes = new byte[48];
           
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    var slice = Slice.From(Allocator, bytes);
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
                    Assert.False(fst.Read(i).HasValue);
                }
                tx.Commit();
            }
        }

        [Theory]
        [InlineDataWithRandomSeed(250)]
        [InlineDataWithRandomSeed(1000)]
        [InlineDataWithRandomSeed(94000)]
        [InlineDataWithRandomSeed(300000)]
        public void CanDeleteRange_TryToFindABranchNextToLeaf(int count, int seed)
        {
            var bytes = new byte[48];
     
            var status = new BitArray(count + 1);
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    var slice = Slice.From(Allocator, bytes);
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
                        Assert.False(true, i.ToString());
                    }
                }
            }
        }

        [Theory]
        [InlineDataWithRandomSeed(1000)]
        [InlineDataWithRandomSeed(100000)]
        [InlineDataWithRandomSeed(500000)]
        [InlineDataWithRandomSeed(1000000)]
        [InlineDataWithRandomSeed(2000000)]
        [InlineData(100000, 1684385375)]// reproduced a bug, do not remove
        public void CanDeleteRange_RandomRanges(int count, int seed)
        {
            var bytes = new byte[48];
            var treeId = Slice.From(Allocator, "test");
            var status = new BitArray(count + 1);
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    var slice = Slice.From(Allocator, bytes);
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

        [Theory]
        [InlineDataWithRandomSeed(100)]
        [InlineDataWithRandomSeed(10000)]
        [InlineDataWithRandomSeed(75000)]
        [InlineDataWithRandomSeed(300000)]
        public void CanDeleteRange_RandomRanges_WithGaps(int count, int seed)
        {
            var bytes = new byte[48];
            var slice = Slice.From(Allocator, bytes);
            var treeId = Slice.From(Allocator, "test");
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

        [Theory]
        [InlineData(8)]
        [InlineData(12)]
        [InlineData(16)]
        [InlineData(100)]
        [InlineData(10000)]
        public void SeekToLast_ShouldWork(int count)
        {
            var bytes = new byte[48];
            var slice = Slice.From(Allocator, bytes);
            var treeId = Slice.From(Allocator, "test");
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
    }
}