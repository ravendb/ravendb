// -----------------------------------------------------------------------
//  <copyright file="LargeFixedSizeTrees.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections;
using Voron.Debugging;
using Voron.Tests.Util;
using Voron.Util.Conversion;
using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.FixedSize
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
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, bytes);
                    Assert.NotNull(fst.Read(i));
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

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
            var slice = new Slice(bytes);
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, slice);
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);
                var total = 0;
                using (var it = fst.Iterate())
                {
                    Assert.True(it.Seek(long.MinValue));
                    do
                    {
                        Assert.Equal(total++, it.CreateReaderForCurrent().ReadLittleEndianInt64());
                    } while (it.MoveNext());
                }
                Assert.Equal(count, total);
                tx.Commit();
            }
        }

        [Theory]
        [InlineData(8)]
        [InlineData(16)]
        [InlineData(128)]
        [InlineData(1024*256)]
        public void CanRemove_ALot_ForPageSplits(int count)
        {
            var bytes = new byte[48];
            var slice = new Slice(bytes);
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, slice);
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    fst.Delete(i);
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

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
        [InlineData(1024*256)]
        public void CanDeleteRange(int count)
        {
            var bytes = new byte[48];
            var slice = new Slice(bytes);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    Assert.Equal(i - 1, fst.NumberOfEntries);
                    fst.Add(i, slice);
                }

                tx.Commit();
                Assert.Equal(count, fst.NumberOfEntries);
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);
                Assert.Equal(count, fst.NumberOfEntries);
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                var itemsRemoved = fst.DeleteRange(4, count - 3);
                Assert.Equal(count - 6, itemsRemoved.NumberOfEntriesDeleted);
                Assert.Equal(false, itemsRemoved.TreeRemoved);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    if (i >= 4 && i <= count - 3)
                    {
                        Assert.False(fst.Contains(i), i.ToString());
                        Assert.Null(fst.Read(i));
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
            var slice = new Slice(bytes);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, slice);
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                var itemsRemoved = fst.DeleteRange(0, DateTime.MaxValue.Ticks);
                Assert.Equal(count, itemsRemoved.NumberOfEntriesDeleted);
                Assert.Equal(true, itemsRemoved.TreeRemoved);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    Assert.False(fst.Contains(i), i.ToString());
                    Assert.Null(fst.Read(i));
                }
                tx.Commit();
            }
        }

        [Theory]
        [InlineData(250)]
        [InlineData(1000)]
        [InlineData(94000)]
        [InlineData(300000)]
        public void CanDeleteRange_TryToFindABranchNextToLeaf(int count)
        {
            var bytes = new byte[48];
            var slice = new Slice(bytes);
            var status = new BitArray(count + 1);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, slice);
                    status[i] = true;
                }

                tx.Commit();
            }

			var tc = Environment.TickCount;
			var random = new Random(tc);
            // del exactly 1 page
            for (var i = 0; i < count/100; i++)
            {
                var start = Math.Floor(random.Next(count)/(decimal) 72)*72;
                start += 1;
                var end = Math.Min(count, start + 71);

                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);
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

                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);
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

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);
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
        [InlineData(1000)]
        [InlineData(100000)]
        [InlineData(500000)]
        [InlineData(1000000)]
        [InlineData(2000000)]
        public void CanDeleteRange_RandomRanges(int count)
        {
            var bytes = new byte[48];
            var slice = new Slice(bytes);

            var status = new BitArray(count + 1);
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 1; i <= count; i++)
                {
                    EndianBitConverter.Little.CopyBytes(i, bytes, 0);
                    fst.Add(i, slice);
                    status[i] = true;
                }

                tx.Commit();
            }

			var tc = Environment.TickCount;
			var random = new Random(tc);
			for (var i = 0; i < count/100; i++)
            {
                var start = random.Next(count);
                var end = random.Next(start, count);

                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);
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
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);
                for (int i = 0; i <= count; i++)
                {
                    Assert.Equal(status[i], fst.Contains(i));
                }
            }
        }

        [Theory]
        [InlineData(100)]
        [InlineData(10000)]
        [InlineData(75000)]
        [InlineData(300000)]
        public void CanDeleteRange_RandomRanges_WithGaps(int count)
        {
            var bytes = new byte[48];
            var slice = new Slice(bytes);

            var status = new BitArray(count * 3);
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);
                for (var i = 1; i < count; i++)
                {
                    fst.Add(i*3, slice);
                    status[i*3] = true;
                }

                tx.Commit();
            }
            var tc = Environment.TickCount;
            var random = new Random(tc);
            for (var i = 0; i < count/10; i++)
            {
                var start = random.Next(status.Length);
                var end = random.Next(start, status.Length);
                using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);
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
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var fst = tx.State.Root.FixedTreeFor("test", valSize: 48);

                for (int i = 0; i < count; i++)
                {
                    Assert.Equal(status[i * 3], fst.Contains(i * 3));
                }
            }
        }
    }
}