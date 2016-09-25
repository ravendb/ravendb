// -----------------------------------------------------------------------
//  <copyright file="SimpleFixedSizeTrees.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Xunit;
using Voron;

namespace FastTests.Voron.FixedSize
{
    public class SimpleFixedSizeTrees : StorageTest
    {
        [Fact]
        public void TimeSeries()
        {
            var watchId = Slice.From(Allocator, "watches/12831-12345");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(watchId, valSize: 8);

                fst.Add(DateTime.Today.AddHours(8).Ticks, Slice.From(Allocator, BitConverter.GetBytes(80D)));
                fst.Add(DateTime.Today.AddHours(9).Ticks, Slice.From(Allocator, BitConverter.GetBytes(65D)));
                fst.Add(DateTime.Today.AddHours(10).Ticks, Slice.From(Allocator, BitConverter.GetBytes(44D)));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(watchId, valSize: 8);

                var it = fst.Iterate();
                Assert.True(it.Seek(DateTime.Today.AddHours(7).Ticks));
                var buffer = new byte[8];
                Slice val;
                using (it.Value(out val))
                    val.CopyTo(buffer);
                Assert.Equal(80D, BitConverter.ToDouble(buffer, 0));
                Assert.True(it.MoveNext());
                using (it.Value(out val))
                    val.CopyTo(buffer);
                Assert.Equal(65D, BitConverter.ToDouble(buffer, 0));
                Assert.True(it.MoveNext());
                using (it.Value(out val))
                    val.CopyTo(buffer);
                Assert.Equal(44d, BitConverter.ToDouble(buffer, 0));
                Assert.False(it.MoveNext());

                tx.Commit();
            }
        }

        [Fact]
        public void CanAdd()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 0);

                fst.Add(1);
                fst.Add(2);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 0);

                Assert.True(fst.Contains(1));
                Assert.True(fst.Contains(2));
                Assert.False(fst.Contains(3));
                tx.Commit();
            }
        }

        [Fact]
        public void SeekShouldGiveTheNextKey()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 0);

                fst.Add(635634432000000000);
                fst.Add(635634468000000000);
                fst.Add(635634504000000000);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var it = tx.FixedTreeFor(treeId, 0).Iterate();

                Assert.True(it.Seek(635634432000000000));
                Assert.Equal(635634432000000000, it.CurrentKey);
                Assert.True(it.Seek(635634468000000000));
                Assert.Equal(635634468000000000, it.CurrentKey);
                Assert.True(it.Seek(635634504000000000));
                Assert.Equal(635634504000000000, it.CurrentKey);
                Assert.False(it.Seek(635634504000000001));
                tx.Commit();
            }
        }

        [Fact]
        public void CanAdd_Mixed()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 0);

                fst.Add(2);
                fst.Add(6);
                fst.Add(1);
                fst.Add(3);
                fst.Add(-3);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 0);

                Assert.True(fst.Contains(1));
                Assert.True(fst.Contains(2));
                Assert.False(fst.Contains(5));
                Assert.True(fst.Contains(6));
                Assert.False(fst.Contains(4));
                Assert.True(fst.Contains(-3));
                Assert.True(fst.Contains(3));
                tx.Commit();
            }
        }

        [Fact]
        public void CanIterate()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 0);

                fst.Add(3);
                fst.Add(1);
                fst.Add(2);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 0);

                var it = fst.Iterate();
                Assert.True(it.Seek(long.MinValue));
                Assert.Equal(1L, it.CurrentKey);
                Assert.True(it.MoveNext());
                Assert.Equal(2L, it.CurrentKey);
                Assert.True(it.MoveNext());
                Assert.Equal(3L, it.CurrentKey);
                Assert.False(it.MoveNext());


                tx.Commit();
            }
        }


        [Fact]
        public void CanRemove()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 0);

                fst.Add(1);
                fst.Add(2);
                fst.Add(3);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 0);

                fst.Delete(2);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 0);

                Assert.True(fst.Contains(1));
                Assert.False(fst.Contains(2));
                Assert.True(fst.Contains(3));
                tx.Commit();
            }
        }

        [Fact]
        public void CanDeleteRange()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                for (int i = 1; i <= 10; i++)
                {
                    fst.Add(i, Slice.From(Allocator, BitConverter.GetBytes(i + 10L)));
                }
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                var itemsRemoved = fst.DeleteRange(2, 5);
                Assert.Equal(4, itemsRemoved.NumberOfEntriesDeleted);
                Assert.Equal(false, itemsRemoved.TreeRemoved);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Slice read;
                var fst = tx.FixedTreeFor(treeId, 8);

                for (int i = 1; i <= 10; i++)
                {
                    if (i >= 2 && i <= 5)
                    {
                        Assert.False(fst.Contains(i), i.ToString());
                        using (fst.Read(i, out read))
                            Assert.False(read.HasValue);
                    }
                    else
                    {
                        Assert.True(fst.Contains(i), i.ToString());
                        using (fst.Read(i, out read))
                            Assert.Equal(i + 10L, read.CreateReader().ReadLittleEndianInt64());
                    }
                }
                tx.Commit();
            }
        }

        [Fact]
        public void CanDeleteRangeWithGaps()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                for (int i = 1; i <= 10; i++)
                {
                    fst.Add(i, Slice.From(Allocator, BitConverter.GetBytes(i + 10L)));
                }
                for (int i = 30; i <= 40; i++)
                {
                    fst.Add(i, Slice.From(Allocator, BitConverter.GetBytes(i + 10L)));
                }
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                var itemsRemoved = fst.DeleteRange(2, 35);
                Assert.Equal(15, itemsRemoved.NumberOfEntriesDeleted);
                Assert.Equal(false, itemsRemoved.TreeRemoved);

                tx.Commit();
            }
            Slice read;
            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                for (int i = 1; i <= 10; i++)
                {
                    if (i >= 2)
                    {
                        Assert.False(fst.Contains(i), i.ToString());
                        using (fst.Read(i, out read))
                            Assert.False(read.HasValue);
                    }
                    else
                    {
                        Assert.True(fst.Contains(i), i.ToString());
                        using (fst.Read(i, out read))
                            Assert.Equal(i + 10L, read.CreateReader().ReadLittleEndianInt64());
                    }
                }
                for (int i = 30; i <= 40; i++)
                {
                    if (i <= 35)
                    {
                        Assert.False(fst.Contains(i), i.ToString());
                        using (fst.Read(i, out read))
                            Assert.False(read.HasValue);
                    }
                    else
                    {
                        Assert.True(fst.Contains(i), i.ToString());
                        using (fst.Read(i, out read))
                            Assert.Equal(i + 10L, read.CreateReader().ReadLittleEndianInt64());
                    }
                }
                tx.Commit();
            }
        }

        [Fact]
        public void CanDeleteAllRange()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                for (int i = 1; i <= 10; i++)
                {
                    fst.Add(i, Slice.From(Allocator, BitConverter.GetBytes(i + 10L)));
                }
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                var itemsRemoved = fst.DeleteRange(0, DateTime.MaxValue.Ticks);
                Assert.Equal(10, itemsRemoved.NumberOfEntriesDeleted);
                Assert.Equal(true, itemsRemoved.TreeRemoved);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);
                for (int i = 1; i <= 10; i++)
                {
                    Assert.False(fst.Contains(i), i.ToString());
                    Slice read;
                    using (fst.Read(i, out read))
                        Assert.False(read.HasValue);
                }
                tx.Commit();
            }
        }

        [Fact]
        public void CanAdd_WithValue()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                fst.Add(1, Slice.From(Allocator, BitConverter.GetBytes(1L)));
                fst.Add(2, Slice.From(Allocator, BitConverter.GetBytes(2L)));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                Slice read;

                using (fst.Read(1, out read))
                    Assert.Equal(1L, read.CreateReader().ReadLittleEndianInt64());

                using (fst.Read(2, out read))
                    Assert.Equal(2L, read.CreateReader().ReadLittleEndianInt64());
                using (fst.Read(3, out read))
                    Assert.False(read.HasValue);
                tx.Commit();
            }
        }

        [Fact]
        public void CanRemove_WithValue()
        {
            var treeId = Slice.From(Allocator, "test");
            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                fst.Add(1, Slice.From(Allocator, BitConverter.GetBytes(1L)));
                fst.Add(2, Slice.From(Allocator, BitConverter.GetBytes(2L)));
                fst.Add(3, Slice.From(Allocator, BitConverter.GetBytes(3L)));

                tx.Commit();
            }


            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                fst.Delete(2);

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, 8);

                Slice read;
                using (fst.Read(1, out read))
                    Assert.Equal(1L, read.CreateReader().ReadLittleEndianInt64());
                using (fst.Read(2, out read))
                    Assert.False(read.HasValue);
                using (fst.Read(3, out read))
                    Assert.Equal(3L, read.CreateReader().ReadLittleEndianInt64());
                tx.Commit();
            }
        }
    }
}