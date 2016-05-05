using System;
using System.Collections.Generic;
using System.IO;
using Xunit;
using Voron;
using Voron.Impl;

namespace FastTests.Voron.Trees
{
    public class Basic : StorageTest
    {

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.PageSize = 4 * Constants.Size.Kilobyte;
            base.Configure(options);
        }

        [Fact]
        public void CanAddVeryLargeValue()
        {
            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);

            List<long> allPages = null;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("a", new MemoryStream(buffer));
                allPages = tree.AllPages();
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("foo");
                Assert.Equal(tree.State.PageCount, allPages.Count);
                Assert.Equal(4, tree.State.PageCount);
                Assert.Equal(3, tree.State.OverflowPages);
            }
        }

        [Fact]
        public void CanAdd()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("test", StreamFor("value"));
            }
        }

        [Fact]
        public void CanAddAndRead()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("b", StreamFor("2"));
                tree.Add("c", StreamFor("3"));
                tree.Add("a", StreamFor("1"));
                var actual = tree.Read("a");

                using (var it = tree.Iterate())
                {
                    Assert.True(it.Seek(Slice.From(tx.Allocator, "a")));
                    Assert.Equal("a", it.CurrentKey.ToString());
                }

                Assert.Equal("1", actual.Reader.ToString());
            }
        }

        [Fact]
        public void CanAddAndReadStats()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice key = Slice.From(tx.Allocator, "test");
                var tree = tx.CreateTree("foo");
                tree.Add(key, StreamFor("value"));

                tx.Commit();

                Assert.Equal(1, tree.State.PageCount);
                Assert.Equal(1, tree.State.LeafPages);
            }
        }

        [Fact]
        public void CanAddEnoughToCausePageSplit()
        {
            using (var tx = Env.WriteTransaction())
            {
                Stream stream = StreamFor("value");
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 256; i++)
                {
                    stream.Position = 0;
                    tree.Add("test-" + i, stream);

                }

                tx.Commit();


                Assert.Equal(4, tree.State.PageCount);
                Assert.Equal(3, tree.State.LeafPages);
                Assert.Equal(1, tree.State.BranchPages);
                Assert.Equal(2, tree.State.Depth);

            }
        }

        [Fact]
        public void AfterPageSplitAllDataIsValid()
        {
            const int count = 256;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < count; i++)
                {
                   tree.Add("test-" + i.ToString("000"), StreamFor("val-" + i));

                }

                tx.Commit();
            }
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("foo");
                using (var it = tree.Iterate())
                {
                    for (int i = 0; i < count; i++)
                    {
                        Assert.True(it.Seek(Slice.From(tx.Allocator, "test-" + i.ToString("000"))));
                        Assert.Equal("test-" + i.ToString("000"), it.CurrentKey.ToString());
                        Assert.Equal("val-" + i, it.CreateReaderForCurrent().ToString());
                    }
                }
            }
        }

        [Fact]
        public void PageSplitsAllAround()
        {
            using (var tx = Env.WriteTransaction())
            {
                Stream stream = StreamFor("value");
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 256; i++)
                {
                    for (int j = 0; j < 5; j++)
                    {
                        stream.Position = 0;
                       
                        tree.Add("test-" + j.ToString("000") + "-" + i.ToString("000"), stream);
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("foo");
                for (int i = 0; i < 256; i++)
                {
                    for (int j = 0; j < 5; j++)
                    {
                        var key = "test-" + j.ToString("000") + "-" + i.ToString("000");

                        Assert.True(tree.Read(key) != null);
                    }
                }
            }
        }
    }
}
