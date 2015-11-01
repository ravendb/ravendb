using System;
using System.Collections.Generic;
using System.IO;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Trees
{
    public class Basic : StorageTest
    {

        [PrefixesFact]
        public void CanAddVeryLargeValue()
        {
            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);

            List<long> allPages = null;
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("a", new MemoryStream(buffer));
                allPages = tx.Root.AllPages();
                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Assert.Equal(tx.Root.State.PageCount, allPages.Count);
                Assert.Equal(4, tx.Root.State.PageCount);
                Assert.Equal(3, tx.Root.State.OverflowPages);
            }
        }

        [PrefixesFact]
        public void CanAdd()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("test", StreamFor("value"));
            }
        }

        [PrefixesFact]
        public void CanAddAndRead()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("b", StreamFor("2"));
                tx.Root.Add			("c", StreamFor("3"));
                tx.Root.Add			("a", StreamFor("1"));
                var actual = ReadKey(tx, "a");

                Assert.Equal("a", actual.Item1);
                Assert.Equal("1", actual.Item2);
            }
        }

        [PrefixesFact]
        public void CanAddAndReadStats()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Slice key = "test";
                tx.Root.Add			(key, StreamFor("value"));

                tx.Commit();

                Assert.Equal(1, tx.Root.State.PageCount);
                Assert.Equal(1, tx.Root.State.LeafPages);
            }
        }

        [PrefixesFact]
        public void CanAddEnoughToCausePageSplit()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Stream stream = StreamFor("value");

                for (int i = 0; i < 256; i++)
                {
                    stream.Position = 0;
                    tx.Root.Add			("test-" + i, stream);

                }

                tx.Commit();
                if (AbstractPager.PageSize != 4096)
#pragma warning disable 162
                    return;
#pragma warning restore 162
                Assert.Equal(4, tx.Root.State.PageCount);
                Assert.Equal(3, tx.Root.State.LeafPages);
                Assert.Equal(1, tx.Root.State.BranchPages);
                Assert.Equal(2, tx.Root.State.Depth);

            }
        }

        [PrefixesFact]
        public void AfterPageSplitAllDataIsValid()
        {
            const int count = 256;
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < count; i++)
                {
                    tx.Root.Add			("test-" + i.ToString("000"), StreamFor("val-" + i));

                }

                tx.Commit();
            }
            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                for (int i = 0; i < count; i++)
                {
                    var read = ReadKey(tx, "test-" + i.ToString("000"));
                    Assert.Equal("test-" + i.ToString("000"), read.Item1);
                    Assert.Equal("val-" + i, read.Item2);
                }
            }
        }

        [PrefixesFact]
        public void PageSplitsAllAround()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Stream stream = StreamFor("value");

                for (int i = 0; i < 256; i++)
                {
                    for (int j = 0; j < 5; j++)
                    {
                        stream.Position = 0;
                        if (j == 2 && i == 205)
                        {

                        }
                        tx.Root.Add			("test-" + j.ToString("000") + "-" + i.ToString("000"), stream);
                    }
                }

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                for (int i = 0; i < 256; i++)
                {
                    for (int j = 0; j < 5; j++)
                    {
                        var key = "test-" + j.ToString("000") + "-" + i.ToString("000");
                        var readKey = ReadKey(tx, key);
                        Assert.Equal(readKey.Item1, key);
                    }
                }
            }
        }
    }
}
