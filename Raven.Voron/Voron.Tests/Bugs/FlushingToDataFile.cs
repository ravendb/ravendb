// -----------------------------------------------------------------------
//  <copyright file="FlushingToDataFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Voron.Tests.Bugs
{
    using Sparrow.Platform;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Voron.Impl;
    using Voron.Impl.Paging;
    using Voron.Trees;
    using Xunit;

    public class FlushingToDataFile : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.MaxLogFileSize = 2 * AbstractPager.PageSize;
        }

        [PrefixesFact]
        public unsafe void ReadTransactionShouldNotReadFromJournalSnapshotIfJournalWasFlushedInTheMeanwhile()
        {
            var value1 = new byte[4000];

            new Random().NextBytes(value1);

            Assert.Equal(2 * AbstractPager.PageSize, Env.Options.MaxLogFileSize);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("foo/0", new MemoryStream(value1));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("foo/1", new MemoryStream(value1));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Env.FlushLogToDataFile(); // force flushing during read transaction

                using (var txw = Env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    // empty transaction is enough to expose the issue because it allocates 1 page in the scratch space for the transaction header
                    txw.Commit();
                }

                for (var i = 0; i < 2; i++)
                {
                    var readResult = tx.Root.Read("foo/" + i);

                    Assert.NotNull(readResult);
                    Assert.Equal(value1.Length, readResult.Reader.Length);

                    var memoryStream = new MemoryStream(readResult.Reader.Length);
                    readResult.Reader.CopyTo(memoryStream);

                    fixed (byte* b = value1)
                    fixed (byte* c = memoryStream.GetBuffer())
                        Assert.Equal(0, UnmanagedMemory.Compare(b, c, value1.Length));
                }
            }
        }

        [PrefixesFact]
        public void FlushingOperationShouldHaveOwnScratchPagerStateReference()
        {
            var value1 = new byte[4000];

            new Random().NextBytes(value1);

            Assert.Equal(2 * AbstractPager.PageSize, Env.Options.MaxLogFileSize);

            Env.FlushLogToDataFile();

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("foo/0", new MemoryStream(value1));
                tx.Root.Add			("foo/1", new MemoryStream(value1));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("foo/0", new MemoryStream(value1));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("foo/4", new MemoryStream(value1));
                tx.Commit();
            }


            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var readResult = tx.Root.Read("foo/0");

                Assert.NotNull(readResult);
                Assert.Equal(value1.Length, readResult.Reader.Length);

                var memoryStream = new MemoryStream();
                readResult.Reader.CopyTo(memoryStream);
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Env.FlushLogToDataFile();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var readResult = tx.Root.Read("foo/0");

                Assert.NotNull(readResult);
                Assert.Equal(value1.Length, readResult.Reader.Length);

                var memoryStream = new MemoryStream();
                readResult.Reader.CopyTo(memoryStream);
            }
        }

        [PrefixesFact]
        public void OldestActiveTransactionShouldBeCalculatedProperly()
        {
            using (var options = StorageEnvironmentOptions.CreateMemoryOnly())
            {
                options.ManualFlushing = true;
                using (var env = new StorageEnvironment(options))
                {
                    var trees = CreateTrees(env, 1, "tree");
                    var transactions = new List<Transaction>();

                    for (int a = 0; a < 100; a++)
                    {
                        var random = new Random(1337);
                        var buffer = new byte[random.Next(100, 1000)];
                        random.NextBytes(buffer);

                        using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                        {
                            for (int i = 0; i < 100; i++)
                            {
                                foreach (var tree in trees)
                                {
                                    tx.Environment.CreateTree(tx, tree).Add(string.Format("key/{0}/{1}", a, i), new MemoryStream(buffer));
                                }

                            }

                            tx.Commit();
                            env.FlushLogToDataFile(tx);
                            var txr = env.NewTransaction(TransactionFlags.Read);

                            transactions.Add(txr);
                        }
                    }

                    Assert.Equal(transactions.OrderBy(x => x.Id).First().Id, env.OldestTransaction);

                    foreach (var tx in transactions)
                    {
                        foreach (var tree in trees)
                        {
                            using (var iterator = tx.Environment.CreateTree(tx, tree).Iterate())
                            {
                                if (!iterator.Seek(Slice.BeforeAllKeys))
                                    continue;

                                do
                                {
                                    Assert.Contains("key/", iterator.CurrentKey.ToString());
                                } while (iterator.MoveNext());
                            }
                        }
                    }
                }
            }
        }
    }
}
