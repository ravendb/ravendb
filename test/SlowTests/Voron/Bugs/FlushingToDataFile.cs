// -----------------------------------------------------------------------
//  <copyright file="FlushingToDataFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Sparrow;
using Voron;
using Voron.Global;
using Voron.Impl;
using Xunit;

namespace SlowTests.Voron.Bugs
{
    public class FlushingToDataFile : FastTests.Voron.StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.MaxLogFileSize = 2 * Constants.Storage.PageSize;
        }

        [Fact]
        public unsafe void ReadTransactionShouldNotReadFromJournalSnapshotIfJournalWasFlushedInTheMeanwhile()
        {
            var value1 = new byte[4000];

            new Random().NextBytes(value1);

            Assert.Equal(2 * Constants.Storage.PageSize, Env.Options.MaxLogFileSize);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("foo/0", new MemoryStream(value1));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("foo/1", new MemoryStream(value1));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Env.FlushLogToDataFile(); // force flushing during read transaction

                using (var txw = Env.WriteTransaction())
                {
                    // empty transaction is enough to expose the issue because it allocates 1 page in the scratch space for the transaction header
                    txw.Commit();
                }

                var tree = tx.CreateTree("foo");
                for (var i = 0; i < 2; i++)
                {
                    var readResult = tree.Read("foo/" + i);

                    Assert.NotNull(readResult);
                    Assert.Equal(value1.Length, readResult.Reader.Length);

                    var memoryStream = new MemoryStream(readResult.Reader.Length);
                    readResult.Reader.CopyTo(memoryStream);

                    fixed (byte* b = value1)
                    fixed (byte* c = memoryStream.ToArray())
                        Assert.Equal(0, Memory.Compare(b, c, value1.Length));
                }
            }
        }

        [Fact]
        public void FlushingOperationShouldHaveOwnScratchPagerStateReference()
        {
            var value1 = new byte[4000];

            new Random().NextBytes(value1);

            Assert.Equal(2 * Constants.Storage.PageSize, Env.Options.MaxLogFileSize);

            Env.FlushLogToDataFile();

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("foo/0", new MemoryStream(value1));
                tree.Add("foo/1", new MemoryStream(value1));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("foo/0", new MemoryStream(value1));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("foo/4", new MemoryStream(value1));
                tx.Commit();
            }


            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                var readResult = tree.Read("foo/0");

                Assert.NotNull(readResult);
                Assert.Equal(value1.Length, readResult.Reader.Length);

                var memoryStream = new MemoryStream();
                readResult.Reader.CopyTo(memoryStream);
            }

            using (var tx = Env.ReadTransaction())
            {
                Env.FlushLogToDataFile();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                var readResult = tree.Read("foo/0");

                Assert.NotNull(readResult);
                Assert.Equal(value1.Length, readResult.Reader.Length);

                var memoryStream = new MemoryStream();
                readResult.Reader.CopyTo(memoryStream);
            }
        }

        [Fact]
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

                        using (var tx = env.WriteTransaction())
                        {
                            for (int i = 0; i < 100; i++)
                            {
                                foreach (var tree in trees)
                                {
                                    tx.CreateTree(tree).Add(string.Format("key/{0}/{1}", a, i), new MemoryStream(buffer));
                                }

                            }

                            var txr = env.ReadTransaction();
                            transactions.Add(txr);
                            tx.Commit();
                        }
                        env.FlushLogToDataFile();

                    }

                    Assert.Equal(transactions.OrderBy(x => x.LowLevelTransaction.Id).First().LowLevelTransaction.Id, env.ActiveTransactions.OldestTransaction);

                    foreach (var tx in transactions)
                    {
                        foreach (var tree in trees)
                        {
                            using (var iterator = tx.CreateTree(tree).Iterate(false))
                            {
                                if (!iterator.Seek(Slices.BeforeAllKeys))
                                    continue;

                                do
                                {
                                    Assert.Contains("key/", iterator.CurrentKey.ToString());
                                } while (iterator.MoveNext());
                            }
                        }
                    }

                    foreach (var transaction in transactions)
                    {
                        transaction.Dispose();
                    }
                }
            }
        }
    }
}
