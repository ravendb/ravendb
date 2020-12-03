using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron
{
    public class Recovery : StorageTest
    {
        public Recovery(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void StorageRecoveryShouldWorkWhenThereAreNoTransactionsToRecoverFromLog()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
            }
        }

        [Fact]
        public void StorageRecoveryShouldWorkWhenThereSingleTransactionToRecoverFromLog()
        {

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree(  "tree");

                    for (var i = 0; i < 100; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree(  "tree");

                    tx.Commit();
                }


                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.CreateTree("tree");

                    for (var i = 0; i < 100; i++)
                    {
                        Assert.NotNull(tree.Read("key" + i));
                    }
                }
            }
        }

        [Fact]
        public void StorageRecoveryShouldWorkWhenThereAreCommitedAndUncommitedTransactions()
        {

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree(  "tree");

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    for (var i = 0; i < 10000; i++)
                    {
                        tx.CreateTree("tree").Add("a" + i, new MemoryStream());
                    }
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
            }
        }

        [Fact]
        public void StorageRecoveryShouldWorkWhenThereAreCommitedAndUncommitedTransactions2()
        {

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("atree");
                    tx.CreateTree("btree");

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    for (var i = 0; i < 10000; i++)
                    {
                        tx.CreateTree("atree").Add("a" + i, new MemoryStream());
                        tx.CreateTree("btree").MultiAdd("a" + i, "a" + i);
                    }
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
            }
        }

        [Fact]
        public void StorageRecoveryShouldWorkWhenThereAreMultipleCommitedTransactions()
        {

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree( "atree");

                    for (var i = 0; i < 1000; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree( "btree");

                    for (var i = 0; i < 1; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("atree");
                    tx.CreateTree("btree");

                    tx.Commit();
                }

                using (var tx = env.ReadTransaction())
                {
                    var aTree = tx.CreateTree("atree");
                    var bTree = tx.CreateTree("btree");

                    for (var i = 0; i < 1000; i++)
                    {
                        Assert.NotNull(aTree.Read("key" + i));
                    }

                    for (var i = 0; i < 1; i++)
                    {
                        Assert.NotNull(bTree.Read("key" + i));
                    }
                }
            }

        }

        [Fact]
        public void StorageRecoveryShouldWorkWhenThereAreMultipleCommitedTransactions2()
        {

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree( "atree");

                    for (var i = 0; i < 1000; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("btree");

                    for (var i = 0; i < 5; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree( "atree");
                    tx.CreateTree( "btree");

                    tx.Commit();
                }

                using (var tx = env.ReadTransaction())
                {
                    var aTree = tx.CreateTree("atree");
                    var bTree = tx.CreateTree("btree");

                    for (var i = 0; i < 1000; i++)
                    {
                        Assert.NotNull(aTree.Read("key" + i));
                    }

                    for (var i = 0; i < 5; i++)
                    {
                        Assert.NotNull(bTree.Read("key" + i));
                    }
                }
            }

        }

        [Fact]
        public void StorageRecoveryShouldWorkForSplitTransactions()
        {
            var random = new Random(1234);
            var buffer = new byte[4096];
            random.NextBytes(buffer);
            var count = 1000;

            var options = StorageEnvironmentOptions.ForPath(DataDir);
            options.MaxLogFileSize = 10 * Constants.Storage.PageSize;

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree( "atree");
                    tx.CreateTree( "btree");

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var aTree = tx.CreateTree("atree");
                    var bTree = tx.CreateTree("btree");

                    for (var i = 0; i < count; i++)
                    {
                        aTree.Add("a" + i, new MemoryStream(buffer));
                        bTree.MultiAdd("a", "a" + i);
                    }

                    tx.Commit();
                }
            }

            var expectedString = Encoding.UTF8.GetString(buffer);

            options = StorageEnvironmentOptions.ForPath(DataDir);
            options.MaxLogFileSize = 10 * Constants.Storage.PageSize;

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree( "atree");
                    tx.CreateTree( "btree");

                    tx.Commit();
                }

                using (var tx = env.ReadTransaction())
                {
                    var aTree = tx.CreateTree("atree");
                    var bTree = tx.CreateTree("btree");

                    for (var i = 0; i < count; i++)
                    {
                        var read = aTree.Read("a" + i);
                        Assert.NotNull(read);
                        Assert.Equal(expectedString, read.Reader.ToStringValue());
                    }

                    using (var iterator = bTree.MultiRead("a"))
                    {
                        Assert.True(iterator.Seek(Slices.BeforeAllKeys));

                        var keys = new HashSet<string>();
                        do
                        {
                            keys.Add(iterator.CurrentKey.ToString());
                        }
                        while (iterator.MoveNext());

                        Assert.Equal(count, keys.Count);
                    }
                }
            }
        }

        [Fact]
        public void StorageRecoveryShouldWorkWhenJournalNeedsMultipleSteps()
        {
            var sync = new StorageEnvironmentSynchronization(1, 12 * Constants.Size.Kilobyte);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir), sync))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("tree");

                    for (var i = 0; i < 10000; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(DataDir)))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("tree");

                    tx.Commit();
                }


                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.CreateTree("tree");

                    for (var i = 0; i < 10000; i++)
                    {
                        Assert.NotNull(tree.Read("key" + i));
                    }
                }
            }
        }
    }
}
