using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Tests.Bugs
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    using Xunit;

    public class Recovery : StorageTest
    {
        [PrefixesFact]
        public void StorageRecoveryShouldWorkWhenThereAreNoTransactionsToRecoverFromLog()
        {
            var path = "test2.data";

            DeleteDirectory(path);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
            }

            DeleteDirectory(path);
        }

        [PrefixesFact]
        public void StorageRecoveryShouldWorkWhenThereSingleTransactionToRecoverFromLog()
        {
            var path = "test2.data";
            DeleteDirectory(path);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = env.CreateTree(tx, "tree");

                    for (var i = 0; i < 100; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "tree");

                    tx.Commit();
                }


                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var tree = tx.Environment.CreateTree(tx,"tree");

                    for (var i = 0; i < 100; i++)
                    {
                        Assert.NotNull(tree.Read("key" + i));
                    }
                }
            }

            DeleteDirectory(path);
        }

        [PrefixesFact]
        public void StorageRecoveryShouldWorkWhenThereAreCommitedAndUncommitedTransactions()
        {
            var path = "test2.data";

            DeleteDirectory(path);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "tree");

                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    for (var i = 0; i < 10000; i++)
                    {
                        tx.Environment.CreateTree(tx,"tree").Add("a" + i, new MemoryStream());
                    }
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
            }

            DeleteDirectory(path);
        }

        [PrefixesFact]
        public void StorageRecoveryShouldWorkWhenThereAreCommitedAndUncommitedTransactions2()
        {
            var path = "test2.data";
            DeleteDirectory(path);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "atree");
                    env.CreateTree(tx, "btree");

                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    for (var i = 0; i < 10000; i++)
                    {
                        tx.Environment.CreateTree(tx,"atree").Add("a" + i, new MemoryStream());
                        tx.Environment.CreateTree(tx,"btree").MultiAdd("a" + i, "a" + i);
                    }
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
            }

            DeleteDirectory(path);
        }

        [PrefixesFact]
        public void StorageRecoveryShouldWorkWhenThereAreMultipleCommitedTransactions()
        {
            var path = "test2.data";

            DeleteDirectory(path);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = env.CreateTree(tx, "atree");

                    for (var i = 0; i < 1000; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = env.CreateTree(tx, "btree");

                    for (var i = 0; i < 1; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "atree");
                    env.CreateTree(tx, "btree");

                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var aTree = tx.Environment.CreateTree(tx,"atree");
                    var bTree = tx.Environment.CreateTree(tx,"btree");

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

            DeleteDirectory(path);
        }

        [PrefixesFact]
        public void StorageRecoveryShouldWorkWhenThereAreMultipleCommitedTransactions2()
        {
            var path = "test2.data";

            DeleteDirectory(path);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = env.CreateTree(tx, "atree");

                    for (var i = 0; i < 1000; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = env.CreateTree(tx, "btree");

                    for (var i = 0; i < 5; i++)
                    {
                        tree.Add("key" + i, new MemoryStream());
                    }

                    tx.Commit();
                }
            }

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "atree");
                    env.CreateTree(tx, "btree");

                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var aTree = tx.Environment.CreateTree(tx,"atree");
                    var bTree = tx.Environment.CreateTree(tx,"btree");

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

            DeleteDirectory(path);
        }

        [PrefixesFact]
        public void StorageRecoveryShouldWorkForSplitTransactions()
        {
            var random = new Random(1234);
            var buffer = new byte[4096];
            random.NextBytes(buffer);
            var path = "test2.data";
            var count = 1000;
            DeleteDirectory(path);

            var options = StorageEnvironmentOptions.ForPath(path);
            options.MaxLogFileSize = 10 * AbstractPager.PageSize;

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "atree");
                    env.CreateTree(tx, "btree");

                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var aTree = tx.Environment.CreateTree(tx,"atree");
                    var bTree = tx.Environment.CreateTree(tx,"btree");

                    for (var i = 0; i < count; i++)
                    {
                        aTree.Add("a" + i, new MemoryStream(buffer));
                        bTree.MultiAdd("a", "a" + i);
                    }

                    tx.Commit();
                }
            }

            var expectedString = Encoding.UTF8.GetString(buffer);

            options = StorageEnvironmentOptions.ForPath(path);
            options.MaxLogFileSize = 10 * AbstractPager.PageSize;

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    env.CreateTree(tx, "atree");
                    env.CreateTree(tx, "btree");

                    tx.Commit();
                }

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var aTree = tx.Environment.CreateTree(tx,"atree");
                    var bTree = tx.Environment.CreateTree(tx,"btree");

                    for (var i = 0; i < count; i++)
                    {
                        var read = aTree.Read("a" + i);
                        Assert.NotNull(read);
                        Assert.Equal(expectedString, read.Reader.ToStringValue());
                    }

                    using (var iterator = bTree.MultiRead("a"))
                    {
                        Assert.True(iterator.Seek(Slice.BeforeAllKeys));

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

            DeleteDirectory(path);
        }
    }
}
