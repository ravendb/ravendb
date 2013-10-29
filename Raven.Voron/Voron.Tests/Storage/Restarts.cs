using System.IO;
using Voron.Impl;
using Voron.Trees;
using Xunit;

namespace Voron.Tests.Storage
{
    public class Restarts
    {
        [Fact]
        public void DataIsKeptAfterRestart()
        {
            using (var pureMemoryPager = StorageEnvironmentOptions.GetInMemory())
            {
                pureMemoryPager.OwnsPagers = false;
                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        tx.State.Root.Add(tx, "test/1", new MemoryStream());
                        tx.Commit();
                    }
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        tx.State.Root.Add(tx, "test/2", new MemoryStream());
                        tx.Commit();
                    }
                }

                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.Read))
                    {
                        Assert.NotNull(tx.State.Root.Read(tx, "test/1"));
                        Assert.NotNull(tx.State.Root.Read(tx, "test/2"));
                        tx.Commit();
                    }
                }
            }
        }

        [Fact]
        public void DataIsKeptAfterRestartForSubTrees()
        {
            using (var pureMemoryPager = StorageEnvironmentOptions.GetInMemory())
            {
                pureMemoryPager.OwnsPagers = false;
                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.CreateTree(tx, "test");
                        tx.Commit();
                    }
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var tree = tx.GetTree("test");
                        tree.Add(tx, "test", Stream.Null);
                        tx.Commit();

                        Assert.NotNull(tree.Read(tx, "test"));
                    }
                }

                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var tree = env.CreateTree(tx, "test");
                        tx.Commit();
                    }

                    using (var tx = env.NewTransaction(TransactionFlags.Read))
                    {
                        var tree = tx.GetTree("test");
                        Assert.NotNull(tree.Read(tx, "test"));
                        tx.Commit();
                    }
                }
            }
        }
    }
}