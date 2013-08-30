using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Storage
{
    public class Restarts
    {
        [Fact]
        public void DataIsKeptAfterRestart()
        {
            using (var pureMemoryPager = new PureMemoryPager())
            {
                using (var env = new StorageEnvironment(pureMemoryPager, ownsPager: false))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.Root.Add(tx, "test/1", new MemoryStream());
                        tx.Commit();
                    }
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.Root.Add(tx, "test/2", new MemoryStream());
                        tx.Commit();
                    }
                }

                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.Read))
                    {
                        Assert.NotNull(env.Root.Read(tx, "test/1"));
                        Assert.NotNull(env.Root.Read(tx, "test/2"));
                        tx.Commit();
                    }
                }
           }
        }

        [Fact]
        public void DataIsKeptAfterRestartForSubTrees()
        {
            using (var pureMemoryPager = new PureMemoryPager())
            {
                using (var env = new StorageEnvironment(pureMemoryPager, ownsPager: false))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.CreateTree(tx, "test");
                        tx.Commit();
                    }
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.GetTree(tx,"test").Add(tx, "test", Stream.Null);
                        tx.Commit();
                    }
                }

                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.CreateTree(tx, "test");
                        tx.Commit();
                    }

                    using (var tx = env.NewTransaction(TransactionFlags.Read))
                    {
                        Assert.NotNull(env.GetTree(tx,"test").Read(tx, "test"));
                        tx.Commit();
                    }
                }
            }
        }
    }
}