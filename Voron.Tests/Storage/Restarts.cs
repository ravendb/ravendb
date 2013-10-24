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
            using (var pureMemoryPager = new PureMemoryPager())
            {
                using (var env = new StorageEnvironment(pureMemoryPager, ownsPager: false))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.RootTree(tx).Add(tx, "test/1", new MemoryStream());
                        tx.Commit();
                    }
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.RootTree(tx).Add(tx, "test/2", new MemoryStream());
                        tx.Commit();
                    }
                }

                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.Read))
                    {
                        Assert.NotNull(env.RootTree(tx).Read(tx, "test/1"));
                        Assert.NotNull(env.RootTree(tx).Read(tx, "test/2"));
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
	                    var tree = env.GetTree(tx,"test");
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
	                    var tree = env.GetTree(tx,"test");
	                    Assert.NotNull(tree.Read(tx, "test"));
                        tx.Commit();
                    }
                }
            }
        }
    }
}