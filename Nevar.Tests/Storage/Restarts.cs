using System.IO;
using Nevar.Impl;
using Nevar.Tests.Trees;
using Xunit;

namespace Nevar.Tests.Storage
{
    public class Restarts
    {
        [Fact]
        public void DataIsKepAfterRestart()
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
    }
}