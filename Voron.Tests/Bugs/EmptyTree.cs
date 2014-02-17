using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Bugs
{
    public class EmptyTree : StorageTest
    {
        [Fact]
        public void ShouldBeEmpty()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "events");

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var treeIterator = tx.Environment.State.GetTree(tx,"events").Iterate(tx);

                Assert.False(treeIterator.Seek(Slice.AfterAllKeys));

                tx.Commit();
            }
        }

        [Fact]
        public void SurviveRestart()
        {
            using (var options = StorageEnvironmentOptions.CreateMemoryOnly())
            {
                options.OwnsPagers = false;
                using (var env = new StorageEnvironment(options))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.CreateTree(tx, "events");

                        tx.Commit();
                    }

                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        tx.Environment.State.GetTree(tx,"events").Add(tx, "test", new MemoryStream(0));

                        tx.Commit();
                    }
                }

                using (var env = new StorageEnvironment(options))
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        env.CreateTree(tx, "events");

                        tx.Commit();
                    }

                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var tree = tx.Environment.State.GetTree(tx,"events");
                        var readResult = tree.Read(tx, "test");
                        Assert.NotNull(readResult);

                        tx.Commit();
                    }
                }
            }


        }
    }
}