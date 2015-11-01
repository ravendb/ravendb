using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Bugs
{
    public class EmptyTree : StorageTest
    {
        [PrefixesFact]
        public void ShouldBeEmpty()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "events");

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var treeIterator = tx.Environment.CreateTree(tx,"events").Iterate();

                Assert.False(treeIterator.Seek(Slice.AfterAllKeys));

                tx.Commit();
            }
        }

        [PrefixesFact]
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
                        tx.Environment.CreateTree(tx,"events").Add("test", new MemoryStream(0));

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
                        var tree = tx.Environment.CreateTree(tx,"events");
                        var readResult = tree.Read("test");
                        Assert.NotNull(readResult);

                        tx.Commit();
                    }
                }
            }


        }
    }
}
