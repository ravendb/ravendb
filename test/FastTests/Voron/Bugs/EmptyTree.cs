using System.IO;
using Xunit;
using Voron;

namespace FastTests.Voron.Bugs
{
    public class EmptyTree : StorageTest
    {
        [Fact]
        public void ShouldBeEmpty()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("events");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var treeIterator = tx.CreateTree("events").Iterate();

                Assert.False(treeIterator.Seek(Slices.AfterAllKeys));

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
                    using (var tx = env.WriteTransaction())
                    {
                        tx.CreateTree("events");

                        tx.Commit();
                    }

                    using (var tx = env.WriteTransaction())
                    {
                        tx.CreateTree("events").Add("test", new MemoryStream(0));

                        tx.Commit();
                    }
                }

                using (var env = new StorageEnvironment(options))
                {
                    using (var tx = env.WriteTransaction())
                    {
                        tx.CreateTree("events");

                        tx.Commit();
                    }

                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("events");
                        var readResult = tree.Read("test");
                        Assert.NotNull(readResult);

                        tx.Commit();
                    }
                }
            }


        }
    }
}
