using System.IO;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class EmptyTree : FastTests.Voron.StorageTest
    {
        public EmptyTree(ITestOutputHelper output) : base(output)
        {
        }

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
                var treeIterator = tx.CreateTree("events").Iterate(false);

                Assert.False(treeIterator.Seek(Slices.AfterAllKeys));

                tx.Commit();
            }
        }

        [Fact]
        public void SurviveRestart()
        {
            using (var options = StorageEnvironmentOptions.CreateMemoryOnlyForTests())
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
