using System.IO;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Storage
{
    public class Restarts : FastTests.Voron.StorageTest
    {
        public Restarts(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DataIsKeptAfterRestart_OnDisk()
        {
            using (var pager = StorageEnvironmentOptions.ForPathForTests(DataDir))
            {
                pager.OwnsPagers = false;
                using (var env = new StorageEnvironment(pager))
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("foo");
                        tree.Add("test/1", new MemoryStream());
                        tx.Commit();
                    }
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("foo");
                        tree.Add("test/2", new MemoryStream());
                        tx.Commit();
                    }
                }

                using (var env = new StorageEnvironment(pager))
                {
                    using (var tx = env.ReadTransaction())
                    {
                        var tree = tx.CreateTree("foo");
                        Assert.NotNull(tree.Read("test/1"));
                        Assert.NotNull(tree.Read("test/2"));
                        tx.Commit();
                    }
                }
            }
        }

        [Fact]
        public void DataIsKeptAfterRestart()
        {
            using (var pureMemoryPager = StorageEnvironmentOptions.CreateMemoryOnlyForTests())
            {
                pureMemoryPager.OwnsPagers = false;
                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("foo");
                        tree.Add("test/1", new MemoryStream());
                        tx.Commit();
                    }
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("foo");
                        tree.Add("test/2", new MemoryStream());
                        tx.Commit();
                    }
                }

                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.ReadTransaction())
                    {
                        var tree = tx.CreateTree("foo");
                        Assert.NotNull(tree.Read("test/1"));
                        Assert.NotNull(tree.Read("test/2"));
                        tx.Commit();
                    }
                }
            }
        }

        [Fact]
        public void DataIsKeptAfterRestartForSubTrees()
        {
            using (var pureMemoryPager = StorageEnvironmentOptions.CreateMemoryOnlyForTests())
            {
                pureMemoryPager.OwnsPagers = false;
                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.WriteTransaction())
                    {
                       tx.CreateTree("test");
                        tx.Commit();
                    }
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("test");
                        tree.Add("test", Stream.Null);

                        Assert.NotNull(tree.Read("test"));
                        tx.Commit();
                    }
                }

                using (var env = new StorageEnvironment(pureMemoryPager))
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree( "test");
                        tx.Commit();
                    }

                    using (var tx = env.ReadTransaction())
                    {
                        var tree = tx.CreateTree("test");
                        Assert.NotNull(tree.Read("test"));
                        tx.Commit();
                    }
                }
            }
        }
    }
}
