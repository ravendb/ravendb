using System;
using System.Diagnostics;
using System.IO;
using Voron.Debugging;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Storage
{
    public class Restarts
    {
        [Fact]
        public void DataIsKeptAfterRestart_OnDisk()
        {
            if (Directory.Exists("test.data"))
                Directory.Delete("test.data", true);
            using (var pager = StorageEnvironmentOptions.ForPath("test.data"))
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
            using (var pureMemoryPager = StorageEnvironmentOptions.CreateMemoryOnly())
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
            using (var pureMemoryPager = StorageEnvironmentOptions.CreateMemoryOnly())
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
                        tx.Commit();

                        Assert.NotNull(tree.Read("test"));
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
