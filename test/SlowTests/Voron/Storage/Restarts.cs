﻿using System.IO;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public class Restarts : FastTests.Voron.StorageTest
    {
        public Restarts(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
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
                        Assert.True(tree.TryRead("test/1", out _));
                        Assert.True(tree.TryRead("test/2", out _));
                        tx.Commit();
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
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
                        Assert.True(tree.TryRead("test/1", out _));
                        Assert.True(tree.TryRead("test/2", out _));
                        tx.Commit();
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
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

                        Assert.True(tree.TryRead("test", out _));
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
                        Assert.True(tree.TryRead("test", out _));
                        tx.Commit();
                    }
                }
            }
        }
    }
}
