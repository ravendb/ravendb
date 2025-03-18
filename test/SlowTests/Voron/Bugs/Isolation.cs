using System.Collections.Generic;
using System.IO;
using Raven.Server.Utils;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class Isolation : FastTests.Voron.StorageTest
    {
        public Isolation(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void MultiTreeIteratorShouldBeIsolated1()
        {
            IOExtensions.DeleteDirectory(DataDir);

            var options = StorageEnvironmentOptions.ForPathForTests(DataDir);

            using (var env = new StorageEnvironment(options))
            {
                CreateTrees(env, 1, "tree");

                for (var i = 0; i < 10; i++)
                    Write(env, i);

                using (var txr = env.ReadTransaction())
                {
                    var key = Write(env, 10);

                    using (var iterator = txr.ReadTree("tree0").MultiRead("key/1"))
                    {
                        Assert.True(iterator.Seek(Slices.BeforeAllKeys));

                        var count = 0;

                        do
                        {
                            Assert.True(iterator.CurrentKey.ToString() != key, string.Format("Key '{0}' should not be present in multi-iterator", key));

                            count++;
                        }
                        while (iterator.MoveNext());

                        Assert.Equal(10, count);
                    }
                }
            }
        }

        [Fact]
        public void MultiTreeIteratorShouldBeIsolated2()
        {
            IOExtensions.DeleteDirectory(DataDir);

            var options = StorageEnvironmentOptions.ForPathForTests(DataDir);

            using (var env = new StorageEnvironment(options))
            {
                CreateTrees(env, 1, "tree");

                for (var i = 0; i < 11; i++)
                    Write(env, i);

                using (var txr = env.ReadTransaction())
                {
                    var key = Delete(env, 10);

                    using (var iterator = txr.ReadTree("tree0").MultiRead("key/1"))
                    {
                        Assert.True(iterator.Seek(Slices.BeforeAllKeys));

                        var keys = new List<string>();

                        do
                        {
                            keys.Add(iterator.CurrentKey.ToString());
                        }
                        while (iterator.MoveNext());

                        Assert.Equal(11, keys.Count);
                        Assert.Contains(key, keys);
                    }
                }
            }
        }

        private static string Delete(StorageEnvironment env, int i)
        {
            using (var txw = env.WriteTransaction())
            {
                var key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" + i.ToString("D2");

                txw.ReadTree("tree0").MultiDelete("key/1", key);
                txw.Commit();

                return key;
            }
        }

        private static string Write(StorageEnvironment env, int i)
        {
            using (var txw = env.WriteTransaction())
            {
                var key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" + i.ToString("D2");

                txw.ReadTree("tree0").MultiAdd("key/1", key);
                txw.Commit();

                return key;
            }
        }

        [Fact]
        public void ScratchPagesShouldNotBeReleasedUntilNotUsed()
        {
            var options = StorageEnvironmentOptions.ForPathForTests(DataDir);

            options.ManualFlushing = true;
            using (var env = new StorageEnvironment(options))
            {
                CreateTrees(env, 2, "tree");
                for (int a = 0; a < 3; a++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        tx.CreateTree(  "tree0").Add(string.Format("key/{0}/{1}/1", new string('0', 1000), a), new MemoryStream());
                        tx.CreateTree(  "tree0").Add(string.Format("key/{0}/{1}/2", new string('0', 1000), a), new MemoryStream());

                        tx.Commit();
                    }
                }

                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree( "tree1").Add("yek/1", new MemoryStream());

                    tx.Commit();
                }

                using (var txr = env.ReadTransaction())
                {
                    using (var iterator = txr.CreateTree("tree0").Iterate(false))
                    {
                        Assert.True(iterator.Seek(Slices.BeforeAllKeys)); // all pages are from scratch (one from position 11)

                        var currentKey = iterator.CurrentKey.ToString();

                        env.FlushLogToDataFile(); // frees pages from scratch (including the one at position 11)

                        using (var txw = env.WriteTransaction())
                        {
                            var tree = txw.CreateTree("tree1");
                            tree.Add(string.Format("yek/{0}/0/0", new string('0', 1000)), new MemoryStream()); // allocates new page from scratch (position 11)

                            txw.Commit();
                        }

                        Assert.Equal(currentKey, iterator.CurrentKey.ToString());

                        using (var txw = env.WriteTransaction())
                        {
                            var tree = txw.CreateTree( "tree1");
                            tree.Add("fake", new MemoryStream());

                            txw.Commit();
                        }

                        Assert.Equal(currentKey, iterator.CurrentKey.ToString());

                        var count = 0;

                        do
                        {
                            currentKey = iterator.CurrentKey.ToString();
                            count++;

                            Assert.Contains("key/", currentKey);
                        }
                        while (iterator.MoveNext());

                        Assert.Equal(6, count);
                    }
                }
            }
        }
    }
}
