using System.Collections.Generic;

namespace Voron.Tests.Bugs
{
    using System.IO;

    using Xunit;

    public class Isolation : StorageTest
    {
        [PrefixesFact]
        public void MultiTreeIteratorShouldBeIsolated1()
        {
            var directory = "Test2";

            DeleteDirectory(directory);

            var options = StorageEnvironmentOptions.ForPath(directory);

            using (var env = new StorageEnvironment(options))
            {
                CreateTrees(env, 1, "tree");

                for (var i = 0; i < 10; i++)
                    Write(env, i);

                using (var txr = env.NewTransaction(TransactionFlags.Read))
                {
                    var key = Write(env, 10);

                    using (var iterator = txr.ReadTree("tree0").MultiRead("key/1"))
                    {
                        Assert.True(iterator.Seek(Slice.BeforeAllKeys));

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

        [PrefixesFact]
        public void MultiTreeIteratorShouldBeIsolated2()
        {
            var directory = "Test2";

            DeleteDirectory(directory);

            var options = StorageEnvironmentOptions.ForPath(directory);

            using (var env = new StorageEnvironment(options))
            {
                CreateTrees(env, 1, "tree");

                for (var i = 0; i < 11; i++)
                    Write(env, i);

                using (var txr = env.NewTransaction(TransactionFlags.Read))
                {
                    var key = Delete(env, 10);

                    using (var iterator = txr.ReadTree("tree0").MultiRead("key/1"))
                    {
                        Assert.True(iterator.Seek(Slice.BeforeAllKeys));

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
            using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" + i.ToString("D2");

                txw.ReadTree("tree0").MultiDelete("key/1", key);
                txw.Commit();

                return key;
            }
        }

        private static string Write(StorageEnvironment env, int i)
        {
            using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" + i.ToString("D2");

                txw.ReadTree("tree0").MultiAdd("key/1", key);
                txw.Commit();

                return key;
            }
        }

        [PrefixesFact]
        public void ScratchPagesShouldNotBeReleasedUntilNotUsed()
        {
            var directory = "Test2";

            if (Directory.Exists(directory))
                Directory.Delete(directory, true);

            var options = StorageEnvironmentOptions.ForPath(directory);

            options.ManualFlushing = true;
            using (var env = new StorageEnvironment(options))
            {
                CreateTrees(env, 2, "tree");
                for (int a = 0; a < 3; a++)
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        tx.Environment.CreateTree(tx, "tree0").Add(string.Format("key/{0}/{1}/1", new string('0', 1000), a), new MemoryStream());
                        tx.Environment.CreateTree(tx, "tree0").Add(string.Format("key/{0}/{1}/2", new string('0', 1000), a), new MemoryStream());

                        tx.Commit();
                    }
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tx.Environment.CreateTree(tx, "tree1").Add("yek/1", new MemoryStream());

                    tx.Commit();
                }

                using (var txr = env.NewTransaction(TransactionFlags.Read))
                {
                    using (var iterator = txr.Environment.CreateTree(txr, "tree0").Iterate())
                    {
                        Assert.True(iterator.Seek(Slice.BeforeAllKeys)); // all pages are from scratch (one from position 11)

                        var currentKey = iterator.CurrentKey.ToString();

                        env.FlushLogToDataFile(); // frees pages from scratch (including the one at position 11)

                        using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
                        {
                            var tree = txw.Environment.CreateTree(txw, "tree1");
                            tree.Add(string.Format("yek/{0}/0/0", new string('0', 1000)), new MemoryStream()); // allocates new page from scratch (position 11)

                            txw.Commit();
                        }

                        Assert.Equal(currentKey, iterator.CurrentKey.ToString());

                        using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
                        {
                            var tree = txw.Environment.CreateTree(txw, "tree1");
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
