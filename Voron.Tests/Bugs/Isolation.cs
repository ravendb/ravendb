namespace Voron.Tests.Bugs
{
    using System.IO;

    using Xunit;

    public class Isolation : StorageTest
    {
        [Fact]
        public void ScratchPagesShouldNotBeReleasedUntilNotUsed()
        {
            var directory = "Test";

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
                        tx.GetTree("tree0").Add(tx, string.Format("key/{0}/{1}/1", new string('0', 1000), a), new MemoryStream());
                        tx.GetTree("tree0").Add(tx, string.Format("key/{0}/{1}/2", new string('0', 1000), a), new MemoryStream());

                        tx.Commit();
                    }
                }

                using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    tx.GetTree("tree1").Add(tx, "yek/1", new MemoryStream());

                    tx.Commit();
                }

                using (var txr = env.NewTransaction(TransactionFlags.Read))
                {
                    using (var iterator = txr.GetTree("tree0").Iterate(txr))
                    {
                        Assert.True(iterator.Seek(Slice.BeforeAllKeys)); // all pages are from scratch (one from position 11)

                        var currentKey = iterator.CurrentKey.ToString();

                        env.FlushLogToDataFile(); // frees pages from scratch (including the one at position 11)

                        using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
                        {
                            var tree = txw.GetTree("tree1");
                            tree.Add(txw, string.Format("yek/{0}/0/0", new string('0', 1000)), new MemoryStream()); // allocates new page from scratch (position 11)

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