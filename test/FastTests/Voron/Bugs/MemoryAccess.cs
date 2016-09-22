using System.IO;
using System.Threading;
using Xunit;
using Voron;
using Voron.Data;

namespace FastTests.Voron.Bugs
{
    public class MemoryAccess : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);
            options.ManualFlushing = true;
        }

        [Fact]
        public void ShouldNotThrowAccessViolation()
        {
            var trees = CreateTrees(Env, 1, "tree");

            for (int a = 0; a < 2; a++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    foreach (var tree in trees)
                    {
                        tx.ReadTree(tree).Add(string.Format("key/{0}/{1}/1", new string('0', 1000), a), new MemoryStream());
                        tx.ReadTree(tree).Add(string.Format("key/{0}/{1}/2", new string('0', 1000), a), new MemoryStream());
                    }

                    tx.Commit();
                }
            }

            using (var txr = Env.ReadTransaction())
            {
                foreach (var tree in trees)
                {
                    using (var iterator = txr.ReadTree(tree).Iterate(false))
                    {
                        if (!iterator.Seek(Slices.BeforeAllKeys))
                            continue;

                        Env.FlushLogToDataFile();

                        using (var txw = Env.WriteTransaction())
                        {
                            txw.ReadTree(tree).Add(string.Format("key/{0}/0/0", new string('0', 1000)), new MemoryStream());

                            txw.Commit();
                        }

                        Thread.Sleep(1000);

                        do
                        {
                            Assert.Contains("key/", iterator.CurrentKey.ToString());
                        }
                        while (iterator.MoveNext());
                    }
                }
            }
        }
    }
}
