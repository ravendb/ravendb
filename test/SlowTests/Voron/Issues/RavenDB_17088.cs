using System;
using FastTests.Voron;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_17088 : StorageTest
    {
        public RavenDB_17088(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxScratchBufferSize = 64 * 1024 * 4;
        }

        [Fact]
        public void CannotTryToGetPreventNewTransactionsLockRecursivelyDuringFlushing()
        {
            for (int i = 0; i < 100; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("items");

                    tree.Add("items/" + i, new byte[] { 1, 2, 3 });

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("items");

                tree.Add("foo/0", new byte[] { 1, 2, 3 });

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("items");

                tree.Add("foo/1", new byte[] { 1, 2, 3 });

                tx.Commit();
            }

            Env.ScratchBufferPool.RecycledScratchFileTimeout = TimeSpan.Zero; // by default we don't dispose scratches if they were recycled less than 1 minute ago

            Env.FlushLogToDataFile();
        }
    }
}
