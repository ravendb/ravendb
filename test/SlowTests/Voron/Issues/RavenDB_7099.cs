using System;
using System.IO;
using System.Threading;
using SlowTests.Utils;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_7099 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Theory]
        [InlineDataWithRandomSeed()]
        public void Do_not_create_pending_recycle_files_on_db_load(int seed)
        {
            RequireFileBasedPager();

            var r = new Random(seed);

            var bytes = new byte[1024];

            for (int i = 0; i < 100; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    r.NextBytes(bytes);

                    tx.CreateTree("items").Add($"item/{i}", new MemoryStream(bytes));

                    tx.Commit();
                }
            }

            var journalsForReuse = new DirectoryInfo(DataDir).GetFiles($"{StorageEnvironmentOptions.PendingRecycleFileNamePrefix}*");

            Assert.Equal(0, journalsForReuse.Length);

            RestartDatabase();

            journalsForReuse = new DirectoryInfo(DataDir).GetFiles($"{StorageEnvironmentOptions.PendingRecycleFileNamePrefix}*");

            Assert.Equal(0, journalsForReuse.Length);
        }

        [Theory]
        [InlineDataWithRandomSeed()]
        public void Flushed_journals_should_become_pending_recycle_files_after_sync(int seed)
        {
            RequireFileBasedPager();

            var r = new Random(seed);

            var bytes = new byte[1024];

            for (int i = 0; i < 1000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    r.NextBytes(bytes);

                    tx.CreateTree("items").Add($"item/{i}", new MemoryStream(bytes));

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();
            Env.ForceSyncDataFile();

            SpinWait.SpinUntil(() => new DirectoryInfo(DataDir).GetFiles($"{StorageEnvironmentOptions.PendingRecycleFileNamePrefix}*").Length > 0,
                TimeSpan.FromSeconds(30));
        }
    }
}