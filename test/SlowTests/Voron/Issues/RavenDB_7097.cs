using System;
using System.IO;
using System.Threading;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_7097 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void Recyclable_journal_files_are_deleted_on_dispose()
        {
            RequireFileBasedPager();

            var r = new Random(1);

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

            SpinWait.SpinUntil(() => new DirectoryInfo(DataDir).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*").Length == 6,
                TimeSpan.FromSeconds(30));

            Env.Dispose();

            var journalsForReuse = new DirectoryInfo(DataDir).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*");

            Assert.Equal(0, journalsForReuse.Length);
        }
    }
}