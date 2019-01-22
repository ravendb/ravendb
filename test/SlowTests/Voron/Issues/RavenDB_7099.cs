using System;
using System.IO;
using System.Threading;
using FastTests.Voron;
using SlowTests.Utils;
using Voron;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_7099 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
        }

        [Theory]
        [InlineDataWithRandomSeed()]
        public void Do_not_create_recyclable_journal_files_on_db_load(int seed)
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

            var journalPath = ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).JournalPath.FullPath;

            var journalsForReuse = new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*");

            Assert.Equal(0, journalsForReuse.Length);

            RestartDatabase();

            journalsForReuse = new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*");

            Assert.Equal(0, journalsForReuse.Length);
        }

        [Theory]
        [InlineDataWithRandomSeed()]
        public void Flushed_journals_should_become_recyclable_files_after_sync(int seed)
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

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                var result = operation.SyncDataFile();

                Assert.True(result);
            }

            var journalPath = ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).JournalPath.FullPath;

            Assert.True(SpinWait.SpinUntil(() => new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*").Length > 0,
                TimeSpan.FromSeconds(30)));
        }
    }
}
