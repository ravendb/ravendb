using System;
using System.IO;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_16860 : StorageTest
    {
        public RavenDB_16860(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.MaxNumberOfRecyclableJournals = 3;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Should_limit_number_of_recycled_journals()
        {
            RequireFileBasedPager();

            Assert.Equal(3, Env.Options.MaxNumberOfRecyclableJournals);

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

            using (var op = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                op.SyncDataFile();
            }

            var journalPath = ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).JournalPath.FullPath;

            Assert.Equal(3, new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*").Length);
        }
    }
}
