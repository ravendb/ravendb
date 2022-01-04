using System;
using System.IO;
using FastTests.Voron;
using Voron;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_17354 : StorageTest
    {
        public RavenDB_17354(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.MaxNumberOfRecyclableJournals = 0;
        }

        [Fact]
        public void Can_disable_journals_recycling()
        {
            RequireFileBasedPager();

            Assert.Equal(0, Env.Options.MaxNumberOfRecyclableJournals);

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

            Assert.Equal(0, new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*").Length);
        }
    }
}
