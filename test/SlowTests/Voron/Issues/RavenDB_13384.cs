using System;
using FastTests.Voron;
using Voron;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_13384 : StorageTest
    {
        public RavenDB_13384(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = 4096;
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void Recovery_should_handle_empty_journal_file_and_correctly_set_last_flushed_journal(bool runInMemory)
        {
            if (runInMemory == false)
                RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("item", new byte[]{ 1, 2, 3});

                tx.Commit();
            }

            var numberOfJournals = Env.Journal.Files.Count;

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("item", new byte[] { 1, 2, 3 });

                Assert.Throws<InvalidOperationException>(() =>
                {
                    using (tx.LowLevelTransaction.ForTestingPurposesOnly().CallJustBeforeWritingToJournal(() => throw new InvalidOperationException()))
                    {
                        tx.Commit();
                    }
                });
            }

            // we failed to commit the above transaction and write data to file 
            // but we managed to create _empty_ journal file

            Assert.Equal(numberOfJournals + 1, Env.Journal.Files.Count);

            RestartDatabase();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                // attempt to sync throws the following exception:
                // System.InvalidOperationException : The lock task failed
                // ----Voron.Exceptions.VoronUnrecoverableErrorException : Error syncing the data file.The last sync tx is 2, but the journal's last tx id is -1, possible file corruption?

                operation.SyncDataFile();
            }

            // let's make sure we can restart immediately after sync

            RestartDatabase();

            // there is nothing to flush but let's validate it won't throw

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            // let's make sure we can put more stuff there

            for (int i = 0; i < 5; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("item", new byte[] { 1, 2, 3 });

                    tx.Commit();
                }
            }

            RestartDatabase();

            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            // lets add more data once again and force flushing and syncing

            for (int i = 0; i < 5; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("item", new byte[] { 1, 2, 3 });

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }
        }
    }
}
