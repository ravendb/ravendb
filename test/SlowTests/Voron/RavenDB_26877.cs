using System;
using System.IO;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron
{
    public class RavenDB_26877 : StorageTest
    {
        public RavenDB_26877(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = 128 * 1024;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Can_write_after_recovery_which_ignored_missing_journals()
        {
            RequireFileBasedPager();

            WriteToTree("A", numberOfTxs: 2, itemsPerTx: 10);

            Env.FlushLogToDataFile();
            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                Assert.True(operation.SyncDataFile());
            }

            // the sync point (header) must stay in the first journal while further transactions are added to it
            Assert.Equal(0, Env.HeaderAccessor.CopyHeader().Journal.LastSyncedJournal);

            WriteToTree("B", numberOfTxs: 30, itemsPerTx: 30);

            // apply everything to the data file but do NOT sync - the data file content gets ahead of the header
            Env.FlushLogToDataFile();

            StopDatabase(shouldDisposeOptions: true);

            var journals = new DirectoryInfo(Path.Combine(DataDir, "Journals")).GetFiles("*.journal").OrderBy(x => x.Name).ToList();
            Assert.True(journals.Count > 1);

            // the first journal contains transactions after the sync point, deleting it makes
            // the recovery skip it and all the following journals (tx id gap)
            journals.First().Delete();

            Options = StorageEnvironmentOptions.ForPathForTests(DataDir);
            Options.IgnoreInvalidJournalErrors = true;
            Configure(Options);
            StartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                Tree tree = tx.CreateTree("tree");
                tree.Add("key-after-recovery", new byte[16]);
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Tree tree = tx.ReadTree("tree");

                Assert.NotNull(tree.Read("key-after-recovery"));

                // everything was flushed to the data file before the restart so no data must be lost
                for (int i = 0; i < 2; i++)
                for (int j = 0; j < 10; j++)
                    Assert.NotNull(tree.Read($"A-{i:D5}-{j:D5}"));

                for (int i = 0; i < 30; i++)
                for (int j = 0; j < 30; j++)
                    Assert.NotNull(tree.Read($"B-{i:D5}-{j:D5}"));
            }
        }

        private void WriteToTree(string prefix, int numberOfTxs, int itemsPerTx)
        {
            var r = new Random(42);
            var bytes = new byte[1024];

            for (int i = 0; i < numberOfTxs; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    Tree tree = tx.CreateTree("tree");

                    for (int j = 0; j < itemsPerTx; j++)
                    {
                        r.NextBytes(bytes);
                        tree.Add($"{prefix}-{i:D5}-{j:D5}", bytes);
                    }

                    tx.Commit();
                }
            }
        }
    }
}
