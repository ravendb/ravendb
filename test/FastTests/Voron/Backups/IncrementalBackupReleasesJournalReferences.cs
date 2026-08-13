using System;
using System.IO;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Backup;
using Voron.Impl.Journal;
using Voron.Util.Settings;
using Xunit;

namespace FastTests.Voron.Backups
{
    public class IncrementalBackupReleasesJournalReferences : StorageTest
    {
        public IncrementalBackupReleasesJournalReferences(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 1000 * Constants.Storage.PageSize;
            options.IncrementalBackupEnabled = true;
            options.ManualFlushing = true;
            options.ManualSyncing = true;
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.BackupExportImport)]
        public void BackupReleasesJournalReference()
        {
            RequireFileBasedPager();

            var random = new Random(1);
            var buffer = new byte[512];
            long firstCycleCurrentJournal = -1;

            for (int cycle = 0; cycle < 2; cycle++)
            {
                var prefix = "cycle" + cycle + "/";

                WriteRandomData(random, buffer, prefix);
                Env.FlushLogToDataFile();
                AssertSync();

                if (cycle == 0)
                {
                    // the flush sealed journal 0 and removed it from the open set,
                    // so GetJournalFile has to construct an ad-hoc JournalFile for it
                    Assert.DoesNotContain(Env.Journal.Files, x => x.Number == 0);
                    Assert.True(Env.Options.JournalExists(0));
                }

                // after the flush there may be no current journal; a write before the backup
                // keeps one alive, otherwise the deletion guard's lastWrittenLogFile is -1
                WriteRandomData(random, buffer, prefix + "current", count: 1);
                Assert.NotNull(Env.Journal.CurrentFile);

                if (cycle == 0)
                {
                    firstCycleCurrentJournal = Env.Journal.CurrentFile.Number;
                }
                else
                {
                    // the journal protected as current during the first backup is still on disk
                    Assert.True(Env.Options.JournalExists(firstCycleCurrentJournal));
                }

                BackupMethods.Incremental.ToFile(Env,
                    new VoronPathSetting(DataDir).Combine($"incremental-backup-{cycle}.zip").FullPath);

                // the single reference GetJournalFile takes is released by the backup's finally,
                // so a fully captured journal's refcount hits zero and its file is moved to the
                // recyclable set; the second cycle proves references do not accumulate
                if (cycle == 0)
                {
                    Assert.False(Env.Options.JournalExists(0));
                }
                else
                {
                    Assert.False(Env.Options.JournalExists(firstCycleCurrentJournal));
                }
            }
        }

        private void WriteRandomData(Random random, byte[] buffer, string prefix, int count = 20000)
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("items");
                for (int i = 0; i < count; i++)
                {
                    random.NextBytes(buffer);
                    tree.Add(prefix + i, new MemoryStream(buffer));
                }
                tx.Commit();
            }
        }

        private void AssertSync()
        {
            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator)
            {
                AfterGatherInformationAction = () => Env.FlushLogToDataFile()
            })
            {
                Assert.True(operation.SyncDataFile());
            }
        }
    }
}
