using System;
using System.IO;
using FastTests.Voron;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.Backup;
using Voron.Impl.Journal;
using Voron.Util.Settings;
using Xunit;

namespace SlowTests.Voron
{
    public class RavenDB_12725 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = 1 * 1024 * 1024;
        }

        [Fact]
        public void Recovery_must_not_delete_journals_that_havent_been_synced_yet()
        {
            RequireFileBasedPager();

            var r = new Random();
            var bytes = new byte[512];

            for (int i = 0; i < 10; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    Tree tree = tx.CreateTree("tree");

                    for (int j = 0; j < 100; j++)
                    {
                        r.NextBytes(bytes);
                        tree.Add(new string((char) j, 1000), bytes);
                    }

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();

            for (int i = 0; i < 10; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    Tree tree = tx.CreateTree("tree");

                    for (int j = 0; j < 100; j++)
                    {
                        r.NextBytes(bytes);
                        tree.Add(new string((char)j, 1000), bytes);
                    }

                    tx.Commit();
                }
            }

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator)
            {
                AfterGatherInformationAction = () => Env.FlushLogToDataFile()
            })
            {
                var syncResult = operation.SyncDataFile();
            }

            RestartDatabase();
        }

        [Fact]
        public void Full_backup_must_backup_journals_that_we_havent_synced_yet()
        {
            RequireFileBasedPager();

            var r = new Random();
            var bytes = new byte[512];

            for (int i = 0; i < 10; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    Tree tree = tx.CreateTree("tree");

                    for (int j = 0; j < 100; j++)
                    {
                        r.NextBytes(bytes);
                        tree.Add(new string((char)j, 1000), bytes);
                    }

                    tx.Commit();
                }
            }

            Env.FlushLogToDataFile();

            for (int i = 0; i < 10; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    Tree tree = tx.CreateTree("tree");

                    for (int j = 0; j < 100; j++)
                    {
                        r.NextBytes(bytes);
                        tree.Add(new string((char)j, 1000), bytes);
                    }

                    tx.Commit();
                }
            }

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator)
            {
                AfterGatherInformationAction = () =>
                {
                    Env.FlushLogToDataFile();
                }
            })
            {
                var syncResult = operation.SyncDataFile();
            }

            var voronDataDir = new VoronPathSetting(DataDir);

            BackupMethods.Full.ToFile(Env, voronDataDir.Combine("voron-test.backup"));

            BackupMethods.Full.Restore(voronDataDir.Combine("voron-test.backup"), voronDataDir.Combine("backup-test.data"));

            var options = StorageEnvironmentOptions.ForPath(Path.Combine(DataDir, "backup-test.data"));
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            using (var env = new StorageEnvironment(options))
            {
                
            }
        }

    }
}
