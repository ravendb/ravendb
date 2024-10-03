﻿using System;
using System.IO;
using FastTests.Voron;
using Sparrow.Backups;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.Backup;
using Voron.Impl.Journal;
using Voron.Util.Settings;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron
{
    public class RavenDB_12725_2 : StorageTest
    {
        public RavenDB_12725_2(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = 4096;
        }


        [RavenTheory(RavenTestCategory.BackupExportImport)]
        [InlineData(SnapshotBackupCompressionAlgorithm.Deflate)]
        [InlineData(SnapshotBackupCompressionAlgorithm.Zstd)]
        public void After_backup_and_restore_recovery_must_not_throw_missing_journal_if_we_have_synced_everything(SnapshotBackupCompressionAlgorithm compressionAlgorithm)
        {
            RequireFileBasedPager();

            var r = new Random(1); // we must use seed to ensure we always fill up the journal
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

            for (int i = 0; i < 20; i++)
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

            // here we're filling up the last journal completely, we'll have Available4Kbs == 0 after the commit

            using (var tx = Env.WriteTransaction())
            {
                Tree tree = tx.CreateTree("tree");

                for (int j = 0; j < 226; j++)
                {
                    var specialBytes = new byte[1024];

                    r.NextBytes(specialBytes);
                    tree.Add(new string((char)j, 1000), specialBytes);
                }

                tx.Commit();
            }

            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                var syncResult = operation.SyncDataFile();
            }

            var voronDataDir = new VoronPathSetting(DataDir);

            BackupMethods.Full.ToFile(Env, voronDataDir.Combine("voron-test.backup"), compressionAlgorithm);

            BackupMethods.Full.Restore(voronDataDir.Combine("voron-test.backup"), voronDataDir.Combine("backup-test.data"));

            var options = StorageEnvironmentOptions.ForPathForTests(Path.Combine(DataDir, "backup-test.data"));
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.ReadTransaction())
                {
                    using (var it = tx.ReadTree("tree").Iterate(prefetch: false))
                    {
                        Assert.True(it.Seek(Slices.BeforeAllKeys));

                        var count = 0;

                        do
                        {
                            count++;
                        } while (it.MoveNext());

                        Assert.Equal(226, count);
                    }
                }
            }
        }

        [Fact]
        public void Recovery_must_not_throw_missing_journal_if_we_have_synced_everything()
        {
            RequireFileBasedPager();

            var r = new Random(1); // we must use seed to ensure we always fill up the journal
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

            for (int i = 0; i < 20; i++)
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

            // here we're filling up the last journal completely, we'll have Available4Kbs == 0 after the commit

            using (var tx = Env.WriteTransaction())
            {
                Tree tree = tx.CreateTree("tree");

                for (int j = 0; j < 226; j++)
                {
                    var specialBytes = new byte[1024];

                    r.NextBytes(specialBytes);
                    tree.Add(new string((char)j, 1000), specialBytes);
                }

                tx.Commit();
            }

            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                var syncResult = operation.SyncDataFile();
            }

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("foobar");

                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                using (var it = tx.ReadTree("tree").Iterate(prefetch: false))
                {
                    Assert.True(it.Seek(Slices.BeforeAllKeys));

                    var count = 0;

                    do
                    {
                        count++;
                    } while (it.MoveNext());

                    Assert.Equal(226, count);
                }
            }
        }
    }
}
