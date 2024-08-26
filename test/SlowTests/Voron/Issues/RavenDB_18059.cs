using System;
using System.IO;
using System.Threading;
using FastTests.Voron;
using Sparrow.Backups;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Backup;
using Voron.Impl.Journal;
using Voron.Util.Settings;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues;

public class RavenDB_18059 : StorageTest
{
    public RavenDB_18059(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.MaxLogFileSize = 1000 * Constants.Storage.PageSize;
        options.ManualFlushing = true;
        options.ManualSyncing = true;
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData(SnapshotBackupCompressionAlgorithm.Deflate)]
    [InlineData(SnapshotBackupCompressionAlgorithm.Zstd)]
    public void RaceConditionBetweenFullBackupAndUpdateDatabaseStateAfterSync(SnapshotBackupCompressionAlgorithm compressionAlgorithm)
    {
        RequireFileBasedPager();
        var random = new Random(2);
        var buffer = new byte[8192];
        random.NextBytes(buffer);

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree("foo");
            for (int i = 0; i < 5000; i++)
            {
                tree.Add("items/" + i, new MemoryStream(buffer));
            }

            tx.Commit();
        }

        Assert.True(Env.Journal.Files.Count > 1);

        Env.FlushLogToDataFile(); // force writing data to the data file

        var voronDataDir = new VoronPathSetting(DataDir);

        Thread syncOperationThread = null;
        bool syncResult = false;

        Env.ForTestingPurposesOnly().ActionToCallDuringFullBackupRighAfterCopyHeaders += () =>
        {
            // here we remove 0000000000000000000.journal file while during backup we'll try to backup it

            syncOperationThread = new Thread(() =>
            {
                using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
                {
                    syncResult = operation.SyncDataFile();
                }
            });

            syncOperationThread.Start();

            // ActionToCallDuringFullBackupRighAfterCopyHeaders() is called under the flushing lock
            // the sync operation is trying to get the same lock
            // it means that as long as we wait here the lock won't be released so the thread won't be terminated
            Assert.False(syncOperationThread.Join(TimeSpan.FromSeconds(5))); 
        };

        BackupMethods.Full.ToFile(Env, voronDataDir.Combine("voron-test.backup"), compressionAlgorithm);

        Assert.True(syncOperationThread.Join(TimeSpan.FromMinutes(1))); // let's wait for the sync operation thread to complete

        Assert.True(syncResult); // at this point we should be after the successful sync 

        BackupMethods.Full.Restore(voronDataDir.Combine("voron-test.backup"), voronDataDir.Combine("backup-test.data"));

        var options = StorageEnvironmentOptions.ForPathForTests(Path.Combine(DataDir, "backup-test.data"));
        options.MaxLogFileSize = Env.Options.MaxLogFileSize;

        using (var env = new StorageEnvironment(options))
        {
            using (var tx = env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 5000; i++)
                {
                    var readResult = tree.Read("items/" + i);
                    Assert.NotNull(readResult);
                    var memoryStream = new MemoryStream();
                    readResult.Reader.CopyTo(memoryStream);
                    Assert.Equal(memoryStream.ToArray(), buffer);
                }
            }
        }
    }


    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData(SnapshotBackupCompressionAlgorithm.Deflate)]
    [InlineData(SnapshotBackupCompressionAlgorithm.Zstd)]
    public void FullBackupMustNotDeadlockWithFlush(SnapshotBackupCompressionAlgorithm compressionAlgorithm)
    {
        RequireFileBasedPager();
        var random = new Random(2);
        var buffer = new byte[8192];
        random.NextBytes(buffer);

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree("foo");

            for (int i = 0; i < 5000; i++)
            {
                tree.Add("items/" + i, new MemoryStream(buffer));
            }

            tx.Commit();
        }

        var voronDataDir = new VoronPathSetting(DataDir);

        var backupCompleted = false;
        Exception backupException = null;

        Thread backup = new Thread(() =>
        {
            try
            {
                BackupMethods.Full.ToFile(Env, voronDataDir.Combine("voron-test.backup"), compressionAlgorithm);

                backupCompleted = true;
            }
            catch (Exception e)
            {
                backupException = e;
            }
        });

        Env.Journal.Applicator.ForTestingPurposesOnly().OnApplyLogsToDataFileUnderFlushingLock += () =>
        {
            backup.Start();
        };

        Env.FlushLogToDataFile();

        backup.Join(TimeSpan.FromSeconds(60));

        Assert.Null(backupException);
        Assert.True(backupCompleted);
    }
}
