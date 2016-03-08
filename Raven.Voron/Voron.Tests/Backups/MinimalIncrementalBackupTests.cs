using System;
using System.IO;
using System.IO.Compression;
using System.IO.Packaging;
using System.Linq;
using System.Text;
using System.Threading;
using Voron.Impl.Backup;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Backups
{
    public class MinimalIncrementalBackupTests : StorageTest
    {
        private string _tempDir;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.IncrementalBackupEnabled = true;
            options.MaxLogFileSize = 1000 * AbstractPager.PageSize;
        }

        public override void Dispose()
        {
            base.Dispose();

            if (_tempDir == null)
                return;

            Directory.Delete(_tempDir, true);
        }

        [PrefixesFact]
        public void Can_write_minimal_incremental_backup_and_restore_with_regular_incremental()
        {
            const int UserCount = 5000;
            _tempDir = Guid.NewGuid().ToString();
            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(_tempDir);
            storageEnvironmentOptions.IncrementalBackupEnabled = true;
            using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions))
            {
                int index = 0;
                for (int xi = 0; xi < 10; xi++)
                {
                    using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var tree = envToSnapshot.CreateTree(tx, "test");

                        for (int i = 0; i < UserCount / 10; i++)
                        {
                            tree.Add("users/" + index, "john doe/" + index);
                            index++;
                        }

                        tx.Commit();
                    }
                }

                var snapshotWriter = new MinimalIncrementalBackup();
                snapshotWriter.ToFile(envToSnapshot, Path.Combine(_tempDir, "1.snapshot"));
            }

            var incremental = new IncrementalBackup();

            var restoredOptions = StorageEnvironmentOptions.ForPath(Path.Combine(_tempDir, "restored"));
            incremental.Restore(restoredOptions, new[] { Path.Combine(_tempDir, "1.snapshot") });

            using (var snapshotRestoreEnv = new StorageEnvironment(restoredOptions))
            {
                using (var tx = snapshotRestoreEnv.NewTransaction(TransactionFlags.Read))
                {
                    var tree = tx.ReadTree("test");
                    Assert.NotNull(tree);

                    for (int i = 0; i < UserCount; i++)
                    {
                        var readResult = tree.Read("users/" + i);
                        Assert.NotNull(readResult);
                        Assert.Equal("john doe/" + i, readResult.Reader.ToStringValue());
                    }
                }
            }
        }

        [PrefixesFact]
        public void Can_use_full_back_then_full_min_backup()
        {
            _tempDir = Guid.NewGuid().ToString();
            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(_tempDir);
            storageEnvironmentOptions.IncrementalBackupEnabled = true;
            using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions))
            {
                using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = envToSnapshot.CreateTree(tx, "test");

                    for (int i = 0; i < 1000; i++)
                    {
                        tree.Add("users/" + i, "first/" + i);
                    }

                    tx.Commit();
                }

                new FullBackup().ToFile(envToSnapshot, Path.Combine(_tempDir, "full.backup"), CancellationToken.None);

                using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = envToSnapshot.CreateTree(tx, "test");

                    for (int i = 0; i < 500; i++)
                    {
                        tree.Add("users/" + i, "second/" + (i * 2));
                    }

                    for (int i = 0; i < 500; i++)
                    {
                        tree.Add("users/" + (i + 10000), "third/" + i);
                    }

                    tx.Commit();
                }
                new MinimalIncrementalBackup().ToFile(envToSnapshot, Path.Combine(_tempDir, "1.backup"));
            }


            new FullBackup().Restore(Path.Combine(_tempDir, "full.backup"), Path.Combine(_tempDir, "restored"));
            var restoredOptions = StorageEnvironmentOptions.ForPath(Path.Combine(_tempDir, "restored"));
            new IncrementalBackup().Restore(restoredOptions, new[] { Path.Combine(_tempDir, "1.backup") });

            using (var snapshotRestoreEnv = new StorageEnvironment(restoredOptions))
            {
                using (var tx = snapshotRestoreEnv.NewTransaction(TransactionFlags.Read))
                {
                    var tree = tx.ReadTree("test");
                    Assert.NotNull(tree);

                    for (int i = 0; i < 500; i++)
                    {
                        var readResult = tree.Read("users/" + i);
                        Assert.NotNull(readResult);
                        Assert.Equal("second/" + (i * 2), readResult.Reader.ToStringValue());
                    }
                    for (int i = 0; i < 500; i++)
                    {
                        var readResult = tree.Read("users/" + (i + 10000));
                        Assert.NotNull(readResult);
                        Assert.Equal("third/" + i, readResult.Reader.ToStringValue());
                    }

                    for (int i = 0; i < 500; i++)
                    {
                        var readResult = tree.Read("users/" + (i + 500));
                        Assert.NotNull(readResult);
                        Assert.Equal("first/" + (i+500), readResult.Reader.ToStringValue());
                    }
                }
            }
        }

        [PrefixesFact]
        public void Can_make_multiple_min_inc_backups_and_then_restore()
        {
            const int UserCount = 5000;
            _tempDir = Guid.NewGuid().ToString();
            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(_tempDir);
            storageEnvironmentOptions.IncrementalBackupEnabled = true;
            using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions))
            {
                int index = 0;
                for (int xi = 0; xi < 5; xi++)
                {
                    for (int yi = 0; yi < 2; yi++)
                    {
                        using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
                        {
                            var tree = envToSnapshot.CreateTree(tx, "test");

                            for (int i = 0; i < UserCount / 10; i++)
                            {
                                tree.Add("users/" + index, "john doe/" + index);
                                index++;
                            }

                            tx.Commit();
                        }
                    }
                    var snapshotWriter = new MinimalIncrementalBackup();
                    snapshotWriter.ToFile(envToSnapshot, Path.Combine(_tempDir, xi + ".snapshot"));
                }
            }

            var incremental = new IncrementalBackup();

            var restoredOptions = StorageEnvironmentOptions.ForPath(Path.Combine(_tempDir, "restored"));
            incremental.Restore(restoredOptions, Enumerable.Range(0, 5).Select(i => Path.Combine(_tempDir, i + ".snapshot")));

            using (var snapshotRestoreEnv = new StorageEnvironment(restoredOptions))
            {
                using (var tx = snapshotRestoreEnv.NewTransaction(TransactionFlags.Read))
                {
                    var tree = tx.ReadTree("test");
                    Assert.NotNull(tree);

                    for (int i = 0; i < UserCount; i++)
                    {
                        var readResult = tree.Read("users/" + i);
                        Assert.NotNull(readResult);
                        Assert.Equal("john doe/" + i, readResult.Reader.ToStringValue());
                    }
                }
            }
        }

        [PrefixesFact]
        public unsafe void Min_inc_backup_is_smaller_than_normal_inc_backup()
        {
            const int UserCount = 5000;
            _tempDir = Guid.NewGuid().ToString();
            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(_tempDir);
            storageEnvironmentOptions.IncrementalBackupEnabled = true;
            using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions))
            {
                for (int xi = 0; xi < 10; xi++)
                {
                    using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var tree = envToSnapshot.CreateTree(tx, "test");

                        for (int i = 0; i < UserCount / 10; i++)
                        {
                            tree.Add("users/" + i, "john doe/" + i);
                        }

                        tx.Commit();
                    }
                }

                var incrementalBackupInfo = envToSnapshot.HeaderAccessor.Get(ptr => ptr->IncrementalBackup);

                var snapshotWriter = new MinimalIncrementalBackup();
                snapshotWriter.ToFile(envToSnapshot, Path.Combine(_tempDir, "1.snapshot"));

                // reset the incremental backup stuff

                envToSnapshot.HeaderAccessor.Modify(ptr => ptr->IncrementalBackup = incrementalBackupInfo);

                var incBackup = new IncrementalBackup();
                incBackup.ToFile(envToSnapshot, Path.Combine(_tempDir, "2.snapshot"), CancellationToken.None);

                var incLen = new FileInfo(Path.Combine(_tempDir, "2.snapshot")).Length;
                var minInLen = new FileInfo(Path.Combine(_tempDir, "1.snapshot")).Length;

                Assert.True(incLen > minInLen);
            }
        }


        [PrefixesFact]
        public void Can_split_merged_transaction_to_multiple_tx()
        {
            _tempDir = Guid.NewGuid().ToString();
            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(_tempDir);
            storageEnvironmentOptions.IncrementalBackupEnabled = true;
            storageEnvironmentOptions.MaxNumberOfPagesInMergedTransaction = 8;
            using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions))
            {
                for (int xi = 0; xi < 100; xi++)
                {
                    using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var tree = envToSnapshot.CreateTree(tx, "test");

                        for (int i = 0; i < 1000; i++)
                        {
                            tree.Add("users/" + i, "john doe/" + i);
                        }

                        tx.Commit();
                    }
                }

                var snapshotWriter = new MinimalIncrementalBackup();
                var backupPath = Path.Combine(_tempDir, "1.snapshot");
                snapshotWriter.ToFile(envToSnapshot, backupPath);

                using (var stream = File.OpenRead(backupPath))
                using (var zip = new ZipArchive(stream, ZipArchiveMode.Read))
                {
                    Assert.True(zip.Entries.Count > 1);
                }
            }
        }

        [PrefixesFact]
        public void Mixed_small_and_overflow_changes()
        {
            _tempDir = Guid.NewGuid().ToString();
            var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(_tempDir);
            storageEnvironmentOptions.IncrementalBackupEnabled = true;
            using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions))
            {
                using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = envToSnapshot.CreateTree(tx, "test");
                    tree.Add("users/1", "john doe");
                    tree.Add("users/2", new String('a', 5000));

                    tx.Commit();
                }

                using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var tree = envToSnapshot.CreateTree(tx, "test");
                    tree.Add("users/2", "jane darling");
                    tree.Add("users/3", new String('b', 5000));

                    tx.Commit();
                }

                var snapshotWriter = new MinimalIncrementalBackup();
                snapshotWriter.ToFile(envToSnapshot, Path.Combine(_tempDir, "1.snapshot"));

                var restoredOptions = StorageEnvironmentOptions.ForPath(Path.Combine(_tempDir, "restored"));
                new IncrementalBackup().Restore(restoredOptions, new[] { Path.Combine(_tempDir, "1.snapshot") });

                using (var snapshotRestoreEnv = new StorageEnvironment(restoredOptions))
                {
                    using (var tx = snapshotRestoreEnv.NewTransaction(TransactionFlags.Read))
                    {
                        var tree = tx.ReadTree("test");
                        Assert.NotNull(tree);

                        Assert.Equal("john doe", tree.Read("users/1").Reader.ToStringValue());
                        Assert.Equal("jane darling", tree.Read("users/2").Reader.ToStringValue());
                        Assert.Equal(new String('b', 5000), tree.Read("users/3").Reader.ToStringValue());
                    }
                }
            }
        }

        [PrefixesFact]
        public void Can_write_minimal_incremental_backup()
        {
            _tempDir = Guid.NewGuid().ToString();
            Directory.CreateDirectory(_tempDir);

            var snapshotWriter = new MinimalIncrementalBackup();
            snapshotWriter.ToFile(Env, Path.Combine(_tempDir, "1.snapshot"));

            Assert.True(File.Exists(Path.Combine(_tempDir, "1.snapshot")), " Even empty minimal backup should create a file");

            var snapshotFileInfo = new FileInfo(Path.Combine(_tempDir, "1.snapshot"));
            Assert.True(snapshotFileInfo.Length > 0, " Even empty minimal backup should create a file with some information");
        }
    }
}
