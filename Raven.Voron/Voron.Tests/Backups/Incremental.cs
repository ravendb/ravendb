// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading;
using Voron.Impl.Backup;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Backups
{
    public class Incremental : StorageTest
    {

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 1000 * AbstractPager.PageSize;
            options.IncrementalBackupEnabled = true;
            options.ManualFlushing = true;
        }

        public Incremental()
        {
            IncrementalBackupTestUtils.Clean();
        }

        [PrefixesFact]
        public void CanBackupAndRestoreOnEmptyStorage()
        {
            RequireFileBasedPager();

            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 500; i++)
                {
                    tx.Root.Add			("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(0), CancellationToken.None);

            var options = StorageEnvironmentOptions.ForPath(IncrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[] { IncrementalBackupTestUtils.IncrementalBackupFile(0) });

            using (var env = new StorageEnvironment(options))
            {

                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    for (int i = 0; i < 500; i++)
                    {
                        var readResult = tx.Root.Read("items/" + i);
                        Assert.NotNull(readResult);
                        var memoryStream = new MemoryStream();
                        readResult.Reader.CopyTo(memoryStream);
                        Assert.Equal(memoryStream.ToArray(), buffer);
                    }
                }
            }
        }

        [PrefixesFact]
        public void CanDoMultipleIncrementalBackupsAndRestoreOneByOne()
        {
            RequireFileBasedPager();

            var random = new Random();
            var buffer = new byte[1024];
            random.NextBytes(buffer);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 300; i++)
                {
                    tx.Root.Add			("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(0), CancellationToken.None);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 300; i < 600; i++)
                {
                    tx.Root.Add			("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(1), CancellationToken.None);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 600; i < 1000; i++)
                {
                    tx.Root.Add			("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            Env.FlushLogToDataFile(); // make sure that incremental backup will work even if we flushed journals to the data file

            BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(2), CancellationToken.None);

            var options = StorageEnvironmentOptions.ForPath(IncrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                IncrementalBackupTestUtils.IncrementalBackupFile(0),
                IncrementalBackupTestUtils.IncrementalBackupFile(1),
                IncrementalBackupTestUtils.IncrementalBackupFile(2)
            });

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        var readResult = tx.Root.Read("items/" + i);
                        Assert.NotNull(readResult);
                        var memoryStream = new MemoryStream();
                        readResult.Reader.CopyTo(memoryStream);
                        Assert.Equal(memoryStream.ToArray(), buffer);
                    }
                }
            }
        }

        [PrefixesFact]
        public void IncrementalBackupShouldCopyJustNewPagesSinceLastBackup()
        {
            RequireFileBasedPager();
            var random = new Random();
            var buffer = new byte[100];
            random.NextBytes(buffer);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 5; i++)
                {
                    tx.Root.Add			("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            var usedPagesInJournal = Env.Journal.CurrentFile.WritePagePosition;

            var backedUpPages = BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(0), CancellationToken.None);

            Assert.Equal(usedPagesInJournal, backedUpPages);

            var writePos = Env.Journal.CurrentFile.WritePagePosition;
        
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 5; i < 10; i++)
                {
                    tx.Root.Add			("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            var usedByLastTransaction = Env.Journal.CurrentFile.WritePagePosition - writePos;

            backedUpPages = BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(1), CancellationToken.None);

            Assert.Equal(usedByLastTransaction, backedUpPages);

            var options = StorageEnvironmentOptions.ForPath(IncrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                IncrementalBackupTestUtils.IncrementalBackupFile(0),
                IncrementalBackupTestUtils.IncrementalBackupFile(1)
            });

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var readResult = tx.Root.Read("items/" + i);
                        Assert.NotNull(readResult);
                        var memoryStream = new MemoryStream();
                        readResult.Reader.CopyTo(memoryStream);
                        Assert.Equal(memoryStream.ToArray(), buffer);
                    }
                }
            }
        }

        [PrefixesFact]
        public void IncrementalBackupShouldAcceptEmptyIncrementalBackups()
        {
            RequireFileBasedPager();
            var random = new Random();
            var buffer = new byte[100];
            random.NextBytes(buffer);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                for (int i = 0; i < 5; i++)
                {
                    tx.Root.Add			("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            var usedPagesInJournal = Env.Journal.CurrentFile.WritePagePosition;

            var backedUpPages = BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(0), CancellationToken.None);

            Assert.Equal(usedPagesInJournal, backedUpPages);

            // We don't modify anything between backups - to create empty incremental backup

            var writePos = Env.Journal.CurrentFile.WritePagePosition;

            var usedByLastTransaction = Env.Journal.CurrentFile.WritePagePosition - writePos;
            Assert.Equal(0, usedByLastTransaction);

            backedUpPages = BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(1), CancellationToken.None);

            Assert.Equal(usedByLastTransaction, backedUpPages);

            var options = StorageEnvironmentOptions.ForPath(IncrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                IncrementalBackupTestUtils.IncrementalBackupFile(0),
                IncrementalBackupTestUtils.IncrementalBackupFile(1)
            });

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var readResult = tx.Root.Read("items/" + i);
                        Assert.NotNull(readResult);
                        var memoryStream = new MemoryStream();
                        readResult.Reader.CopyTo(memoryStream);
                        Assert.Equal(memoryStream.ToArray(), buffer);
                    }
                }
            }
        }

        [PrefixesFact]
        public void IncorrectWriteOfOverflowPagesFromJournalsToDataFile_RavenDB_2806()
        {
            RequireFileBasedPager();

            const int testedOverflowSize = 20000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);


            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "test");

                var itemBytes = new byte[16000];

                new Random(2).NextBytes(itemBytes);
                tree.Add("items/1", itemBytes);

                new Random(3).NextBytes(itemBytes);
                tree.Add("items/2", itemBytes);

                tree.Delete("items/1");
                tree.Delete("items/2");

                tree.Add("items/3", overflowValue);

                tx.Commit();
            }

            BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(0), CancellationToken.None);

            var options = StorageEnvironmentOptions.ForPath(IncrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                IncrementalBackupTestUtils.IncrementalBackupFile(0)
            });

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var tree = tx.ReadTree("test");

                    var readResult = tree.Read("items/3");

                    var readBytes = new byte[testedOverflowSize];

                    readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                    Assert.Equal(overflowValue, readBytes);
                }
            }
        }

        [PrefixesFact]
        public void IncorrectWriteOfOverflowPagesFromJournalsToDataFile_2_RavenDB_2806()
        {
            RequireFileBasedPager();

            const int testedOverflowSize = 16000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);


            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "test");

                var itemBytes = new byte[2000];

                new Random(2).NextBytes(itemBytes);
                tree.Add("items/1", itemBytes);


                itemBytes = new byte[30000];
                new Random(3).NextBytes(itemBytes);
                tree.Add("items/2", itemBytes);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "test");
                tree.Delete("items/1");
                tree.Delete("items/2");

                tree.Add("items/3", overflowValue);

                tx.Commit();
            }

            BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(0), CancellationToken.None);

            var options = StorageEnvironmentOptions.ForPath(IncrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                IncrementalBackupTestUtils.IncrementalBackupFile(0)
            });

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var tree = tx.ReadTree("test");

                    var readResult = tree.Read("items/3");

                    var readBytes = new byte[testedOverflowSize];

                    readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                    Assert.Equal(overflowValue, readBytes);
                }
            }
        }

        [PrefixesFact]
        public void IncorrectWriteOfOverflowPagesFromJournalsInBackupToDataFile_RavenDB_2891()
        {
            RequireFileBasedPager();

            const int testedOverflowSize = 50000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "test");

                var itemBytes = new byte[30000];

                new Random(2).NextBytes(itemBytes);
                tree.Add("items/1", itemBytes);

                new Random(3).NextBytes(itemBytes);
                tree.Add("items/2", itemBytes);

                tree.Delete("items/1");

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("test");

                tree.Delete("items/2");

                tree.Add("items/3", overflowValue);

                tx.Commit();
            }

            BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(0), CancellationToken.None);

            var options = StorageEnvironmentOptions.ForPath(IncrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                IncrementalBackupTestUtils.IncrementalBackupFile(0)
            });

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var tree = tx.ReadTree("test");

                    var readResult = tree.Read("items/3");

                    var readBytes = new byte[testedOverflowSize];

                    readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                    Assert.Equal(overflowValue, readBytes);
                }
            }
        }

        public override void Dispose()
        {
            base.Dispose();
            IncrementalBackupTestUtils.Clean();
        }	
    }
}
