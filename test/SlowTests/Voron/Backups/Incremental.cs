// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests.Voron.Backups;
using Raven.Server.Utils;
using Voron;
using Voron.Global;
using Voron.Impl.Backup;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Backups
{
    public class Incremental : FastTests.Voron.StorageTest
    {
        public Incremental(ITestOutputHelper output) : base(output)
        {
        }

        private readonly IncrementalBackupTestUtils _incrementalBackupTestUtils = new IncrementalBackupTestUtils();
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 1000 * Constants.Storage.PageSize;
            options.IncrementalBackupEnabled = true;
            options.ManualFlushing = true;
        }

        [Fact]
        public void CanBackupAndRestoreOnEmptyStorage()
        {
            RequireFileBasedPager();

            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 500; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            long nextPageNumberBeforeBackup = Env.NextPageNumber;

            BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(0));

            var options = StorageEnvironmentOptions.ForPath(_incrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[] { _incrementalBackupTestUtils.IncrementalBackupFile(0) });

            using (var env = new StorageEnvironment(options))
            {
                Assert.Equal(nextPageNumberBeforeBackup, env.NextPageNumber);

                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < 500; i++)
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

        [Fact]
        public void CanDoMultipleIncrementalBackupsAndRestoreOneByOne()
        {
            RequireFileBasedPager();

            var random = new Random();
            var buffer = new byte[1024];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 300; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(0));

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 300; i < 600; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(1));

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 600; i < 1000; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            Env.FlushLogToDataFile(); // make sure that incremental backup will work even if we flushed journals to the data file

            BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(2));

            var options = StorageEnvironmentOptions.ForPath(_incrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                _incrementalBackupTestUtils.IncrementalBackupFile(0),
                _incrementalBackupTestUtils.IncrementalBackupFile(1),
                _incrementalBackupTestUtils.IncrementalBackupFile(2)
            });

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < 1000; i++)
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

        [Fact]
        public void IncrementalBackupShouldCopyJustNewPagesSinceLastBackup()
        {
            RequireFileBasedPager();
            var random = new Random();
            var buffer = new byte[100];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 5; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            var usedPagesInJournal = Env.Journal.CurrentFile.GetWritePosIn4KbPosition(Env.CurrentStateRecord);

            var backedUpPages = BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(0));

            Assert.Equal(usedPagesInJournal, backedUpPages);

            var writePos = Env.Journal.CurrentFile.GetWritePosIn4KbPosition(Env.CurrentStateRecord);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 5; i < 10; i++)
                {
                    tree.Add("items/" + i, new MemoryStream(buffer));
                }

                tx.Commit();
            }

            var usedByLastTransaction = Env.Journal.CurrentFile.GetWritePosIn4KbPosition(Env.CurrentStateRecord) - writePos;

            backedUpPages = BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(1));

            Assert.Equal(usedByLastTransaction, backedUpPages);

            var options = StorageEnvironmentOptions.ForPath(_incrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                _incrementalBackupTestUtils.IncrementalBackupFile(0),
                _incrementalBackupTestUtils.IncrementalBackupFile(1)
            });

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < 10; i++)
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

        [Fact]
        public void IncrementalBackupShouldAcceptEmptyIncrementalBackups()
        {
            RequireFileBasedPager();
            var buffer = "0-1-2-3-4-5-6-7-8-9-10-11-12-13-14-15-16-17-18-19-20-21-22-23-24-25-26-27-28-29-30-31-32-33-34-35-36"u8;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 5; i++)
                {
                    tree.Add("items/" + i,buffer.ToArray());
                }

                tx.Commit();
            }

            var usedPagesInJournal = Env.Journal.CurrentFile.GetWritePosIn4KbPosition(Env.CurrentStateRecord);

            var backedUpPages = BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(0));

            Assert.Equal(usedPagesInJournal, backedUpPages);

            // We don't modify anything between backups - to create empty incremental backup

            var writePos = Env.Journal.CurrentFile.GetWritePosIn4KbPosition(Env.CurrentStateRecord);

            var usedByLastTransaction = Env.Journal.CurrentFile.GetWritePosIn4KbPosition(Env.CurrentStateRecord) - writePos;
            Assert.Equal(0, usedByLastTransaction);

            long nextPageNumberBeforeBackup = Env.NextPageNumber;

            backedUpPages = BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(1));

            Assert.Equal(usedByLastTransaction, backedUpPages);

            var options = StorageEnvironmentOptions.ForPath(_incrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                _incrementalBackupTestUtils.IncrementalBackupFile(0),
                _incrementalBackupTestUtils.IncrementalBackupFile(1)
            });

            using (var env = new StorageEnvironment(options))
            {
                Assert.Equal(nextPageNumberBeforeBackup, env.NextPageNumber);

                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < 5; i++)
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

        [Fact]
        public void IncorrectWriteOfOverflowPagesFromJournalsToDataFile_RavenDB_2806()
        {
            RequireFileBasedPager();

            const int testedOverflowSize = 20000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);


            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");

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

            long nextPageNumberBeforeBackup = Env.NextPageNumber;

            BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(0));

            var options = StorageEnvironmentOptions.ForPath(_incrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                _incrementalBackupTestUtils.IncrementalBackupFile(0)
            });

            using (var env = new StorageEnvironment(options))
            {
                Assert.Equal(nextPageNumberBeforeBackup, env.NextPageNumber);

                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.ReadTree("test");

                    var readResult = tree.Read("items/3");

                    var readBytes = new byte[testedOverflowSize];

                    readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                    Assert.Equal(overflowValue, readBytes);
                }
            }
        }

        [Fact]
        public void IncorrectWriteOfOverflowPagesFromJournalsToDataFile_2_RavenDB_2806()
        {
            RequireFileBasedPager();

            const int testedOverflowSize = 16000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);


            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");

                var itemBytes = new byte[2000];

                new Random(2).NextBytes(itemBytes);
                tree.Add("items/1", itemBytes);


                itemBytes = new byte[30000];
                new Random(3).NextBytes(itemBytes);
                tree.Add("items/2", itemBytes);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");
                tree.Delete("items/1");
                tree.Delete("items/2");

                tree.Add("items/3", overflowValue);

                tx.Commit();
            }

            BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(0));

            var options = StorageEnvironmentOptions.ForPath(_incrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                _incrementalBackupTestUtils.IncrementalBackupFile(0)
            });

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.ReadTree("test");

                    var readResult = tree.Read("items/3");

                    var readBytes = new byte[testedOverflowSize];

                    readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                    Assert.Equal(overflowValue, readBytes);
                }
            }
        }

        [Fact]
        public void IncorrectWriteOfOverflowPagesFromJournalsInBackupToDataFile_RavenDB_2891()
        {
            RequireFileBasedPager();

            const int testedOverflowSize = 50000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");

                var itemBytes = new byte[30000];

                new Random(2).NextBytes(itemBytes);
                tree.Add("items/1", itemBytes);

                new Random(3).NextBytes(itemBytes);
                tree.Add("items/2", itemBytes);

                tree.Delete("items/1");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("test");

                tree.Delete("items/2");

                tree.Add("items/3", overflowValue);

                tx.Commit();
            }

            BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(0));

            var options = StorageEnvironmentOptions.ForPath(_incrementalBackupTestUtils.RestoredStoragePath);
            options.MaxLogFileSize = Env.Options.MaxLogFileSize;

            BackupMethods.Incremental.Restore(options, new[]
            {
                _incrementalBackupTestUtils.IncrementalBackupFile(0)
            });

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.ReadTree("test");

                    var readResult = tree.Read("items/3");

                    var readBytes = new byte[testedOverflowSize];

                    readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                    Assert.Equal(overflowValue, readBytes);
                }
            }
        }

        [Fact]
        public void IncrementalBackupShouldCreateConsecutiveJournalFiles()
        {
            IOExtensions.DeleteDirectory(DataDir);

            Options = StorageEnvironmentOptions.ForPath(DataDir);
            Options.MaxLogFileSize = Constants.Storage.PageSize;
            Options.IncrementalBackupEnabled = true;
            Options.ManualFlushing = true;

            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);

            for (int j = 0; j < 10; j++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = j * 50; i < (j + 1) * 50; i++)
                    {
                        tree.Add("items/" + i, new MemoryStream(buffer));
                    }

                    tx.Commit();
                }

                BackupMethods.Incremental.ToFile(Env, _incrementalBackupTestUtils.IncrementalBackupFile(j));

            }

            // Verify that data is restored

            var options = StorageEnvironmentOptions.ForPath(_incrementalBackupTestUtils.RestoredStoragePath);
            var backupFiles = Enumerable.Range(0, 10).Select(n => _incrementalBackupTestUtils.IncrementalBackupFile(n));

            BackupMethods.Incremental.Restore(options, backupFiles);

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < 500; i++)
                    {
                        var readResult = tree.Read("items/" + i);
                        Assert.NotNull(readResult);
                        var memoryStream = new MemoryStream();
                        readResult.Reader.CopyTo(memoryStream);
                        Assert.Equal(memoryStream.ToArray(), buffer);
                    }
                }
            }

            // Verify that journal files numbering does not contain any gaps

            var journalsPath = Path.Combine(DataDir, "Journals");
            var files = Directory.GetFiles(journalsPath, "*.journal*", SearchOption.AllDirectories);

            Assert.True(files.Length >= 10);
            var list = new List<int>();
            foreach (var file in files)
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                int.TryParse(fileName, out var num);
                list.Add(num);
            }

            for (int i = 0; i < list.Count; i++)
            {
                Assert.Contains(i, list);
            }

        }

        public override void Dispose()
        {
            base.Dispose();
            _incrementalBackupTestUtils.Dispose();
        }
    }
}
