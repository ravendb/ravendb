using System;
using System.IO;
using FastTests.Voron;
using Voron;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_17194 : StorageTest
    {
        public RavenDB_17194(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = 65536;

            // we set those options in RavenDB
            options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = true;
            options.OnIntegrityErrorOfAlreadySyncedData += (sender, eventArgs) =>
            {
                // ignore in tests, in RavenDB we put an alert in that case, but it doesn't prevent the startup process from proceeding
            };
        }

        [Fact]
        public void MustNotReadAndProceedWithRecyclableButEffectivelyEmptyJournalOnRecovery()
        {
            RequireFileBasedPager();

            long lastCommittedTxId = -1;

            var r = new Random(10_09_2021);

            for (int i = 0; i < 20; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");

                    tree.Add($"items/{i}", new byte[]{ 1, 2, 3, (byte)i });

                    tx.Commit();

                    lastCommittedTxId = tx.LowLevelTransaction.Id;
                }
            }

            Assert.Equal(2, Env.Journal.Files.Count);

            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            Assert.Equal(1, Env.Journal.Files.Count);

            var journalPath = ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).JournalPath.FullPath;


            var journalsForReuse = new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*");

            Assert.Equal(1, journalsForReuse.Length);


            using (var tx = Env.WriteTransaction())
            {
                // we are writing big values in this tx to ensure we'll have NextFile() call that will create a new journal (based on the recyclable journal file that exists)

                for (int i = 0; i < 100; i++)
                {
                    var bytes = new byte[2000];

                    r.NextBytes(bytes);

                    tx.CreateTree("bar").Add($"bigValues/{i}", bytes);
                }

                Assert.Throws<InvalidOperationException>(() =>
                {
                    using (tx.LowLevelTransaction.ForTestingPurposesOnly().CallJustBeforeWritingToJournal(() => throw new InvalidOperationException()))
                    {
                        tx.Commit();
                    }
                });
            }

            // we failed to commit the above transaction and write data to file but we managed to create _empty_ journal file
            // this journal was created based on the recyclable journal which had old transactions already there that causes problems during recovery below

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(lastCommittedTxId + 1, tx.LowLevelTransaction.Id);
            }
        }


        [Fact]
        public void TheCaseWithTwoEmptyJournalFiles()
        {
            RequireFileBasedPager();

            long lastCommittedTxId = -1;

            var r = new Random(10_09_2021);

            for (int i = 0; i < 20; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");

                    tree.Add($"items/{i}", new byte[] { 1, 2, 3, (byte)i });

                    tx.Commit();

                    lastCommittedTxId = tx.LowLevelTransaction.Id;
                }
            }

            Assert.Equal(2, Env.Journal.Files.Count);


            var journalPath = ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).JournalPath.FullPath;


            var journalsForReuse = new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*");

            Assert.Equal(0, journalsForReuse.Length);


            using (var tx = Env.WriteTransaction())
            {
                // we are writing big values in this tx to ensure we'll have NextFile() call that will create a new journal (empty file - not recyclable)

                for (int i = 0; i < 100; i++)
                {
                    var bytes = new byte[2000];

                    r.NextBytes(bytes);

                    tx.CreateTree("bar").Add($"bigValues/{i}", bytes);
                }

                Assert.Throws<InvalidOperationException>(() =>
                {
                    using (tx.LowLevelTransaction.ForTestingPurposesOnly().CallJustBeforeWritingToJournal(() => throw new InvalidOperationException()))
                    {
                        tx.Commit();
                    }
                });
            }

            // we failed to commit the above transaction and write data to file but we managed to create _empty_ journal file

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                // we are writing big values in this tx to ensure we'll have NextFile() call that will create a new journal (again - empty file)

                for (int i = 0; i < 100; i++)
                {
                    var bytes = new byte[2000];

                    r.NextBytes(bytes);

                    tx.CreateTree("bar").Add($"bigValues/{i}", bytes);
                }

                Assert.Throws<InvalidOperationException>(() =>
                {
                    using (tx.LowLevelTransaction.ForTestingPurposesOnly().CallJustBeforeWritingToJournal(() => throw new InvalidOperationException()))
                    {
                        tx.Commit();
                    }
                });
            }

            // we failed to commit the above transaction and write data to file but we managed to create _empty_ journal file
            // at this point we have 2 empty journal files and that caused Debug.Assert() to fail in WriteAheadJournal.RecoverDatabase()

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(lastCommittedTxId + 1, tx.LowLevelTransaction.Id);
            }
        }

        [Fact]
        public void FirstJournalIsEmptyButContainsDataBecauseWasCreatedFromRecyclable()
        {
            RequireFileBasedPager();

            long lastCommittedTxId = -1;

            var r = new Random(10_09_2021);

            for (int i = 0; i < 28; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");

                    tree.Add($"items/{i}", new byte[] { 1, 2, 3, (byte)i });

                    tx.Commit();

                    lastCommittedTxId = tx.LowLevelTransaction.Id;
                }
            }

            Assert.Equal(2, Env.Journal.Files.Count);

            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            var journalPath = ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).JournalPath.FullPath;


            var journalsForReuse = new DirectoryInfo(journalPath).GetFiles($"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}*");

            Assert.Equal(1, journalsForReuse.Length);


            using (var tx = Env.WriteTransaction())
            {
                // we are writing big values in this tx to ensure we'll have NextFile() call that will create a new journal (based on the recyclable journal file that exists)

                for (int i = 0; i < 100; i++)
                {
                    var bytes = new byte[2000];

                    r.NextBytes(bytes);

                    tx.CreateTree("bar").Add($"bigValues/{i}", bytes);
                }

                Assert.Throws<InvalidOperationException>(() =>
                {
                    using (tx.LowLevelTransaction.ForTestingPurposesOnly().CallJustBeforeWritingToJournal(() => throw new InvalidOperationException()))
                    {
                        tx.Commit();
                    }
                });
            }

            // we failed to commit the above transaction and write data to file but we managed to create _empty_ journal file
            // this journal was created based on the recyclable journal which had old transactions already there that caused problems during recovery below

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                // we are writing big values in this tx to ensure we'll have NextFile() call that will create a new journal (empty one)

                for (int i = 0; i < 100; i++)
                {
                    var bytes = new byte[2000];

                    r.NextBytes(bytes);

                    tx.CreateTree("bar").Add($"bigValues/{i}", bytes);
                }

                Assert.Throws<InvalidOperationException>(() =>
                {
                    using (tx.LowLevelTransaction.ForTestingPurposesOnly().CallJustBeforeWritingToJournal(() => throw new InvalidOperationException()))
                    {
                        tx.Commit();
                    }
                });
            }

            // we failed to commit the above transaction and write data to file but we managed to create _empty_ journal file

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            // at this point, the first journal is empty but it has old transactions data because it was created based on recyclable journal file

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(lastCommittedTxId + 1, tx.LowLevelTransaction.Id);
            }
        }
    }
}
