using System;
using System.IO;
using System.IO.Compression;
using System.Runtime.InteropServices;
using FastTests.Voron;
using Sparrow.Utils;
using Voron;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron
{
    public class RavenDB_13940 : StorageTest
    {
        public RavenDB_13940(ITestOutputHelper output) : base(output)
        {
        }

        private bool _onIntegrityErrorOfAlreadySyncedDataHandlerWasCalled;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.InitialLogFileSize = 4 * Constants.Size.Megabyte;
            options.OnIntegrityErrorOfAlreadySyncedData += (sender, args) =>
            {
                _onIntegrityErrorOfAlreadySyncedDataHandlerWasCalled = true;
            };
            options.ManualSyncing = true;
            options.ManualFlushing = true;
            options.MaxScratchBufferSize = 1 * 1024 * 1024 * 1024;
            options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = true;
        }

        [Fact]
        public void CorruptedSingleTransactionPage_WontStopTheRecoveryIfIgnoreErrorsOfSyncedTransactionIsSet()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            var random = new Random(1);

            for (var i = 0; i < 100; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("a" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            // let's flush and sync
            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            for (var i = 0; i < 100; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("b" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            StopDatabase();

            CorruptJournal(lastJournal, 4 * Constants.Size.Kilobyte * 4 - 1000);

            StartDatabase();

            Assert.True(_onIntegrityErrorOfAlreadySyncedDataHandlerWasCalled);

            using (var tx = Env.ReadTransaction())
            {
                for (var i = 0; i < 100; i++)
                {
                    for (int j = 0; j < 100; j++)
                    {
                        var readA = tx.ReadTree("tree").Read("a" + i.ToString() + j.ToString());

                        Assert.NotNull(readA);

                        var readB = tx.ReadTree("tree").Read("b" + i.ToString() + j.ToString());

                        Assert.NotNull(readB);
                    }

                    tx.Commit();
                }
            }
        }

        [Fact]
        public unsafe void CorruptedSingleByteInTransactionPageOfFirstTransaction_WontStopTheRecoveryIfIgnoreErrorsOfSyncedTransactionIsSet()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            var random = new Random(1);

            for (var i = 0; i < 100; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("a" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            // let's flush and sync
            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            for (var i = 0; i < 100; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("b" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            StopDatabase();

            CorruptJournal(lastJournal, sizeof(TransactionHeader) + 5, 1);

            StartDatabase();

            Assert.True(_onIntegrityErrorOfAlreadySyncedDataHandlerWasCalled);

            using (var tx = Env.ReadTransaction())
            {
                for (var i = 0; i < 100; i++)
                {
                    for (int j = 0; j < 100; j++)
                    {
                        var readA = tx.ReadTree("tree").Read("a" + i.ToString() + j.ToString());

                        Assert.NotNull(readA);

                        var readB = tx.ReadTree("tree").Read("b" + i.ToString() + j.ToString());

                        Assert.NotNull(readB);
                    }

                    tx.Commit();
                }
            }
        }

        [Fact]
        public unsafe void CorruptionAcrossMultipleTransactions_WontStopTheRecoveryIfIgnoreErrorsOfSyncedTransactionIsSet()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            var random = new Random(1);

            for (var i = 0; i < 100; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("a" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            // let's flush and sync
            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            for (var i = 0; i < 100; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("b" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            StopDatabase();

            CorruptJournal(lastJournal, sizeof(TransactionHeader) + 5, Constants.Size.Kilobyte * 4 * 2);

            StartDatabase();

            Assert.True(_onIntegrityErrorOfAlreadySyncedDataHandlerWasCalled);

            using (var tx = Env.ReadTransaction())
            {
                for (var i = 0; i < 100; i++)
                {
                    for (int j = 0; j < 100; j++)
                    {
                        var readA = tx.ReadTree("tree").Read("a" + i.ToString() + j.ToString());

                        Assert.NotNull(readA);

                        var readB = tx.ReadTree("tree").Read("b" + i.ToString() + j.ToString());

                        Assert.NotNull(readB);
                    }

                    tx.Commit();
                }
            }
        }

        [Fact]
        public void CorruptionOfTransactionHeaderLastPageNumber_WontStopTheRecoveryIfIgnoreErrorsOfSyncedTransactionIsSet()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            var random = new Random(1);

            for (var i = 0; i < 100; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("a" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            // let's flush and sync
            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile();
            }

            for (var i = 0; i < 100; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("b" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            StopDatabase();

            CorruptJournal(lastJournal, Constants.Size.Kilobyte * 4 + (int)Marshal.OffsetOf<TransactionHeader>(nameof(TransactionHeader.LastPageNumber)), 4, 0, preserveValue: true);

            StartDatabase();

            Assert.True(_onIntegrityErrorOfAlreadySyncedDataHandlerWasCalled);

            using (var tx = Env.ReadTransaction())
            {
                for (var i = 0; i < 100; i++)
                {
                    for (int j = 0; j < 100; j++)
                    {
                        var readA = tx.ReadTree("tree").Read("a" + i.ToString() + j.ToString());

                        Assert.NotNull(readA);

                        var readB = tx.ReadTree("tree").Read("b" + i.ToString() + j.ToString());

                        Assert.NotNull(readB);
                    }

                    tx.Commit();
                }
            }
        }

        [Fact]
        public unsafe void ShouldFailBecauseFirstValidTransactionIsTheOneWhichIsNotSynced()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            var random = new Random(1);

            for (var i = 0; i < 3; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("a" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            // let's flush and sync
            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile(); // this will sync up to tx 5
            }

            for (var i = 0; i < 2; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("b" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            StopDatabase();

            // this is going to corrupt the journal from tx 1 to tx 6 while the last synced tx is 5
            // on startup we expect to get error since tx 6 wasn't synced yet
            CorruptJournal(lastJournal, sizeof(TransactionHeader) + 5, Constants.Size.Kilobyte * 4 * 5); 

            Assert.Throws<InvalidJournalException>(StartDatabase);
        }

        [Fact]
        public unsafe void ShouldNotFailBecauseAllCorruptedTransactionsWereSynced()
        {
            // similar test like above but all corrupted transactions were synced so no errors on startup
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            var random = new Random(1);

            for (var i = 0; i < 3; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("a" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            // let's flush and sync
            Env.FlushLogToDataFile();

            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                operation.SyncDataFile(); // this will sync up to tx 5
            }

            for (var i = 0; i < 2; i++)
            {
                var buffer = new byte[1000];
                random.NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        tx.CreateTree("tree").Add("b" + i.ToString() + j.ToString(), new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            StopDatabase();

            // this is going to corrupt the journal from tx 1 to tx 5
            // the last synced tx is 5 so on startup we'll ignore all corrupted transactions and
            // do the recovery from tx 6
            CorruptJournal(lastJournal, sizeof(TransactionHeader) + 5, Constants.Size.Kilobyte * 4 * 4);

            StartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                for (var i = 0; i < 3; i++)
                {
                    for (int j = 0; j < 100; j++)
                    {
                        var readA = tx.ReadTree("tree").Read("a" + i.ToString() + j.ToString());

                        Assert.NotNull(readA);

                        if (i < 2)
                        {
                            var readB = tx.ReadTree("tree").Read("b" + i.ToString() + j.ToString());

                            Assert.NotNull(readB);
                        }
                    }

                    tx.Commit();
                }
            }
        }

        [Theory]
        [InlineData("storage-with-reused-journal-and-synced-data.zip")]
        [InlineData("storage-with-reused-journal-and-synced-data-2.zip")]
        public void ShouldNotInvokeIntegrityError(string fileName)
        {
            Directory.CreateDirectory(DataDir);

            ExtractFile(DataDir, fileName);

            var options = StorageEnvironmentOptions.ForPath(DataDir);

            options.OnIntegrityErrorOfAlreadySyncedData += (sender, args) =>
            {
                _onIntegrityErrorOfAlreadySyncedDataHandlerWasCalled = true;
            };

            options.ManualSyncing = true;
            options.ManualFlushing = true;
            options.MaxScratchBufferSize = 1 * 1024 * 1024 * 1024;
            options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = true;

            using (var storage = new StorageEnvironment(options))
            {
                Assert.False(_onIntegrityErrorOfAlreadySyncedDataHandlerWasCalled);
            }
        }

        private static void ExtractFile(string directory, string fileName)
        {
            var fullZipPath = Path.Combine(directory, fileName);

            using (var file = File.Create(fullZipPath))
            using (var stream = typeof(RavenDB_13940).Assembly.GetManifestResourceStream($"SlowTests.Data.RavenDB_13940.{fileName}"))
            {
                stream.CopyTo(file);
            }

            ZipFile.ExtractToDirectory(fullZipPath, directory);
        }

        private void CorruptJournal(long journal, long position, int numberOfCorruptedBytes = Constants.Size.Kilobyte * 4, byte value = 42, bool preserveValue = false)
        {
            Options.Dispose();
            Options = StorageEnvironmentOptions.ForPathForTests(DataDir);
            Configure(Options);
            using (var fileStream = SafeFileStream.Create(Options.GetJournalPath(journal).FullPath,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.ReadWrite | FileShare.Delete))
            {
                fileStream.Position = position;

                var buffer = new byte[numberOfCorruptedBytes];

                var remaining = buffer.Length;
                var start = 0;
                while (remaining > 0)
                {
                    var read = fileStream.Read(buffer, start, remaining);
                    if (read == 0)
                        break;
                    start += read;
                    remaining -= read;
                }

                for (int i = 0; i < buffer.Length; i++)
                {
                    if (buffer[i] != value || preserveValue)
                        buffer[i] = value;
                    else
                        buffer[i] = (byte)(value + 1); // we really want to change the original value here so it must not stay the same
                }
                fileStream.Position = position;
                fileStream.Write(buffer, 0, buffer.Length);
            }
        }
    }
}
