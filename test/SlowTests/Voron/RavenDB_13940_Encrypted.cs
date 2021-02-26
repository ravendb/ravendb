using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron
{
    public class RavenDB_13940_Encrypted : StorageTest
    {
        public RavenDB_13940_Encrypted(ITestOutputHelper output) : base(output)
        {
        }

        private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
        private bool _onIntegrityErrorOfAlreadySyncedDataHandlerWasCalled;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.InitialLogFileSize = 4 * Constants.Size.Megabyte;
            options.OnIntegrityErrorOfAlreadySyncedData += (sender, args) =>
            {
                _onIntegrityErrorOfAlreadySyncedDataHandlerWasCalled = true;
            }; // just shut it up
            options.ManualSyncing = true;
            options.ManualFlushing = true;
            options.MaxScratchBufferSize = 1 * 1024 * 1024 * 1024;
            options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = true;
            options.Encryption.MasterKey = _masterKey.ToArray();
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

            CorruptJournal(lastJournal, 4 * Constants.Size.Kilobyte * 4);

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

            CorruptJournal(lastJournal, Constants.Size.Kilobyte * 4 + (int)Marshal.OffsetOf<TransactionHeader>(nameof(TransactionHeader.LastPageNumber)), 4, 0);

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
                operation.SyncDataFile();
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

            CorruptJournal(lastJournal, sizeof(TransactionHeader) + 5, Constants.Size.Kilobyte * 4 * 6);

            Assert.Throws<InvalidJournalException>(StartDatabase);
        }

        private void CorruptJournal(long journal, long position, int numberOfCorruptedBytes = Constants.Size.Kilobyte * 4, byte value = 42)
        {
            Options.Dispose();
            Options = StorageEnvironmentOptions.ForPath(DataDir);
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
                    if (buffer[i] != value)
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
