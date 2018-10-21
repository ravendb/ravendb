using System;
using System.IO;
using FastTests.Voron;
using Sparrow;
using Sparrow.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Exceptions;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron
{
    public class RecoveryMultipleJournals : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 10 * Constants.Storage.PageSize;
            options.OnRecoveryError += (sender, args) => { }; // just shut it up
            options.ManualFlushing = true;
            options.MaxScratchBufferSize = 1 * 1024 * 1024 * 1024;
        }

        [Fact]
        public void CanRecoverAfterRestartWithMultipleFilesInSingleTransaction()
        {

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }
            using (var tx = Env.WriteTransaction())
            {
                for (var i = 0; i < 1000; i++)
                {
                    tx.CreateTree("tree").Add("a" + i, new MemoryStream(new byte[100]));
                }
                tx.Commit();
            }


            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                for (var i = 0; i < 1000; i++)
                {
                    var readResult = tx.CreateTree("tree").Read("a" + i);
                    Assert.NotNull(readResult);
                    {
                        Assert.Equal(100, readResult.Reader.Length);
                    }
                }
                tx.Commit();
            }
        }

        [Fact]
        public void CanResetLogInfoAfterBigUncommitedTransaction()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            var currentJournalInfo = Env.Journal.GetCurrentJournalInfo();

            using (var tx = Env.WriteTransaction())
            {
                for (var i = 0; i < 1000; i++)
                {
                    tx.CreateTree("tree").Add("a" + i, new MemoryStream(new byte[100]));
                }
                //tx.Commit(); - not committing here
            }

            Assert.Equal(currentJournalInfo.CurrentJournal, Env.Journal.GetCurrentJournalInfo().CurrentJournal);

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("a", new MemoryStream(new byte[100]));
                tx.Commit();
            }

            Assert.Equal(currentJournalInfo.CurrentJournal, Env.Journal.GetCurrentJournalInfo().CurrentJournal);
        }

        [Fact64Bit]
        public void CanResetLogInfoAfterBigUncommitedTransaction2()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                for (var i = 0; i < 1000; i++)
                {
                    tx.CreateTree("tree").Add("a" + i, new MemoryStream(new byte[100]));
                }
                tx.Commit();
            }

            var currentJournalInfo = Env.Journal.GetCurrentJournalInfo();

            var random = new Random();
            var buffer = new byte[1000000];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                for (var i = 0; i < 1000; i++)
                {
                    tx.CreateTree("tree").Add("b" + i, new MemoryStream(buffer));
                }
                //tx.Commit(); - not committing here
            }

            Assert.Equal(currentJournalInfo.CurrentJournal, Env.Journal.GetCurrentJournalInfo().CurrentJournal);

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("b", new MemoryStream(buffer));
                tx.Commit();
            }

            Assert.Equal(currentJournalInfo.CurrentJournal + 1, Env.Journal.GetCurrentJournalInfo().CurrentJournal);
        }

        [Fact]
        public void CanResetLogInfoAfterBigUncommitedTransactionWithRestart()
        {
            RequireFileBasedPager();
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("exists", new MemoryStream(new byte[100]));

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var random = new Random();
                for (var i = 0; i < 1000; i++)
                {
                    var buffer = new byte[100];
                    random.NextBytes(buffer);
                    tx.CreateTree("tree").Add("a" + i, new MemoryStream(buffer));
                }
                tx.Commit();
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            StopDatabase();

            CorruptJournal(lastJournal, posOf4KbInJrnl: 6);

            StartDatabase();
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree");
                Assert.NotNull(tree.Read("exists"));
                Assert.Null(tree.Read("a1"));
                Assert.Null(tree.Read("a100"));
                Assert.Null(tree.Read("a500"));
                Assert.Null(tree.Read("a1000"));

                tx.Commit();
            }
        }

        [Fact]
        public void CorruptingOneTransactionWillThrow()
        {
            RequireFileBasedPager();
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            for (int i = 0; i < 1001; i++)
            {

                var buffer = new byte[100];
                new Random().NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("a" + i, new MemoryStream(buffer));
                    tx.Commit();
                }
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;
            var lastJournalPosition = Env.Journal.CurrentFile.WritePosIn4KbPosition;

            StopDatabase();

            CorruptJournal(lastJournal - 3, lastJournalPosition + 1);

            Assert.Throws<InvalidDataException>(() => StartDatabase());
        }

        [Fact]
        public void CorruptingAllLastTransactionsConsideredAsEndOfJournal()
        {
            RequireFileBasedPager();
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            for (int i = 0; i < 991; i++)
            {

                var buffer = new byte[100];
                new Random(i).NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("a" + i, new MemoryStream(buffer));
                    tx.Commit();
                }
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;
            var lastJournalPosition = Env.Journal.CurrentFile.WritePosIn4KbPosition;


            StopDatabase();

            Assert.True(Env.Journal.CurrentFile.Available4Kbs - lastJournalPosition > 0);

            for (var pos = lastJournalPosition - 2;
                pos < lastJournalPosition + 1;
                pos++)
            {
                CorruptJournal(lastJournal, pos);
            }

            StartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Assert.Null(tx.CreateTree("tree").Read("a1001"));
            }

        }

        [Fact]
        public void CorruptingLastTransactionsInNotLastJournalShouldThrow()
        {
            RequireFileBasedPager();
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            for (int i = 0; i < 1002; i++)
            {

                var buffer = new byte[100];
                new Random().NextBytes(buffer);
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("a" + i, new MemoryStream(buffer));
                    tx.Commit();
                }
            }

            var middleJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal/2;
            var lastJournalPosition = Env.Journal.CurrentFile.WritePosIn4KbPosition;


            StopDatabase();

            for (var pos = lastJournalPosition - 3;
                pos < lastJournalPosition + Env.Journal.CurrentFile.Available4Kbs; // the current journal info applies also for a middle one
                pos++)
            {
                CorruptJournal(middleJournal, pos);
            }

            Assert.Throws<InvalidDataException>(() => StartDatabase());
        }

        private void CorruptJournal(long journal, long posOf4KbInJrnl)
        {
            Options.Dispose();
            Options = StorageEnvironmentOptions.ForPath(DataDir);
            Configure(Options);
            using (var fileStream = SafeFileStream.Create(Options.GetJournalPath(journal).FullPath,
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.ReadWrite | FileShare.Delete))
            {
                fileStream.Position = posOf4KbInJrnl * Constants.Size.Kilobyte * 4;

                var buffer = new byte[Constants.Size.Kilobyte * 4];

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
                    buffer[i] = 42;
                }
                fileStream.Position = posOf4KbInJrnl * Constants.Size.Kilobyte * 4;
                fileStream.Write(buffer, 0, buffer.Length);
            }
        }

        [Fact]
        public void ShouldThrowIfFirstTransactionIsCorruptedBecauseWeCannotAccessMetadataThen()
        {
            RequireFileBasedPager();

            var currentJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            StopDatabase();

            CorruptJournal(currentJournal, posOf4KbInJrnl: 0);

            Assert.Throws<VoronUnrecoverableErrorException>(() => StartDatabase());
        }
    }
}
