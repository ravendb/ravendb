using System;
using System.IO;
using Voron;
using Voron.Exceptions;
using Xunit;

namespace SlowTests.Voron
{
    public class RecoveryMultipleJournals : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 10 * options.PageSize;
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
                tx.CreateTree( "tree");

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

        [Fact]
        public void CanResetLogInfoAfterBigUncommitedTransaction2()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree( "tree");

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

            Assert.Equal(currentJournalInfo.CurrentJournal +1, Env.Journal.GetCurrentJournalInfo().CurrentJournal);
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
                for (var i = 0; i < 1000; i++)
                {
                    tx.CreateTree("tree").Add("a" + i, new MemoryStream(new byte[100]));
                }
                tx.Commit();
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;
            
            StopDatabase();
            
            CorruptPage(lastJournal, page: 6, pos: 3);

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
        public void CanResetLogInfoAfterBigUncommitedTransactionWithRestart2()
        {
            RequireFileBasedPager();
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree( "tree");

                tx.Commit();
            }

            Random rnd = new Random();

            var buffer = new byte[100];
            rnd.NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                for (var i = 0; i < 1000; i++)
                {                   
                    tx.CreateTree("tree").Add("a" + i, new MemoryStream(buffer));
                }
                tx.Commit();
            }

            var currentJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            using (var tx = Env.WriteTransaction())
            {
                for (var i = 0; i < 1000; i++)
                {
                    rnd.NextBytes(buffer);
                    tx.CreateTree("tree").Add("b" + i, new MemoryStream(buffer));
                }
                tx.Commit();
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            StopDatabase();

            CorruptPage(lastJournal - 1, page: 3, pos: 3);

            StartDatabase();
            Assert.Equal(currentJournal, Env.Journal.GetCurrentJournalInfo().CurrentJournal);
        }


        [Fact]
        public void CorruptingOneTransactionWillKillAllFutureTransactions()
        {
            RequireFileBasedPager();
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree( "tree");

                tx.Commit();
            }

            for (int i = 0; i < 1000; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("a" + i, new MemoryStream(new byte[100]));
                    tx.Commit();
                }
            }

            var lastJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;
            var lastJournalPosition = Env.Journal.CurrentFile.WritePagePosition;

            StopDatabase();

            CorruptPage(lastJournal - 3, lastJournalPosition + 1, 5);

            StartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Assert.Null(tx.CreateTree("tree").Read("a999"));
            }

        }

        private void CorruptPage(long journal, long page, int pos)
        {
            _options.Dispose();
            _options = StorageEnvironmentOptions.ForPath(DataDir);
            Configure(_options);
            using (var fileStream = new FileStream(
                Path.Combine(DataDir, StorageEnvironmentOptions.JournalName(journal)), 
                FileMode.Open,
                FileAccess.ReadWrite, 
                FileShare.ReadWrite | FileShare.Delete))
            {
                fileStream.Position = page*_options.PageSize;

                var buffer = new byte[_options.PageSize];

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

                buffer[pos] = 42;
                fileStream.Position = page * _options.PageSize;
                fileStream.Write(buffer, 0, buffer.Length);
            }
        }

        [Fact]
        public void ShouldThrowIfFirstTransactionIsCorruptedBecauseWeCannotAccessMetadataThen()
        {
            RequireFileBasedPager();

            var currentJournal = Env.Journal.GetCurrentJournalInfo().CurrentJournal;

            StopDatabase();

            CorruptPage(currentJournal, page: 2, pos: 3);

            Assert.Throws<VoronUnrecoverableErrorException>(() => StartDatabase());
        }
    }
}
