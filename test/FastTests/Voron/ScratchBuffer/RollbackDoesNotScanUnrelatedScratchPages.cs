using Tests.Infrastructure;
using Voron;
using Voron.Impl;
using Xunit;

namespace FastTests.Voron.ScratchBuffer
{
    public class RollbackDoesNotScanUnrelatedScratchPages : StorageTest
    {
        public RollbackDoesNotScanUnrelatedScratchPages(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            // the shared scratch pages table only shrinks when the journal is applied to the data file,
            // so manual flushing lets the test hold it at a known size
            options.ManualFlushing = true;
            base.Configure(options);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void A_transaction_that_modified_nothing_examines_no_scratch_pages_when_rolled_back()
        {
            var unflushedScratchPages = PileUpUnflushedScratchPages();

            LowLevelTransaction.TestingStuff testing;

            using (var txw = Env.WriteTransaction())
            {
                testing = txw.LowLevelTransaction.ForTestingPurposesOnly();

                // the shape of a RunIdleOperations cleanup: it takes the write lock, finds nothing to do
                // and is disposed without a commit - which is a rollback
            }

            Assert.True(testing.ScratchPagesExaminedDuringRollback == 0,
                $"rolling back a transaction that modified nothing examined {testing.ScratchPagesExaminedDuringRollback} scratch pages, " +
                $"which is the {unflushedScratchPages} pages every write transaction since the last flush left behind. " +
                "Rollback holds the write transaction lock, so its cost has to be bounded by the transaction being rolled back.");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Rollback_examines_only_the_scratch_pages_that_the_transaction_itself_allocated()
        {
            var unflushedScratchPages = PileUpUnflushedScratchPages();

            LowLevelTransaction.TestingStuff testing;
            int ownScratchPages;

            using (var txw = Env.WriteTransaction())
            {
                var llt = txw.LowLevelTransaction;

                testing = llt.ForTestingPurposesOnly();

                txw.CreateTree("foo").Add("rolled-back", new string('r', 20000));

                ownScratchPages = llt.GetTransactionPages().Count;

                // no Commit()
            }

            Assert.True(ownScratchPages > 0, "the transaction is expected to have allocated scratch pages");
            Assert.True(ownScratchPages < unflushedScratchPages,
                $"the transaction allocated {ownScratchPages} pages out of the {unflushedScratchPages} in the table, " +
                "otherwise this test cannot tell the two costs apart");

            Assert.True(testing.ScratchPagesExaminedDuringRollback == ownScratchPages,
                $"rolling back a transaction that allocated {ownScratchPages} scratch pages examined " +
                $"{testing.ScratchPagesExaminedDuringRollback} of them, out of the {unflushedScratchPages} accumulated since the last flush.");
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Rolling_back_a_transaction_that_freed_a_page_of_an_earlier_transaction_leaves_that_page_alone()
        {
            var overflowValue = new string('a', 20000); // an overflow, allocated as one block in the scratch space

            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("foo").Add("overflow", overflowValue);
                txw.Commit();
            }

            var scratchFile = Env.ScratchBufferPool._current.File;
            var allocatedPagesBefore = scratchFile.AllocatedPagesCount;

            using (var txw = Env.WriteTransaction())
            {
                // the overflow pages this frees were allocated by the committed transaction above, so this
                // transaction never owned them - rolling back must not hand them back to the scratch file
                txw.CreateTree("foo").Delete("overflow");

                // no Commit()
            }

            Assert.True(scratchFile.AllocatedPagesCount == allocatedPagesBefore,
                $"the scratch file held {allocatedPagesBefore} allocated pages before the rolled back transaction and " +
                $"{scratchFile.AllocatedPagesCount} after it");

            // read it back under a write transaction, which resolves pages through the restored scratch pages table
            using (var txw = Env.WriteTransaction())
            {
                var read = txw.CreateTree("foo").Read("overflow");

                Assert.True(read != null, "the value freed by the rolled back transaction is gone");
                Assert.Equal(overflowValue, read.Reader.ToStringValue());
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Rolling_back_the_transaction_started_by_an_async_commit_frees_only_its_own_scratch_pages()
        {
            var committedValue = new string('c', 20000);

            LowLevelTransaction.TestingStuff testing;
            int ownScratchPages;
            long allocatedPagesBefore;

            var tx1 = Env.WriteTransaction();

            try
            {
                tx1.CreateTree("foo").Add("committed", committedValue);

                // tx2 runs on top of tx1 while tx1 is still writing to the journal, and takes over the shared
                // scratch pages table with tx1's pages already in it
                using (var tx2 = tx1.BeginAsyncCommitAndStartNewTransaction(tx1.LowLevelTransaction.PersistentContext))
                {
                    using (tx1)
                    {
                        tx1.EndAsyncCommit();
                    }

                    tx1 = null;

                    allocatedPagesBefore = Env.ScratchBufferPool._current.File.AllocatedPagesCount;

                    testing = tx2.LowLevelTransaction.ForTestingPurposesOnly();

                    tx2.CreateTree("foo").Add("rolled-back", new string('r', 20000));

                    ownScratchPages = tx2.LowLevelTransaction.GetTransactionPages().Count;

                    // no Commit()
                }
            }
            finally
            {
                tx1?.Dispose();
            }

            Assert.True(ownScratchPages > 0, "the transaction is expected to have allocated scratch pages");

            Assert.True(testing.ScratchPagesExaminedDuringRollback == ownScratchPages,
                $"rolling back the transaction started by the async commit allocated {ownScratchPages} scratch pages but examined " +
                $"{testing.ScratchPagesExaminedDuringRollback}, which includes the pages the asynchronously committed transaction left behind");

            Assert.True(Env.ScratchBufferPool._current.File.AllocatedPagesCount == allocatedPagesBefore,
                $"the scratch file held {allocatedPagesBefore} allocated pages before the rolled back transaction and " +
                $"{Env.ScratchBufferPool._current.File.AllocatedPagesCount} after it");

            using (var txw = Env.WriteTransaction())
            {
                var tree = txw.CreateTree("foo");

                var committed = tree.Read("committed");

                Assert.True(committed != null, "the asynchronously committed value did not survive the rollback of the transaction that followed it");
                Assert.Equal(committedValue, committed.Reader.ToStringValue());

                Assert.Null(tree.Read("rolled-back"));
            }
        }

        /// <summary>
        /// Commits enough pages to leave the shared scratch pages table well above the size of any single
        /// transaction below. Nothing removes them - only a flush does, and flushing is disabled.
        /// </summary>
        private int PileUpUnflushedScratchPages()
        {
            var value = new string('a', 4000);

            using (var txw = Env.WriteTransaction())
            {
                var tree = txw.CreateTree("foo");

                for (var i = 0; i < 1000; i++)
                    tree.Add($"keys/{i:D5}", value);

                txw.Commit();
            }

            var count = Env.WriteTransactionPool.ScratchPagesInUse.Count;

            Assert.True(count > 100, $"expected the committed pages to still be in the scratch pages table, but it holds {count} of them");

            return count;
        }
    }
}
