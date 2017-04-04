using Voron;
using Voron.Impl.Journal;
using Voron.Impl.Scratch;
using Xunit;

namespace FastTests.Voron
{
    public class LazyTransactionsRespectPageBoundaries : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void CanSyncWhenLazyTransactionsHasBufferedMultipleTransactions()
        {
            // we start by ensuring that we have enough space in the scratch buffers
            PageFromScratchBuffer allocate;
            using (var tx = Env.WriteTransaction())
            {
                allocate = Env.ScratchBufferPool.Allocate(tx.LowLevelTransaction, 60);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                Env.ScratchBufferPool.Free(allocate.ScratchFileNumber, allocate.PositionInScratchBuffer, tx.LowLevelTransaction.Id);
                tx.Commit();
            }

            // then we run a lazy transaction
            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.IsLazyTransaction = true;
                var tree = tx.CreateTree("foo");
                tree.Add("test", "test");
                tx.Commit();
            }

            // and another one, but if the pager isn't 4KB, the 
            // _journal_ is, and this leaves an empty 4KB between 
            // the entries
            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.IsLazyTransaction = true;
                var tree = tx.CreateTree("foo");
                tree.Add("test2", "test");
                tx.Commit(); // this leaves 2 4kbs in the journal
            }

            // here we flush the lazy tx buffer
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("test3", "test");
                tx.Commit();
            }

            // and now we force them to sync
            Env.FlushLogToDataFile();
            using (var op = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            {
                op.SyncDataFile();
            }
        }
    }

}