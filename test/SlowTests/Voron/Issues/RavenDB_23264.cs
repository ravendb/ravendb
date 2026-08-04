using System;
using System.Threading;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_23264 : StorageTest
{
    public RavenDB_23264(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        base.Configure(options);

        options.ManualFlushing = true;
        options.ManualSyncing = true;
    }
    
    [RavenFact(RavenTestCategory.Voron)]
    public void Piggybacking_tx_failure_after_flush_action_should_poison_the_environment_and_recover_on_restart()
    {
        // RavenDB-23264: the _updateJournalStateAfterFlush action invoked by a piggybacking write tx that then
        // fails to commit must not double-free on the flush thread's retry (the free loop nulls entries after
        // freeing them). RavenDB-27166 then revoked the retry contract entirely: such a rollback would restore
        // the freed scratch entries, so it marks the environment as catastrophically failed and the recovery
        // happens on restart.

        // file-based so RestartDatabase builds fresh options, the way a real database reload does - the
        // catastrophic-failure mark set by the RavenDB-27166 fix lives on the options instance
        RequireFileBasedPager();

        long p1, p2, p3;

        // Step 1: Create initial pages and commit
        using (var txw = Env.WriteTransaction())
        {
            p1 = txw.LowLevelTransaction.AllocatePage(1).PageNumber;
            p2 = txw.LowLevelTransaction.AllocatePage(1).PageNumber;
            p3 = txw.LowLevelTransaction.AllocatePage(1).PageNumber;
            txw.LowLevelTransaction.ModifyPage(p1);
            txw.LowLevelTransaction.ModifyPage(p2);
            txw.LowLevelTransaction.ModifyPage(p3);
            txw.Commit();
        }

        // Step 2: Flush to data file (so scratch pages from step 1 can be freed in next flush)
        Env.FlushLogToDataFile();

        // Step 3: Create more committed txs that modify the same pages.
        // These modifications create new scratch entries; the OLD scratch entries
        // (from step 1) will be in the next flush's bufferOfPageFromScratchBuffersToFree.
        using (var txw = Env.WriteTransaction())
        {
            txw.LowLevelTransaction.ModifyPage(p1);
            txw.LowLevelTransaction.ModifyPage(p2);
            txw.LowLevelTransaction.ModifyPage(p3);
            txw.Commit();
        }

        // Step 4: Create an empty write tx that holds the write lock.
        // This blocks the flush thread from getting the lock.
        var txBlocker = Env.WriteTransaction();

        // Step 5: Set up BeforeCommitFinalization to throw AFTER the flush action completes
        // but BEFORE CommitStage3 sets Committed = true. This simulates a piggybacking tx
        // that successfully invokes the flush action but fails to commit.
        txBlocker.LowLevelTransaction.BeforeCommitFinalization += _ =>
        {
            throw new InvalidOperationException("Simulated failure after flush action completed");
        };

        // Step 6: Set up synchronization to know when the flush sets the action
        var actionSetEvent = new ManualResetEventSlim(false);
        Env.Journal.Applicator.ForTestingPurposesOnly().OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush += () =>
        {
            actionSetEvent.Set();
        };

        // Step 7: Start flush on a background thread.
        // The flush will prepare the state, write to data file, then try to get the write lock.
        // Since txBlocker holds the lock, the flush thread will set _updateJournalStateAfterFlush
        // and wait.
        Exception flushException = null;
        var flushThread = new Thread(() =>
        {
            try
            {
                Env.FlushLogToDataFile();
            }
            catch (Exception e)
            {
                flushException = e;
            }
        });
        flushThread.Start();

        // Step 8: Wait for the flush to set the action
        Assert.True(actionSetEvent.Wait(TimeSpan.FromSeconds(30)), "Timed out waiting for flush to set _updateJournalStateAfterFlush");

        // Step 9: Commit txBlocker. This triggers CommitStage1 → OnTransactionCommitted,
        // which invokes the _updateJournalStateAfterFlush action. The action frees scratch
        // pages from the buffer. Then BeforeCommitFinalization throws, so the tx is NOT
        // committed and the action is NOT cleared.
        try
        {
            txBlocker.Commit();
            Assert.Fail("Expected InvalidOperationException from BeforeCommitFinalization");
        }
        catch (InvalidOperationException e) when (e.Message.Contains("Simulated failure"))
        {
            // Expected - the simulated failure prevents the tx from committing
        }

        // Step 10: Dispose txBlocker → releases write lock.
        // OnTransactionCompleted will NOT clear the action because Committed = false.
        txBlocker.Dispose();

        // Step 11 (RavenDB-27166): the flush thread wakes up, but txBlocker's rollback marked the environment
        // as catastrophically failed - its retry transaction is refused and the flush surfaces the failure.
        flushThread.Join(TimeSpan.FromSeconds(30));

        // Clean up
        Env.Journal.Applicator.ForTestingPurposesOnly().OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush = null;

        Assert.True(Env.Options.IsCatastrophicFailureSet, "the rollback did not mark the environment as catastrophically failed");
        Assert.NotNull(flushException);

        // Step 12: no committed data was lost (the failed transaction never reached the journal) - the forced
        // restart recovers a consistent state
        RestartDatabase();

        using (var rtx = Env.ReadTransaction())
        {
            foreach (var pageNumber in new[] { p1, p2, p3 })
            {
                var page = rtx.LowLevelTransaction.GetPage(pageNumber);
                Assert.Equal(pageNumber, page.PageNumber);
            }
        }
    }
}
