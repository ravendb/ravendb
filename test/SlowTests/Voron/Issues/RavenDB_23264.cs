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
    public void Piggybacking_tx_failure_after_flush_action_should_not_cause_double_free_on_retry()
    {
        // RavenDB-23264: This test validates that if the _updateJournalStateAfterFlush action
        // is invoked by a piggybacking write tx that then fails to commit, the flush thread's
        // retry succeeds because the free loop is idempotent (entries are nulled after freeing).

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

        // Step 11: Flush thread wakes up, creates its own tx, Commit() →
        // CommitStage1 → OnTransactionCommitted → action invoked AGAIN.
        // With the fix (idempotent free loop): entries are null → skip → success.
        // Without the fix: double-free → crash.
        flushThread.Join(TimeSpan.FromSeconds(30));

        // Step 12: Verify no exception from flush thread
        Assert.Null(flushException);

        // Clean up
        Env.Journal.Applicator.ForTestingPurposesOnly().OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush = null;

        // Step 13 (RavenDB-27166): the retry did not double-free, but the environment is not healthy either.
        // txBlocker's rollback restored its tx-start scratch snapshot, resurrecting the entries the flush action
        // had already freed via tx.ForgetAboutScratchPage. The retry cannot repair that: the free buffer entries
        // were nulled by the first (partial) execution, so ForgetAboutScratchPage never runs for them again.
        // The scratch positions are back on the free list while the environment still maps p1/p2/p3 onto them,
        // so reusing the scratch makes those pages resolve to whatever now occupies their old positions.
        for (int c = 0; c < 10; c++)
        {
            using var txw = Env.WriteTransaction();
            var tree = txw.CreateTree("churn");
            for (int i = 0; i < 100; i++)
                tree.Add($"churn-{c}-{i}", new string((char)('0' + i % 10), 512));
            txw.Commit();
        }

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
