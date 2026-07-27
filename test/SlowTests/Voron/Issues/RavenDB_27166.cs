using System;
using System.Collections.Generic;
using System.Threading;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues;

// RavenDB-27166: a write transaction that piggybacks a pending journal flush-state update (freeing the flushed
// scratch pages, including tx.ForgetAboutScratchPage) and then fails to commit rolls back to its tx-start scratch
// snapshot, resurrecting those freed entries. The flush thread's retry cannot repair it - the free buffer entries
// were nulled (RavenDB-23264), so ForgetAboutScratchPage never runs again - so the environment keeps a scratch
// mapping onto positions that are back on the free list. Once later writes reuse those positions, reads of the
// original pages follow the stale mapping to the wrong page: a DEBUG build trips ScratchBufferFile.VerifyMatch,
// while a release build silently loses every committed value. A restart recovers all of it (the data file and
// journal are intact), so the corruption is purely in the environment's in-memory scratch state.
public class RavenDB_27166 : StorageTest
{
    public RavenDB_27166(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        base.Configure(options);

        options.ManualFlushing = true;
        options.ManualSyncing = true;
    }

    // This is RavenDB_23264's scenario (a piggybacking tx applies the flush action, fails to commit, and the flush
    // thread retries without double-freeing), continued past the retry: the environment must not serve corrupted
    // data afterwards.
    [RavenFact(RavenTestCategory.Voron)]
    public void PiggybackedFlushFailure_MustNotLeaveEnvironmentServingCorruptedData()
    {
        var values = new Dictionary<string, string>();

        using (var txw = Env.WriteTransaction())
        {
            var tree = txw.CreateTree("tree");
            for (int i = 0; i < 200; i++)
            {
                var key = $"key-{i:D4}";
                var value = i + "-" + new string((char)('a' + i % 26), 256);
                tree.Add(key, value);
                values[key] = value;
            }
            txw.Commit();
        }

        Env.FlushLogToDataFile();

        // modify the same pages again so the old scratch entries land in the next flush's free buffer
        using (var txw = Env.WriteTransaction())
        {
            var tree = txw.CreateTree("tree");
            for (int i = 0; i < 200; i += 2)
            {
                var key = $"key-{i:D4}";
                var value = "v2-" + i + "-" + new string((char)('A' + i % 26), 256);
                tree.Add(key, value);
                values[key] = value;
            }
            txw.Commit();
        }

        // hold the write lock so the flush thread has to park _updateJournalStateAfterFlush
        var txBlocker = Env.WriteTransaction();

        // throw after the journal write succeeded but before Committed is set, so the transaction rolls back having
        // already applied the flush action
        txBlocker.LowLevelTransaction.BeforeCommitFinalization += _ => throw new InvalidOperationException("Simulated failure after flush action completed");

        var actionSetEvent = new ManualResetEventSlim(false);
        Env.Journal.Applicator.ForTestingPurposesOnly().OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush += () => actionSetEvent.Set();

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
        try
        {
            Assert.True(actionSetEvent.Wait(TimeSpan.FromSeconds(30)), "Timed out waiting for flush to set _updateJournalStateAfterFlush");

            Assert.Throws<InvalidOperationException>(() => txBlocker.Commit());
            txBlocker.Dispose(); // -> Rollback -> restores the tx-start scratch snapshot

            Assert.True(flushThread.Join(TimeSpan.FromSeconds(30)), "flush thread did not complete");
        }
        finally
        {
            Env.Journal.Applicator.ForTestingPurposesOnly().OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush = null;
        }

        // if the environment was poisoned, the failure was reported and the database will be unloaded and recovered -
        // that is an acceptable outcome, the unacceptable one is staying up while serving corrupted data
        if (Env.Options.IsCatastrophicFailureSet)
            return;

        // reuse the scratch: the positions the flush action freed are on the free list, so new allocations take them
        // while the resurrected entries still map the old pages onto them
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
            var tree = rtx.ReadTree("tree");
            Assert.NotNull(tree);
            foreach (var (key, expected) in values)
            {
                var read = tree.Read(key);
                Assert.True(read != null, $"'{key}' is unreadable after the failed piggybacking commit");
                Assert.Equal(expected, read.Reader.ToStringValue());
            }
        }

        // sanity: the data was never lost on disk - a restart recovers everything, which is why the in-memory
        // corruption above is what has to be prevented
        RestartDatabase();

        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.ReadTree("tree");
            Assert.NotNull(tree);
            foreach (var (key, expected) in values)
                Assert.Equal(expected, tree.Read(key).Reader.ToStringValue());
        }
    }

    private const string V0 = "v0-baseline";
    private const string V1 = "v1-committed-must-survive";
    private const string V2 = "v2-rolled-back";

    // The failing tx itself modified "p", whose prior committed version (V1) is NOT covered by the parked flush
    // (a read transaction pins the flush horizon below it). After the rollback the committed V1 must still be
    // readable - it exists only in scratch, the data file does not have it.
    [RavenFact(RavenTestCategory.Voron)]
    public void FailedPiggybackingTx_ModifiedPage_PriorVersionNotCoveredByFlush_MustKeepItReadable()
    {
        // "p" = V0, flushed to the data file
        using (var txw = Env.WriteTransaction())
        {
            txw.CreateTree("tree").Add("p", V0);
            txw.Commit();
        }
        Env.FlushLogToDataFile();

        // filler commit - the work the parked flush will cover
        using (var txw = Env.WriteTransaction())
        {
            var filler = txw.CreateTree("filler");
            for (int i = 0; i < 64; i++)
                filler.Add($"f-{i}", new string('f', 256));
            txw.Commit();
        }

        // bump the tx id so the read tx below pins the flush horizon above the filler but below "p" = V1
        using (var txw = Env.WriteTransaction())
        {
            txw.CreateTree("filler").Add("bump", "bump");
            txw.Commit();
        }

        using (var _ = Env.ReadTransaction()) // pins the flush horizon
        {
            // committed but not covered by the parked flush - lives only in scratch
            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("tree").Add("p", V1);
                txw.Commit();
            }

            RunPiggybackingTxThatModifiesPageAndFails();
        }

        // poisoning the environment is the other acceptable outcome: the failure is reported and recovery
        // restores a consistent state (the doomed write never reached the journal)
        if (Env.Options.IsCatastrophicFailureSet)
            return;

        // the failed pre-write commit leaves its entry in the merge buffer (separate issue) - clear it so this
        // test isolates the scratch mapping
        Env.Journal.SharedJournalState.Reset();

        using (var txw = Env.WriteTransaction()) // publish the post-rollback state
        {
            txw.CreateTree("sanity").Add("s", "s");
            txw.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var read = rtx.ReadTree("tree").Read("p");
            Assert.True(read != null, "'p' is unreadable after the failed piggybacking commit");
            Assert.Equal(V1, read.Reader.ToStringValue()); // the committed-but-unflushed version must survive
        }
    }

    // The failing tx itself modified "p", whose prior committed version (V1) IS covered by the parked flush:
    // the flush wrote V1 to the data file and freed its scratch entry (the map already pointed to the failing
    // tx's entry, so nothing was removed from the map at that point). After the rollback "p" must not be mapped
    // onto the freed scratch position.
    [RavenFact(RavenTestCategory.Voron)]
    public void FailedPiggybackingTx_ModifiedPage_PriorVersionCoveredByFlush_MustNotResurrectFreedScratch()
    {
        // "p" = V0, flushed to the data file
        using (var txw = Env.WriteTransaction())
        {
            txw.CreateTree("tree").Add("p", V0);
            txw.Commit();
        }
        Env.FlushLogToDataFile();

        // "p" = V1, committed - covered by the parked flush below (no read tx pins the horizon)
        using (var txw = Env.WriteTransaction())
        {
            txw.CreateTree("tree").Add("p", V1);
            txw.Commit();
        }

        RunPiggybackingTxThatModifiesPageAndFails();

        // poisoning the environment is the other acceptable outcome, as above
        if (Env.Options.IsCatastrophicFailureSet)
            return;

        Env.Journal.SharedJournalState.Reset(); // failed pre-write commit leaves its entry in the merge buffer - separate issue

        // reuse the scratch so a resurrected mapping onto freed positions turns into wrong-page reads
        for (int c = 0; c < 10; c++)
        {
            using var txw = Env.WriteTransaction();
            var churn = txw.CreateTree("churn");
            for (int i = 0; i < 100; i++)
                churn.Add($"churn-{c}-{i}", new string((char)('0' + i % 10), 512));
            txw.Commit();
        }

        using (var rtx = Env.ReadTransaction())
        {
            var read = rtx.ReadTree("tree").Read("p");
            Assert.True(read != null, "'p' is unreadable after the failed piggybacking commit");
            Assert.Equal(V1, read.Reader.ToStringValue()); // flushed to the data file by the parked flush
        }
    }

    // Opens a write tx that updates "p" to V2, lets the parked flush install its journal-state update so the tx
    // piggybacks it in CommitStage1, and fails the commit just before the journal write. The tx rolls back.
    private void RunPiggybackingTxThatModifiesPageAndFails()
    {
        var armed = 1;
        Env.ForTestingPurposesOnly().ModifyNewLowLevelTransaction = t =>
        {
            if (Interlocked.Exchange(ref armed, 0) == 1)
                t.ActionToCallJustBeforeWritingToJournal = () => throw new InvalidOperationException("RavenDB-27166 simulated pre-write commit failure");
        };

        var txBlocker = Env.WriteTransaction();
        Env.ForTestingPurposesOnly().ModifyNewLowLevelTransaction = null;
        try
        {
            txBlocker.CreateTree("tree").Add("p", V2);

            var actionSetEvent = new ManualResetEventSlim(false);
            Env.Journal.Applicator.ForTestingPurposesOnly().OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush += () => actionSetEvent.Set();

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
            try
            {
                Assert.True(actionSetEvent.Wait(TimeSpan.FromSeconds(30)), "Timed out waiting for flush to set _updateJournalStateAfterFlush");

                Assert.Throws<InvalidOperationException>(() => txBlocker.Commit());
            }
            finally
            {
                txBlocker.Dispose(); // -> Rollback

                Assert.True(flushThread.Join(TimeSpan.FromSeconds(30)), "flush thread did not complete");
                Env.Journal.Applicator.ForTestingPurposesOnly().OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush = null;
            }
            // on an environment that stayed up the flush retry must have completed cleanly; a poisoned
            // environment faults the retry by design and the calling test stops there
            if (Env.Options.IsCatastrophicFailureSet == false)
                Assert.Null(flushException);
        }
        finally
        {
            Env.ForTestingPurposesOnly().ModifyNewLowLevelTransaction = null;
        }
    }
}
