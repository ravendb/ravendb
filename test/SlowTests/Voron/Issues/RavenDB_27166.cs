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
// snapshot, restoring those freed entries. The flush thread's retry cannot repair it - the free buffer entries
// were nulled (RavenDB-23264), so ForgetAboutScratchPage never runs again - so the environment would keep a scratch
// mapping onto positions that are back on the free list, and reads would follow the stale mapping to the wrong page
// once those positions get reused. The data file and journal are intact, so such a rollback marks the environment
// as catastrophically failed (the restored in-memory scratch state cannot be trusted) and a restart recovers all
// the committed data.
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

    // This is RavenDB_23264's scenario (a piggybacking tx applies the flush action and fails to commit), continued
    // past the rollback: the environment must be poisoned and a restart must bring back every committed value.
    [RavenFact(RavenTestCategory.Voron)]
    public void PiggybackedFlushFailure_MustNotLeaveEnvironmentServingCorruptedData()
    {
        RequireFileBasedPager(); // the restart below must build fresh options - the catastrophic-failure mark lives on the options instance

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
            txBlocker.Dispose(); // -> Rollback -> would restore the freed scratch entries -> poisons the environment

            Assert.True(flushThread.Join(TimeSpan.FromSeconds(30)), "flush thread did not complete");
        }
        finally
        {
            Env.Journal.Applicator.ForTestingPurposesOnly().OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush = null;
        }

        Assert.True(Env.Options.IsCatastrophicFailureSet, "the rollback did not mark the environment as catastrophically failed");
        Assert.NotNull(flushException); // the flush retry is refused on the poisoned environment

        // the data was never lost on disk - the forced restart recovers every committed value
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
    // (a read transaction pins the flush horizon below it). V1 exists only in scratch and the journal - the data
    // file does not have it - so the recovery after the poisoning rollback must bring it back.
    [RavenFact(RavenTestCategory.Voron)]
    public void FailedPiggybackingTx_ModifiedPage_PriorVersionNotCoveredByFlush_MustKeepItReadable()
    {
        RequireFileBasedPager(); // the restart below must build fresh options - the catastrophic-failure mark lives on the options instance

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

        RestartDatabase();

        using (var rtx = Env.ReadTransaction())
        {
            var read = rtx.ReadTree("tree").Read("p");
            Assert.True(read != null, "'p' is unreadable after recovery");
            Assert.Equal(V1, read.Reader.ToStringValue()); // the committed-but-unflushed version must survive; the doomed V2 never reached the journal
        }
    }

    // The failing tx itself modified "p", whose prior committed version (V1) IS covered by the parked flush:
    // the flush wrote V1 to the data file and freed its scratch entry. After the poisoning rollback the forced
    // restart must bring "p" back as V1.
    [RavenFact(RavenTestCategory.Voron)]
    public void FailedPiggybackingTx_ModifiedPage_PriorVersionCoveredByFlush_MustKeepItReadable()
    {
        RequireFileBasedPager(); // the restart below must build fresh options - the catastrophic-failure mark lives on the options instance

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

        RestartDatabase();

        using (var rtx = Env.ReadTransaction())
        {
            var read = rtx.ReadTree("tree").Read("p");
            Assert.True(read != null, "'p' is unreadable after recovery");
            Assert.Equal(V1, read.Reader.ToStringValue()); // written to the data file by the parked flush
        }
    }

    // Opens a write tx that updates "p" to V2, lets the parked flush install its journal-state update so the tx
    // piggybacks it in CommitStage1, and fails the commit just before the journal write. The rollback would restore
    // the scratch entries the flush action freed, so it must poison the environment.
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
                txBlocker.Dispose(); // -> Rollback -> poisons the environment

                Assert.True(flushThread.Join(TimeSpan.FromSeconds(30)), "flush thread did not complete");
                Env.Journal.Applicator.ForTestingPurposesOnly().OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush = null;
            }

            Assert.True(Env.Options.IsCatastrophicFailureSet, "the rollback did not mark the environment as catastrophically failed");
            Assert.NotNull(flushException); // the flush retry is refused on the poisoned environment
        }
        finally
        {
            Env.ForTestingPurposesOnly().ModifyNewLowLevelTransaction = null;
        }
    }
}
