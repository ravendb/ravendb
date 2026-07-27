using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.SharedJournal;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27156(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void FailedSharedJournalWrite_WithPendingFlushStateUpdate_PoisonsBranchAndRecoversOnRestart()
        => RunScenario(flushPendingDuringDoomedCommit: true);

    [RavenFact(RavenTestCategory.Voron)]
    public void FailedSharedJournalWrite_WithoutPendingFlush_PoisonsBranchAndRecoversOnRestart()
        => RunScenario(flushPendingDuringDoomedCommit: false);

    // Generic torn-tail recovery bug behind the wrong-page reads / AccessViolation seen after a failed
    // shared-journal write (no shared journals needed): a journal that grew the data pager and then ends in
    // a torn tail must still publish the grown pager state, else DataPagerState < NextPageNumber and reads fault.
    [RavenFact(RavenTestCategory.Voron)]
    public void TornJournalTail_AfterDataPagerGrowth_RecoversWithPagerCoveringNextPage()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        var expected = new Dictionary<string, string>();

        // phase 1: a big tx (grows the pager on replay) then a torn-tail tx, both in one journal. Manual
        // flush/sync keeps everything in the journal so recovery has to rebuild and grow the data pager.
        {
            var options = StorageEnvironmentOptions.ForPathForTests(path);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.InitialLogFileSize = 64 * 1024 * 1024; // one journal holds the big tx + the torn tail

            var env = new StorageEnvironment(options);
            try
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("tree");
                    for (int i = 0; i < 5000; i++)
                    {
                        var key = $"items/{i:D6}";
                        var value = i + "-" + new string((char)('a' + i % 26), 300);
                        tree.Add(key, value);
                        expected[key] = value;
                    }
                    tx.Commit();
                }

                Output.WriteLine($"after big tx: NextPageNumber={env.NextPageNumber}");

                var armed = 1;
                env.Options.ForTestingPurposesOnly().SimulatePartialJournalWriteFailure = total =>
                {
                    if (Interlocked.Exchange(ref armed, 0) == 0)
                        return null;
                    return new StorageEnvironmentOptions.TestingStuff.PartialJournalWriteFailure
                    {
                        NumberOf4KbsToWrite = total / 2,
                        Error = new IOException("RavenDB-27156 simulated torn journal write")
                    };
                };

                // incompressible value: the write must span several 4KB blocks so writing half leaves a real
                // torn tail (a compressible value collapses to one block and writing half writes nothing)
                var tailBytes = new byte[32 * 1024];
                new Random(1234).NextBytes(tailBytes);
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("torn-tail", Convert.ToBase64String(tailBytes));
                    Assert.ThrowsAny<Exception>(() => tx.Commit());
                }
            }
            finally
            {
                Record.Exception(() => env.Dispose()); // env is catastrophically failed after the torn write
            }
        }

        // phase 2: reopen and recover
        {
            var options = StorageEnvironmentOptions.ForPathForTests(path);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.InitialLogFileSize = 64 * 1024 * 1024;
            options.OnRecoveryError += (_, _) => { }; // continue past the torn tail (as a running server does) instead of throwing at it

            using var env = new StorageEnvironment(options);

            var state = env.CurrentStateRecord;
            Output.WriteLine($"recovered: NextPageNumber={state.NextPageNumber} dataPagerAllocPages={state.DataPagerState.NumberOfAllocatedPages}");

            // without the fix the pager stays at its pre-growth size (e.g. allocPages=8 vs NextPageNumber=212)
            Assert.True(state.DataPagerState.NumberOfAllocatedPages >= state.NextPageNumber,
                $"data pager undersized after torn-tail recovery: allocPages={state.DataPagerState.NumberOfAllocatedPages} < NextPageNumber={state.NextPageNumber} (RavenDB-27156)");

            using var rtx = env.ReadTransaction();
            var tree = rtx.ReadTree("tree");
            Assert.NotNull(tree);
            foreach (var (key, value) in expected)
            {
                var read = tree.Read(key);
                Assert.True(read != null, $"value for '{key}' is gone after recovery");
                Assert.Equal(value, read.Reader.ToStringValue());
            }
            Assert.Null(tree.Read("torn-tail"));
        }
    }

    private void RunScenario(bool flushPendingDuringDoomedCommit)
    {
        var rootPath = NewDataPath(suffix: "root");
        var branchPath = NewDataPath(suffix: "branch");
        IOExtensions.DeleteDirectory(rootPath);
        IOExtensions.DeleteDirectory(branchPath);

        var values = new Dictionary<string, string>();

        // ----- phase 1: seed, then fail a shared-journal write on the root -----
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            using var scope = root.Journal.SharedJournalsScope();
            using var pump = new MergerPump(root);

            StorageEnvironment branch = null;
            var releaseFlusher = new ManualResetEventSlim(false);
            Task flushTask = Task.CompletedTask;
            try
            {
                branch = SharedJournalTests.CreateBranchEnv(branchPath, root);

                // committed data - lives in the branch scratch file until a flush moves it to the data file
                for (int c = 0; c < 3; c++)
                {
                    using var tx = branch.WriteTransaction();
                    var tree = tx.CreateTree("tree");
                    for (int i = 0; i < 32; i++)
                    {
                        var key = $"key-{c}-{i}";
                        var value = $"{c}-{i}-" + new string((char)('a' + i % 26), 128);
                        tree.Add(key, value);
                        values[key] = value;
                    }
                    tx.Commit();
                }

                if (flushPendingDuringDoomedCommit)
                {
                    // park the flusher right after it copied the pages to the data file and installed
                    // _updateJournalStateAfterFlush - the doomed commit applies it in CommitStage1, and
                    // its rollback is what corrupted the scratch state before the fix
                    var installed = new ManualResetEventSlim(false);
                    branch.Journal.Applicator.ForTestingPurposesOnly().OnWaitForJournalStateToBeUpdated_AfterAssigning_updateJournalStateAfterFlush = () =>
                    {
                        installed.Set();
                        releaseFlusher.Wait();
                    };
                    flushTask = Task.Run(() => branch.FlushLogToDataFile());
                    Assert.True(installed.Wait(TimeSpan.FromSeconds(30)), "flush did not reach the journal-state-update stage");
                }

                // fail the next merged commit on the root (one-shot): throw just before the actual
                // journal write, so the branch entries are not yet released with success
                var armed = 1;
                root.ForTestingPurposesOnly().ModifyNewLowLevelTransaction = t =>
                {
                    if (Interlocked.Exchange(ref armed, 0) == 1)
                        t.ActionToCallJustBeforeWritingToJournal = () => throw new InvalidOperationException("RavenDB-27156 simulated commit-stage-2 write failure");
                };

                using (var tx = branch.WriteTransaction())
                {
                    var tree = tx.CreateTree("tree");
                    tree.Add("doomed", "doomed");
                    var doomedEx = Assert.Throws<InvalidOperationException>(() => tx.Commit());
                    Output.WriteLine($"doomed commit failed with: {doomedEx.Message}");
                }

                // the fix: the branch env taking part in the failed merged commit must be marked as
                // catastrophically failed, so it refuses further transactions instead of serving
                // corrupted state
                Assert.True(branch.Options.IsCatastrophicFailureSet, "branch env was not marked as catastrophically failed");

                Assert.Throws<InvalidOperationException>(() =>
                {
                    using var tx = branch.WriteTransaction();
                });

                releaseFlusher.Set();
                var flushEx = Record.Exception(() => flushTask.Wait(TimeSpan.FromSeconds(30)));
                if (flushEx != null)
                    Output.WriteLine($"flush completion: {flushEx.GetType().Name}: {flushEx.Message}");
            }
            finally
            {
                releaseFlusher.Set();
                branch?.Dispose();
            }
        }

        // ----- phase 2: restart - recovery replays the journals; the doomed write never reached the
        // journal (it threw before the write), so its data is simply absent and no committed data is lost -----
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            using var scope = root.Journal.SharedJournalsScope();
            using var pump = new MergerPump(root);

            using var branch = SharedJournalTests.CreateBranchEnv(branchPath, root);
            using var rtx = branch.ReadTransaction();
            var tree = rtx.ReadTree("tree");
            Assert.NotNull(tree);
            foreach (var (key, expected) in values)
            {
                var read = tree.Read(key);
                Assert.True(read != null, $"value for '{key}' is gone after restart");
                Assert.Equal(expected, read.Reader.ToStringValue());
            }

            Assert.Null(tree.Read("doomed"));
        }
    }

    // standing merger mimicking SharedIndexJournals.WriteSharedJournals, including its failure
    // handling (swap SharedJournalState + SetException on all pending branch commits)
    private sealed class MergerPump : IDisposable
    {
        private readonly ManualResetEventSlim _mergeSubmitted = new(false);
        private readonly Thread _thread;
        private volatile bool _stop;

        public MergerPump(StorageEnvironment root)
        {
            root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(_mergeSubmitted);
            _thread = new Thread(() =>
            {
                while (_stop == false)
                {
                    if (_mergeSubmitted.Wait(TimeSpan.FromMilliseconds(50)) == false)
                        continue;
                    _mergeSubmitted.Reset();
                    try
                    {
                        using var tx = root.WriteTransaction();
                        tx.Commit();
                    }
                    catch (Exception e)
                    {
                        Interlocked.Exchange(ref root.Journal.SharedJournalState, new SharedJournalState()).SetException(e);
                    }
                }
            }) { IsBackground = true, Name = "RavenDB_27156 merger pump" };
            _thread.Start();
        }

        public void Dispose()
        {
            _stop = true;
            _thread.Join();
            _mergeSubmitted.Dispose();
        }
    }

    // A journal skipped via IgnoreInvalidJournalErrors may have grown the data pager (and the data file) for
    // transactions applied before the corruption point. The published pager state must reflect that growth -
    // otherwise the environment state claims a smaller mapping than the file recovery actually produced.
    [RavenFact(RavenTestCategory.Voron)]
    public void SkippedInvalidJournal_AfterDataPagerGrowth_PublishesGrownDataPagerState()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        long tx2Start4Kb, tx3Start4Kb;

        // phase 1: synced baseline, then - in one journal - a big tx (grows the pager on replay) followed by
        // two small txs. tx2 gets corrupted on disk; tx3 stays valid so the journal reader classifies the
        // journal as invalid (invalid tx followed by a valid one) instead of as a torn tail.
        {
            var options = StorageEnvironmentOptions.ForPathForTests(path);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.InitialLogFileSize = 64 * 1024 * 1024;

            using var env = new StorageEnvironment(options);

            using (var tx = env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("baseline", "baseline-value");
                tx.Commit();
            }
            env.FlushLogToDataFile();
            Assert.True(env.SyncDataFileImmediately(), "failed to sync the baseline");

            using (var tx = env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree");
                for (int i = 0; i < 5000; i++)
                    tree.Add($"items/{i:D6}", i + "-" + new string((char)('a' + i % 26), 300));
                tx.Commit();
            }
            tx2Start4Kb = env.CurrentStateRecord.Journal.Last4KWritePosition;

            using (var tx = env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("second", "second-value");
                tx.Commit();
            }
            tx3Start4Kb = env.CurrentStateRecord.Journal.Last4KWritePosition;
            Assert.True(tx3Start4Kb > tx2Start4Kb);

            using (var tx = env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("third", "third-value");
                tx.Commit();
            }
            Assert.Equal(0, env.CurrentStateRecord.Journal.Number); // everything must be in a single journal
        }

        // corrupt tx2's transaction header on disk
        var journalFile = Directory.GetFiles(Path.Combine(path, "Journals"), "*.journal").Single();
        using (var file = new FileStream(journalFile, FileMode.Open, FileAccess.ReadWrite))
        {
            file.Position = tx2Start4Kb * 4096;
            var garbage = new byte[512];
            Array.Fill(garbage, (byte)0xDE);
            file.Write(garbage);
        }

        // phase 2: reopen with the dangerous flag - recovery replays the big tx (growing the data pager and
        // the file), hits the corrupted tx, finds the valid tx after it and skips the whole journal
        {
            var options = StorageEnvironmentOptions.ForPathForTests(path);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.InitialLogFileSize = 64 * 1024 * 1024;
            options.IgnoreInvalidJournalErrors = true;
            options.OnRecoveryError += (_, _) => { };
            var initLog = new List<string>();
            options.AddToInitLog = (_, msg) => { lock (initLog) initLog.Add(msg); };

            using var env = new StorageEnvironment(options);

            // guard the scenario: the skip path must have been taken (not the torn-tail / clean-end paths)
            Assert.Contains(initLog, m => m.Contains("Encountered invalid journal"));

            // the pin: the published pager state must cover the data file recovery produced
            var dataFileLength = new FileInfo(Path.Combine(path, "Raven.voron")).Length;
            var state = env.CurrentStateRecord;
            Assert.True(state.DataPagerState.TotalAllocatedSize >= dataFileLength,
                $"published data pager state ({state.DataPagerState.TotalAllocatedSize} bytes) does not cover the data file recovery produced ({dataFileLength} bytes)");

            // the synced baseline must survive; what remains visible of the skipped journal's transactions is
            // indeterminate under the dangerous flag (applied-then-discarded pages can stay reachable), so no
            // assertion is made on it
            using (var rtx = env.ReadTransaction())
            {
                var tree = rtx.ReadTree("tree");
                Assert.NotNull(tree);
                Assert.Equal("baseline-value", tree.Read("baseline").Reader.ToStringValue());
            }

            // and the environment must remain fully usable
            using (var tx = env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("after-recovery", "after-recovery-value");
                tx.Commit();
            }
            using (var rtx = env.ReadTransaction())
                Assert.Equal("after-recovery-value", rtx.ReadTree("tree").Read("after-recovery").Reader.ToStringValue());
        }
    }
}
