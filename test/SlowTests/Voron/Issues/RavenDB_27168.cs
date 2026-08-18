using System;
using System.Collections.Generic;
using System.Threading;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Issues;

// After a flush completes and its journals are deleted, every page covered by those journals must be
// present in the data file: later transactions journal only diffs against that base, and recovery
// re-creates the page by reading the base from the data file and applying the diffs. A page the
// flusher skips is unrecoverable once its journal is gone - recovery reads zeroes, applies a diff to
// them, and fails with "we read a page with header of page 0".
public class RavenDB_27168 : StorageTest
{
    public RavenDB_27168(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.MaxLogFileSize = 64 * 1024; // frequent journal rollover, so flushed journals can actually be deleted
    }

    // The flusher picks its batch of records first and only then reads the pages through the batch's
    // newest record snapshot. Transactions that commit in between push newer versions of the same pages,
    // and pruning must not drop the versions the pending flush still needs - if it does, the flusher
    // skips the page as "freed", the journal holding its only full image is deleted, and the database
    // cannot recover.
    [RavenFact(RavenTestCategory.Voron)]
    public void FlushMustWritePagesModifiedAgainAfterTheFlushRecordWasPicked()
    {
        RequireFileBasedPager();

        var expected = new Dictionary<string, string>();

        // enough transactions to roll through several journal files, so the journals holding the only
        // full images of the hot tree pages become deletable once the flush covers them
        for (var round = 0; round < 8; round++)
            WriteRoundWithAsyncCommits(round, expected);

        using (var hookEntered = new ManualResetEventSlim())
        using (var resumeFlush = new ManualResetEventSlim())
        {
            Env.Journal.Applicator.ForTestingPurposesOnly().OnApplyLogsToDataFile_BeforeWritingToDataFile = () =>
            {
                hookEntered.Set();
                resumeFlush.Wait(TimeSpan.FromSeconds(30));
            };

            try
            {
                var flushThread = new Thread(() => Env.FlushLogToDataFile());
                flushThread.Start();

                Assert.True(hookEntered.Wait(TimeSpan.FromSeconds(30)), "flush never reached the hook");

                // the flush picked its batch - now commit more versions of the same pages, so pruning
                // gets a chance to drop the versions the pending flush is about to read
                for (var round = 8; round < 11; round++)
                    WriteRoundWithAsyncCommits(round, expected);

                resumeFlush.Set();
                flushThread.Join();
            }
            finally
            {
                Env.Journal.Applicator.ForTestingPurposesOnly().OnApplyLogsToDataFile_BeforeWritingToDataFile = null;
            }
        }

        using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            Assert.True(sync.SyncDataFile(), "sync did not run");

        RestartDatabase();

        AssertAllReadable(expected);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PagesFlushedBeforeJournalDeletionMustSurviveRestart_SequentialRounds()
    {
        RequireFileBasedPager();

        var expected = new Dictionary<string, string>();

        for (var round = 0; round < 5; round++)
        {
            WriteRoundWithAsyncCommits(round, expected);

            Env.FlushLogToDataFile();
            using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
                sync.SyncDataFile();
        }

        RestartDatabase();

        AssertAllReadable(expected);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PagesFlushedBeforeJournalDeletionMustSurviveRestart_FlushConcurrentWithCommits()
    {
        RequireFileBasedPager();

        var expected = new Dictionary<string, string>();

        using (var stop = new ManualResetEventSlim())
        {
            Exception flushError = null;
            var flusher = new Thread(() =>
            {
                try
                {
                    while (stop.Wait(millisecondsTimeout: 15) == false)
                    {
                        Env.FlushLogToDataFile();
                        Env.ForceSyncDataFile();
                    }
                }
                catch (Exception e)
                {
                    flushError = e;
                }
            });
            flusher.Start();

            try
            {
                for (var round = 0; round < 10; round++)
                    WriteRoundWithAsyncCommits(round, expected);
            }
            finally
            {
                stop.Set();
                flusher.Join();
            }

            Assert.Null(flushError);
        }

        Env.FlushLogToDataFile();
        using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            sync.SyncDataFile();

        RestartDatabase();

        AssertAllReadable(expected);
    }

    // A page that was flushed and freed leaves a tombstone-headed chain in the scratch table. Once no
    // active snapshot can see below the tombstone, the whole chain is unreachable and must be reclaimed -
    // otherwise an append-only workload grows the table by one slot and two entries per flushed page,
    // and every rebuild doubles, holding the write lock for seconds.
    [RavenFact(RavenTestCategory.Voron)]
    public void ScratchTableMustNotRetainFlushedPages()
    {
        RequireFileBasedPager();

        var expected = new Dictionary<string, string>();

        for (var round = 0; round < 12; round++)
        {
            WriteUniqueKeysRound(round, expected);

            Env.FlushLogToDataFile();
            using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
                sync.SyncDataFile();
        }

        // a compaction pass under the write lock - this is what reclaims dead chains
        using (var txw = Env.WriteTransaction())
        {
            Env.ScratchPagesTable.ForceRebuildForTests();
            txw.Commit();
        }

        var (usedSlots, visibleCount, usedEntries, freeEntries) = Env.ScratchPagesTable.GetStateForTests();
        var deadChains = usedSlots - visibleCount;
        var retainedEntries = usedEntries - freeEntries;

        Assert.True(deadChains <= 64,
            $"The table retains {deadChains} dead chains for pages that were flushed and freed " +
            $"(usedSlots={usedSlots} visible={visibleCount} entries={retainedEntries})");
        Assert.True(retainedEntries <= visibleCount * 2 + 64,
            $"The table retains {retainedEntries} entries for {visibleCount} visible pages");

        AssertAllReadable(expected);
    }

    // The scratch table's version chains are walked by wait-free readers while the write transaction
    // prunes them. A reader whose snapshot is older than an entry walks *through* that entry - it loads
    // the link to it, then its sequence, then its next link. If pruning recycles the entry inside that
    // window, the reader follows a rewritten link into another page's chain and resolves the wrong
    // scratch page for the page number it asked for.
    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void ConcurrentReadersMustNeverResolveAWrongScratchPage()
    {
        RequireFileBasedPager();

        const int pageCount = 64;
        var pages = new long[pageCount];

        using (var tx = Env.WriteTransaction())
        {
            var llt = tx.LowLevelTransaction;
            for (var i = 0; i < pageCount; i++)
            {
                var page = llt.AllocatePage(1);
                pages[i] = page.PageNumber;
                *(long*)page.DataPointer = page.PageNumber;
            }

            tx.Commit();
        }

        Exception failure = null;
        void Fail(Exception e) => Interlocked.CompareExchange(ref failure, e, null);

        using (var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
        {
            var token = stop.Token;

            var writer = new Thread(() =>
            {
                try
                {
                    var rng = new Random(1);
                    var tempPage = -1L;
                    for (var iteration = 0L; token.IsCancellationRequested == false; iteration++)
                    {
                        using (var tx = Env.WriteTransaction())
                        {
                            var llt = tx.LowLevelTransaction;
                            for (var i = 0; i < 16; i++)
                            {
                                var pageNumber = pages[rng.Next(pageCount)];
                                var page = llt.ModifyPage(pageNumber);
                                *(long*)page.DataPointer = pageNumber;
                                *((long*)page.DataPointer + 1) = iteration;
                            }

                            // allocate-then-free churn, so remove tombstones flow through the chains too
                            if (tempPage != -1)
                                llt.FreePage(tempPage);
                            tempPage = llt.AllocatePage(1).PageNumber;

                            // a rebuild prunes and re-packs every chain while readers keep walking the
                            // previous generation
                            if (iteration % 512 == 511)
                                Env.ScratchPagesTable.ForceRebuildForTests();

                            tx.Commit();
                        }

                        if (iteration % 64 == 0)
                            Env.FlushLogToDataFile();
                    }
                }
                catch (Exception e)
                {
                    Fail(e);
                }
            });

            // heavy oversubscription on purpose: the race window is the two loads between following a
            // chain link and reading the entry's sequence, and it only opens wide when a reader gets
            // preempted right there - which is how the indexing workload that found this bug behaves
            var readers = new Thread[Environment.ProcessorCount * 4];
            for (var r = 0; r < readers.Length; r++)
            {
                // staggered snapshot ages: long-held transactions keep old bounds alive, so pruning runs
                // against a mix of bounds and takes the precise, entry-dropping path
                var seed = r;
                var holdReads = 1024 << (r % 6);
                readers[r] = new Thread(() =>
                {
                    try
                    {
                        var rng = new Random(17 + holdReads + seed);
                        while (token.IsCancellationRequested == false)
                        {
                            using (var tx = Env.ReadTransaction())
                            {
                                for (var i = 0; i < holdReads; i++)
                                {
                                    var pageNumber = pages[rng.Next(pageCount)];

                                    // bypass the transaction's page cache - every read must resolve
                                    // through the scratch table snapshot, the code under test
                                    var page = tx.LowLevelTransaction.GetPageWithoutCache(pageNumber);
                                    var stamp = *(long*)page.DataPointer;
                                    if (page.PageNumber != pageNumber || stamp != pageNumber)
                                        throw new InvalidOperationException(
                                            $"Asked for page {pageNumber} but the snapshot resolved a page " +
                                            $"with header {page.PageNumber} and stamp {stamp}");
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Fail(e);
                    }
                });
            }

            writer.Start();
            foreach (var reader in readers)
                reader.Start();

            writer.Join();
            foreach (var reader in readers)
                reader.Join();
        }

        if (failure != null)
            throw failure;
    }

    private void WriteUniqueKeysRound(int round, Dictionary<string, string> expected)
    {
        var value = new string((char)('a' + round % 26), 1500);

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree("tree");
            for (var i = 0; i < 400; i++)
            {
                var key = $"unique/{round:D3}/{i:D4}";
                tree.Add(key, value);
                expected[key] = value;
            }

            tx.Commit();
        }
    }

    private void WriteRoundWithAsyncCommits(int round, Dictionary<string, string> expected)
    {
        var value = new string((char)('a' + round % 26), 1500);

        var tx = Env.WriteTransaction();
        try
        {
            for (var chain = 0; chain < 10; chain++)
            {
                var tree = tx.CreateTree("tree");

                // same keys and same value sizes every round: after the first round builds the tree,
                // every later round is pure in-place page modification, journaled as diffs - the diffs
                // are only recoverable if the flushed base actually reached the data file
                for (var i = 0; i < 20; i++)
                {
                    var key = $"key/{chain:D2}/{i:D3}";
                    tree.Add(key, value + key);
                    expected[key] = value + key;
                }

                var next = tx.BeginAsyncCommitAndStartNewTransaction(tx.LowLevelTransaction.PersistentContext);
                using (tx)
                {
                    tx.EndAsyncCommit();
                }

                tx = next;
            }

            tx.Commit();
        }
        finally
        {
            tx.Dispose();
        }
    }

    private void AssertAllReadable(Dictionary<string, string> expected)
    {
        using (var rtx = Env.ReadTransaction())
        {
            var tree = rtx.ReadTree("tree");
            Assert.NotNull(tree);

            foreach (var (key, value) in expected)
            {
                var read = tree.Read(key);
                Assert.True(read != null, $"'{key}' is unreadable after restart");
                Assert.Equal(value, read.Reader.ToStringValue());
            }
        }
    }
}
