using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Voron;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Issues;

// a page written in tx1 and freed in tx2 is still written by a flush that covers both; that write must not cancel the punch
public class RavenDB_27662 : StorageTest
{
    private const long ValueSize = 32 * Constants.Size.Megabyte;
    private const int Pages = 600; // > MinNumberOfFreePagesInSectionForSparseConsideration (512), freeing the run records a punchable region

    public RavenDB_27662(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.ManualFlushing = true;
        options.ManualSyncing = true;
    }

    // idleOnly: true is the Windows default (punched by the idle timer), false is Linux (punched at sync)
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public void FreedValueIsPunched(bool idleOnly, bool flushBetween)
    {
        RequireFileBasedPager();
        Options.PunchSparseRegionsOnIdleOnly = idleOnly;

        byte[] value = new byte[ValueSize];
        value.AsSpan().Fill(1);

        using (var tx = Env.WriteTransaction())
        {
            tx.CreateTree("t").Add("big", value);
            tx.Commit();
        }

        if (flushBetween)
            Env.FlushLogToDataFile();

        using (var tx = Env.WriteTransaction())
        {
            tx.ReadTree("t").Delete("big");
            tx.Commit();
        }

        long recorded = Env.CurrentStateRecord.SparseRegions?.Sum(r => r.Count) * Constants.Storage.PageSize ?? 0;
        Assert.True(recorded >= ValueSize * 3 / 4, $"the delete should record the freed run, recorded {new Size(recorded, SizeUnit.Bytes)}");

        Env.FlushLogToDataFile();
        Assert.True(Env.Journal.Applicator.HasPendingSparseRegions, "the freed run should be pending after the flush");

        SyncAndPunch();
        AssertHoleOfAtLeast((int)(ValueSize / Constants.Storage.PageSize));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void RunReallocatedInTheSameFlushIsNotPunched()
    {
        RequireFileBasedPager();

        long a;
        using (var tx = Env.WriteTransaction())
        {
            a = AllocateRun(tx, Pages, 1);
            tx.Commit();
        }
        FreeRun(a, Pages);
        Reallocate(a, 2);

        Env.FlushLogToDataFile();
        Assert.False(Env.Journal.Applicator.HasPendingSparseRegions, "the run was reallocated after the free, it must not be punched");

        SyncAndPunch();
        AssertRunContent(a, Pages, 2);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void FreedReallocatedAndFreedAgainInTheSameFlush()
    {
        RequireFileBasedPager();

        long a;
        using (var tx = Env.WriteTransaction())
        {
            a = AllocateRun(tx, Pages, 1);
            tx.Commit();
        }
        FreeRun(a, Pages);
        Reallocate(a, 2);
        FreeRun(a, Pages);

        Env.FlushLogToDataFile();
        Assert.True(Env.Journal.Applicator.HasPendingSparseRegions, "A is free at the end of the batch, it must be pending");
        SyncAndPunch();
        AssertHoleOfAtLeast(Pages);

        Reallocate(a, 3);
        Env.FlushLogToDataFile();
        SyncAndPunch();
        AssertRunContent(a, Pages, 3);
    }

    // the punch runs on the flush thread (WaitForJournalStateToBeUpdated -> RunTaskIfNotAlreadyRan) right after that flush wrote the freed run
    [RavenFact(RavenTestCategory.Voron)]
    public void MidFlushPunchKeepsOlderWriteOfFreedRunAndSparesSibling()
    {
        RequireFileBasedPager();
        Options.PunchSparseRegionsOnIdleOnly = false;

        // an unsynced flush so the mid-flush sync has work to do (GatherInformationToStartSync bails if the last flush is already synced)
        using (var tx = Env.WriteTransaction())
        {
            AllocateRun(tx, 2, 9);
            tx.Commit();
        }
        Env.FlushLogToDataFile();

        long a, c;
        using (var tx = Env.WriteTransaction())
        {
            a = AllocateRun(tx, Pages, 1);
            c = AllocateRun(tx, Pages, 1);
            tx.Commit();
        }
        FreeRun(a, Pages);

        RunFlushWithMidFlushSync();

        AssertRunContent(c, Pages, 1);
        AssertHoleOfAtLeast(Pages);

        Reallocate(a, 2);
        Env.FlushLogToDataFile();
        SyncAndPunch();
        AssertRunContent(a, Pages, 2);
        AssertRunContent(c, Pages, 1);
    }

    // the restart's load-time scan is the oracle: it must not reclaim more than the flush path did
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(218346199)] // a section that reaches exactly 512 free pages
    [InlineDataWithRandomSeed]
    public void RandomAllocateFreeFlushPunch_MatchesTheLoadTimeScan(int seed)
    {
        RequireFileBasedPager();
        Options.PunchSparseRegionsOnIdleOnly = false;

        var random = new Random(seed);
        var live = new List<(long Start, int Pages, byte Fill)>();
        byte nextFill = 1;

        for (int step = 0; step < 200; step++)
        {
            using (var tx = Env.WriteTransaction())
            {
                int ops = random.Next(1, 4);
                for (int i = 0; i < ops; i++)
                {
                    if (live.Count > 0 && random.Next(3) == 0)
                    {
                        int idx = random.Next(live.Count);
                        var (start, pages, _) = live[idx];
                        for (int p = 0; p < pages; p++)
                            tx.LowLevelTransaction.FreePage(start + p);
                        live.RemoveAt(idx);
                    }
                    else
                    {
                        int pages = random.Next(1, 6) * 128;
                        byte fill = nextFill++;
                        if (nextFill == 0) nextFill = 1;
                        live.Add((AllocateRun(tx, pages, fill), pages, fill));
                    }
                }
                tx.Commit();
            }

            if (random.Next(4) == 0)
                Env.FlushLogToDataFile();
            if (random.Next(8) == 0)
            {
                SyncAndPunch();
                AssertAllLive(live);
            }
        }

        Env.FlushLogToDataFile();
        SyncAndPunch();
        AssertAllLive(live);
        Assert.False(Env.Journal.Applicator.HasPendingSparseRegions);
        (_, long physicalBeforeRestart) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);

        RestartDatabase();
        Env.Options.PunchSparseRegionsOnIdleOnly = false;
        Env.FlushLogToDataFile();
        SyncAndPunch();
        AssertAllLive(live);
        (_, long physicalAfterRestart) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);

        Assert.True(physicalBeforeRestart <= physicalAfterRestart + 2 * Constants.Size.Megabyte,
            $"the restart reclaimed space the flush path missed: before={new Size(physicalBeforeRestart, SizeUnit.Bytes)}, after={new Size(physicalAfterRestart, SizeUnit.Bytes)}, live runs={live.Count}");
    }

    private static int OverflowSize(int pages) => pages * Constants.Storage.PageSize - PageHeader.SizeOf;

    private static long AllocateRun(Transaction tx, int pages, byte fill)
    {
        var p = tx.LowLevelTransaction.AllocatePage(pages);
        p.Flags |= PageFlags.Overflow;
        p.OverflowSize = OverflowSize(pages);
        p.AsSpan(PageHeader.SizeOf, OverflowSize(pages)).Fill(fill);
        return p.PageNumber;
    }

    private void Reallocate(long expected, byte fill)
    {
        using (var tx = Env.WriteTransaction())
        {
            Assert.Equal(expected, AllocateRun(tx, Pages, fill));
            tx.Commit();
        }
    }

    private void FreeRun(long start, int pages)
    {
        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < pages; i++)
                tx.LowLevelTransaction.FreePage(start + i);
            tx.Commit();
        }
    }

    private void RunFlushWithMidFlushSync()
    {
        using (var txLockHeld = new ManualResetEventSlim())
        using (var releaseTxLock = new ManualResetEventSlim())
        using (var flushWrotePages = new ManualResetEventSlim())
        {
            var txHolder = Task.Run(() =>
            {
                using (Env.WriteTransaction())
                {
                    txLockHeld.Set();
                    releaseTxLock.Wait(TimeSpan.FromSeconds(60));
                }
            });
            Assert.True(txLockHeld.Wait(TimeSpan.FromSeconds(60)));

            Action onApply = flushWrotePages.Set;
            Env.Journal.Applicator.ForTestingPurposesOnly().OnApplyJournalStateAfterFlush += onApply;

            var flushTask = Task.Run(() => Env.FlushLogToDataFile());
            try
            {
                Assert.True(flushWrotePages.Wait(TimeSpan.FromSeconds(60)), "expected the flush to write its pages and reach ApplyJournalStateAfterFlush");

                using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
                    Assert.True(sync.SyncDataFile());

                Assert.False(Env.Journal.Applicator.HasPendingSparseRegions, "the mid-flush punch should have consumed pending");
            }
            finally
            {
                releaseTxLock.Set();
                Env.Journal.Applicator.ForTestingPurposesOnly().OnApplyJournalStateAfterFlush -= onApply;
            }

            Assert.True(flushTask.Wait(TimeSpan.FromSeconds(60)), "expected the flush to complete after the tx lock was released");
            Assert.True(txHolder.Wait(TimeSpan.FromSeconds(60)));
        }
    }

    private void SyncAndPunch()
    {
        using (var syncOperation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            syncOperation.SyncDataFile();

        if (Env.Options.PunchSparseRegionsOnIdleOnly == false)
            return;

        Env.Options.TimeToPunchSparseRegionsAfterIdle = TimeSpan.Zero;
        while (Env.Journal.Applicator.HasPendingSparseRegions)
            Env.Journal.Applicator.PunchPendingSparseRegionsOnIdle();
    }

    private void AssertRunContent(long start, int pages, byte fill)
    {
        using (var tx = Env.ReadTransaction())
        {
            var p = tx.LowLevelTransaction.GetPage(start);
            Assert.Equal(OverflowSize(pages), p.OverflowSize);
            Assert.False(p.AsSpan(PageHeader.SizeOf, OverflowSize(pages)).ContainsAnyExcept(fill), $"run at {start} lost its content {fill}");
        }
    }

    private void AssertAllLive(List<(long Start, int Pages, byte Fill)> live)
    {
        foreach (var (start, pages, fill) in live)
            AssertRunContent(start, pages, fill);
    }

    // the file tail counts as physical on NTFS and ext4, so a hole shows up as allocated minus physical
    private void AssertHoleOfAtLeast(int pages)
    {
        (long allocated, long physical) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);
        Assert.True(allocated - physical >= (long)pages * Constants.Storage.PageSize * 3 / 4,
            $"expected a hole of {pages} pages, allocated={new Size(allocated, SizeUnit.Bytes)}, physical={new Size(physical, SizeUnit.Bytes)}");
    }
}
