using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Sparrow;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron.NextGenPagers;

public class SparseRegions(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void CanReleaseDiskSpaceBackToTheOperatingSystem()
    {
        Options.ManualFlushing = true;
        Options.ManualSyncing = true;
        var pages = new List<long>();
        using (var wtx = Env.WriteTransaction())
        {
            for (int i = 0; i < 32; i++)
            {
                // 2MB
                Page allocatePage = wtx.LowLevelTransaction.AllocatePage(256);
                allocatePage.Flags = PageFlags.Overflow | PageFlags.Single;
                allocatePage.OverflowSize = (256 * Constants.Storage.PageSize) - PageHeader.SizeOf;
                // must force an allocation of the data, beacuse Mac will not allocate the space until it is actually used
                // and non zero data is written to it
                Memory.Set(allocatePage.DataPointer, 1, allocatePage.OverflowSize);
                pages.Add(allocatePage.PageNumber);
            }
            wtx.Commit();
        }

        using (Env.ReadTransaction())
        {

        }
        // Before flushing
        Assert.Equal(64 * 1024, Env.CurrentStateRecord.DataPagerState.TotalAllocatedSize);
        Env.FlushLogToDataFile();
        // We allocated 64 MB + 2 pages early on, so we expand to 128MB
        Assert.Equal(128 * 1024 * 1024, Env.CurrentStateRecord.DataPagerState.TotalAllocatedSize);
        using (var wtx = Env.WriteTransaction())
        {
            // delete range of ~14MB - 40MB, expect to free: 16MB - 32MB
            for (int i = 7; i < 20; i++)
            { 
                for (int j = 0; j < 256; j++)
                {
                    wtx.LowLevelTransaction.FreePage(pages[i] + j);
                }
            }

            wtx.Commit();
        }
        Assert.Equal([(2048, 2048), (4096, 1026)], Env.CurrentStateRecord.SparseRegions);

        using (var wtx = Env.WriteTransaction())
        {
            // delete range of ~40MB - 50MB, expect to free: 32MB - 48MB
            for (int i = 20; i < 26; i++)
            {
                for (int j = 0; j < 256; j++)
                {
                    wtx.LowLevelTransaction.FreePage(pages[i] + j);
                }
            }

            wtx.Commit();
        }
        // proof that we can release regions released across multiple transactions
        Assert.Equal([(4096, 2048), (6144, 514)], Env.CurrentStateRecord.SparseRegions);
        (_, long beforePhysicalSize) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);

        Env.FlushLogToDataFile();
        Assert.Equal(128 * 1024 * 1024, Env.CurrentStateRecord.DataPagerState.TotalAllocatedSize);

        // RavenDB-26910: hole-punching is deferred to the post-sync phase, so force a sync to actually reclaim the space
        using (var syncOperation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            Assert.True(syncOperation.SyncDataFile());
        }

        (long allocatedSize, long physicalSize) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);

        // On Linux, we have to deal with hole punching being done on 4KB boundaries, but the file system is 
        // storing sectors using 512 bytes. So if we aren't aligned on 4KB on the disk, hole punching may not actually
        // clear all the blocks. We give ourselves a maximum of 8KB spare for this reason
        Assert.Equal(allocatedSize, Env.CurrentStateRecord.DataPagerState.TotalAllocatedSize);
        const long amountOfSpaceSaved = (36 * 1024 * 1024);
        if (PlatformDetails.RunningOnMacOsx is false)
        {
            long expectedSize = allocatedSize - amountOfSpaceSaved;
            Assert.True(Math.Abs(expectedSize - physicalSize) <= 8192 * 2,
            $"Expected size: {new Size(expectedSize, SizeUnit.Bytes)}, actual size: {new Size(physicalSize, SizeUnit.Bytes)}");
        }
        else
        {
            // MacOS is doing weird stuff here, because it is eagerly marking the file as sparse
            // so we'll just verify that we were able to save _some_ space.
            Assert.True((beforePhysicalSize - physicalSize) > 20*1024*1024);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void DeferredPunch_DoesNotZeroPageReusedInLaterFlushBatch()
    {
        // RavenDB-26910: the hole-punch is deferred to the post-sync phase. A page freed in one flush batch can be reused + written
        // by a later batch before the punch runs; that later flush subtracts its writes from the pending regions (SubtractRanges),
        // so the reused page is never zeroed. If the subtraction is removed, this fails.
        RequireFileBasedPager();
        Options.ManualFlushing = true;
        Options.ManualSyncing = true;

        const int allocationSize = 600;
        const int overflowSize = allocationSize * Constants.Storage.PageSize - PageHeader.SizeOf;

        long pageNum;
        using (var tx = Env.WriteTransaction())
        {
            var p = tx.LowLevelTransaction.AllocatePage(allocationSize);
            p.Flags |= PageFlags.Overflow;
            p.OverflowSize = overflowSize;
            pageNum = p.PageNumber;
            p.AsSpan(PageHeader.SizeOf, overflowSize).Fill(1);
            tx.Commit();
        }

        Env.FlushLogToDataFile();
        using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            Assert.True(sync.SyncDataFile());

        // batch N: free the pages (records a sparse-region candidate), flush without syncing -> the punch is deferred, not yet done
        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < allocationSize; i++)
                tx.LowLevelTransaction.FreePage(pageNum + i);
            tx.Commit();
        }
        Env.FlushLogToDataFile();

        // batch N+1: reuse the same pages and write new content, flush -> the deferred punch from batch N has still not run
        using (var tx = Env.WriteTransaction())
        {
            var p = tx.LowLevelTransaction.AllocatePage(allocationSize);
            Assert.Equal(pageNum, p.PageNumber);
            p.Flags |= PageFlags.Overflow;
            p.OverflowSize = overflowSize;
            p.AsSpan(PageHeader.SizeOf, overflowSize).Fill(2);
            tx.Commit();
        }
        Env.FlushLogToDataFile();

        // now sync: batch N's deferred punch runs over a clean section. Batch N+1 rewrote these pages and subtracted them from the
        // pending regions, so they are excluded from the punch and keep their content.
        using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            Assert.True(sync.SyncDataFile());

        using (var tx = Env.WriteTransaction())
        {
            var p = tx.LowLevelTransaction.GetPage(pageNum);
            Assert.Equal(overflowSize, p.OverflowSize);
            Assert.False(p.AsSpan(PageHeader.SizeOf, overflowSize).ContainsAnyExcept((byte)2),
                "Reused page was zeroed by the deferred sparse-region punch");
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void DeferredPunch_DoesNotZeroPagesStillReadByOlderReadTransaction()
    {
        // RavenDB-26910: a page freed by a transaction newer than an open read tx still has its last flushed content in
        // the data file, and the reader resolves it from there - the deferred punch must not zero it (it may only cover
        // frees older than every active read transaction, the same uptoTxIdExclusive bound the flush obeys)
        RequireFileBasedPager();
        Options.ManualFlushing = true;
        Options.ManualSyncing = true;

        // the flushed free must push the section's free count above NumberOfFreePagesForSparseConsideration (512)
        // to register a sparse candidate; the reader's run only needs to be a punchable >= 128 pages
        const int sectionMatePages = 600;
        const int readerPages = 256;
        const int readerOverflowSize = readerPages * Constants.Storage.PageSize - PageHeader.SizeOf;

        long sectionMate;
        long readerPage;
        using (var tx = Env.WriteTransaction())
        {
            var p1 = tx.LowLevelTransaction.AllocatePage(sectionMatePages);
            p1.Flags |= PageFlags.Overflow;
            p1.OverflowSize = sectionMatePages * Constants.Storage.PageSize - PageHeader.SizeOf;
            sectionMate = p1.PageNumber;

            var p2 = tx.LowLevelTransaction.AllocatePage(readerPages);
            p2.Flags |= PageFlags.Overflow;
            p2.OverflowSize = readerOverflowSize;
            p2.AsSpan(PageHeader.SizeOf, readerOverflowSize).Fill(3);
            readerPage = p2.PageNumber;

            tx.Commit();
        }

        Env.FlushLogToDataFile();

        // both runs must share a free-space section, so the flushed free below makes the whole section a punch candidate
        Assert.Equal(sectionMate / FreeSpaceHandling.NumberOfPagesInSection,
            (readerPage + readerPages - 1) / FreeSpaceHandling.NumberOfPagesInSection);

        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < sectionMatePages; i++)
                tx.LowLevelTransaction.FreePage(sectionMate + i);
            tx.Commit();
        }

        // the free above is flushed, so its sparse region is pending the deferred punch
        Env.FlushLogToDataFile();

        using (var rtx = Env.ReadTransaction())
        {
            // freed by a transaction newer than rtx
            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < readerPages; i++)
                    tx.LowLevelTransaction.FreePage(readerPage + i);
                tx.Commit();
            }

            using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
                Assert.True(sync.SyncDataFile());

            var page = rtx.LowLevelTransaction.GetPage(readerPage);
            Assert.Equal(readerOverflowSize, page.OverflowSize);
            Assert.False(page.AsSpan(PageHeader.SizeOf, readerOverflowSize).ContainsAnyExcept((byte)3),
                "Page freed by a transaction newer than the read tx was zeroed by the deferred sparse-region punch");
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void DeferredPunch_MidFlushPunchDoesNotZeroPageThisFlushRewrote()
    {
        // RavenDB-26910 (C1): the deferred punch can execute on the FLUSH thread, mid-flush. When a user write transaction holds
        // the tx lock, WaitForJournalStateToBeUpdated times out acquiring it and calls RunTaskIfNotAlreadyRan, which executes a
        // queued sync's UpdateDatabaseStateAfterSync (including the punch) while the flush - which already wrote a reallocated
        // page - still holds _flushingLock. Pending must be reconciled (this flush's writes subtracted) BEFORE
        // ApplyJournalStateAfterFlush, or that punch zeroes the just-written page. This test builds the real interleaving:
        // a held write tx forces the flusher into the timeout branch, and a real SyncOperation gets its punch executed mid-flush.
        RequireFileBasedPager();
        Options.ManualFlushing = true;
        Options.ManualSyncing = true;

        const int pages = 600; // > NumberOfFreePagesForSparseConsideration (512), so freeing the run yields a punchable region
        const int overflowSize = pages * Constants.Storage.PageSize - PageHeader.SizeOf;

        long p;
        using (var tx = Env.WriteTransaction())
        {
            var page = tx.LowLevelTransaction.AllocatePage(pages);
            page.Flags |= PageFlags.Overflow;
            page.OverflowSize = overflowSize;
            page.AsSpan(PageHeader.SizeOf, overflowSize).Fill(1);
            p = page.PageNumber;
            tx.Commit();
        }
        Env.FlushLogToDataFile();

        // F1: free the run - its region enters _pendingSparseRegions and stays there (no sync yet)
        using (var tx = Env.WriteTransaction())
        {
            for (int i = 0; i < pages; i++)
                tx.LowLevelTransaction.FreePage(p + i);
            tx.Commit();
        }
        Env.FlushLogToDataFile();

        // reallocate the same run and write new content - flushed by F2 below
        using (var tx = Env.WriteTransaction())
        {
            var page = tx.LowLevelTransaction.AllocatePage(pages);
            Assert.Equal(p, page.PageNumber);
            page.Flags |= PageFlags.Overflow;
            page.OverflowSize = overflowSize;
            page.AsSpan(PageHeader.SizeOf, overflowSize).Fill(2);
            tx.Commit();
        }

        using (var txLockHeld = new ManualResetEventSlim())
        using (var releaseTxLock = new ManualResetEventSlim())
        using (var flushWrotePages = new ManualResetEventSlim())
        {
            // hold the write tx lock so F2's WaitForJournalStateToBeUpdated keeps timing out and runs the sync's tasks itself
            var txHolder = Task.Run(() =>
            {
                using (Env.WriteTransaction())
                {
                    txLockHeld.Set();
                    releaseTxLock.Wait(TimeSpan.FromSeconds(60));
                }
            });
            Assert.True(txLockHeld.Wait(TimeSpan.FromSeconds(60)));

            // fires on the flush thread after F2 wrote the reallocated page, right before it gets stuck on the tx lock
            Env.Journal.Applicator.ForTestingPurposesOnly().OnApplyJournalStateAfterFlush += flushWrotePages.Set;

            var flushTask = Task.Run(() => Env.FlushLogToDataFile());
            try
            {
                Assert.True(flushWrotePages.Wait(TimeSpan.FromSeconds(60)), "expected F2 to write its pages and reach ApplyJournalStateAfterFlush");

                // a real sync: its GatherInformationToStartSync and UpdateDatabaseStateAfterSync (punch included) cannot take
                // _flushingLock, so both are executed by the stuck F2 flush thread via RunTaskIfNotAlreadyRan - the punch runs mid-flush
                using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
                    Assert.True(sync.SyncDataFile());
            }
            finally
            {
                releaseTxLock.Set();
            }

            Assert.True(flushTask.Wait(TimeSpan.FromSeconds(60)), "expected F2 to complete after the tx lock was released");
            Assert.True(txHolder.Wait(TimeSpan.FromSeconds(60)));
        }

        using (var tx = Env.ReadTransaction())
        {
            var page = tx.LowLevelTransaction.GetPage(p);
            Assert.Equal(overflowSize, page.OverflowSize);
            Assert.False(page.AsSpan(PageHeader.SizeOf, overflowSize).ContainsAnyExcept((byte)2),
                "Page rewritten by the in-progress flush was zeroed by the mid-flush deferred punch");
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WillReleaseFreeSpaceAfterRestart()
    {
        RequireFileBasedPager();
        Options.ManualFlushing = true;
        var pages = new List<long>();

        using (var wtx = Env.WriteTransaction())
        {
            for (int i = 0; i < 32; i++)
            {
                // 2MB
                Page allocatePage = wtx.LowLevelTransaction.AllocatePage(256);
                allocatePage.Flags = PageFlags.Overflow | PageFlags.Single;
                allocatePage.OverflowSize = (256 * Constants.Storage.PageSize) - PageHeader.SizeOf;
                pages.Add(allocatePage.PageNumber);
            }
            for (int i = 0; i < 32; i++)
            {
                for (int j = 0; j < 256; j++)
                {
                    wtx.LowLevelTransaction.FreePage(pages[i] + j);
                }
            }
            wtx.Commit();
        }
        RestartDatabase();
        Assert.Equal([(2, 2046), (2048, 2048), (4096, 2048), (6144, 2048)], Env.CurrentStateRecord.SparseRegions);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void MergeSparseRegions_ShouldReturnMinimumConsolidatedRanges()
    {
        var inputSource = new List<(long Start, long Count)>
        {
            (4703, 1), (4487, 1), (4704, 1), (4707, 9), (4717, 7), (4703, 1), (4714, 3), (4735, 9),
            (4718, 6), (4717, 1), (4738, 6), (4787, 7), (4718, 6), (4743, 1), (4787, 7), (4718, 6),
            (4743, 1), (4787, 7), (4809, 6), (4718, 2), (4720, 4), (4743, 1), (4787, 7), (4809, 2),
            (4813, 2), (4835, 2), (4722, 2), (4743, 1), (4787, 7), (4809, 2), (4813, 2), (4835, 2),
            (4853, 2), (4722, 2), (4743, 1), (4787, 7), (4809, 2), (4813, 2), (4835, 2), (4853, 2),
            (4857, 2), (4788, 6), (4809, 2), (4813, 2), (4835, 2), (4853, 2), (4857, 2), (4875, 2),
            (4788, 2), (4790, 4), (4809, 2), (4813, 2), (4835, 2), (4853, 2), (4857, 2), (4875, 2),
            (4879, 2), (4792, 2), (4809, 2), (4813, 2), (4835, 2), (4853, 2), (4857, 2), (4875, 2),
            (4879, 2), (4897, 2)
        };

        var expectedResult = new List<(long Start, long Count)>
        {
            (4487, 1),
            (4703, 2),
            (4707, 17),
            (4735, 9),
            (4787, 7),
            (4809, 6),
            (4835, 2),
            (4853, 2),
            (4857, 2),
            (4875, 2),
            (4879, 2),
            (4897, 2)
        };

        StorageEnvironment.MergeSparseRegions(inputSource);

        Assert.Equal(expectedResult , inputSource);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void SubtractRanges_ShouldRemoveFlushedPagesFromSparseRegions()
    {
        static void AssertSubtract(
            List<(long Start, long Count)> sparseRegions,
            List<(long Start, long Count)> flushedPageRanges,
            List<(long Start, long Count)> expected)
        {
            WriteAheadJournal.JournalApplicator.SubtractRanges(sparseRegions, flushedPageRanges);
            Assert.Equal(expected, sparseRegions);
        }

        // no overlap
        AssertSubtract([(100, 50)], [(10, 20), (200, 30)], [(100, 50)]);

        // touching boundaries are not overlaps
        AssertSubtract([(100, 50)], [(80, 20), (150, 10)], [(100, 50)]);

        // exact cover and superset remove the region entirely
        AssertSubtract([(100, 50)], [(100, 50)], []);
        AssertSubtract([(100, 50)], [(90, 100)], []);

        // trim head, trim tail
        AssertSubtract([(100, 50)], [(90, 20)], [(110, 40)]);
        AssertSubtract([(100, 50)], [(140, 20)], [(100, 40)]);

        // a flushed range strictly inside splits the region
        AssertSubtract([(100, 50)], [(120, 10)], [(100, 20), (130, 20)]);

        // multiple holes in one region
        AssertSubtract([(100, 100)], [(110, 10), (150, 10), (190, 20)], [(100, 10), (120, 30), (160, 30)]);

        // one flushed range spanning two regions must trim both
        AssertSubtract([(100, 50), (200, 50)], [(140, 70)], [(100, 40), (210, 40)]);

        // empty inputs
        AssertSubtract([], [(100, 50)], []);
        AssertSubtract([(100, 50)], [], [(100, 50)]);
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineDataWithRandomSeed]
    public void SubtractRanges_Fuzzy(int seed)
    {
        var random = new Random(seed);

        for (int i = 0; i < 128; i++)
        {
            // merged pending regions cannot be adjacent (minGap 1); flushed page ranges can be (minGap 0)
            var sparseRegions = GenerateSortedRanges(random, maxEntries: 24, minGap: 1, maxGap: 64, maxCount: 96);
            var flushedPageRanges = GenerateSortedRanges(random, maxEntries: 48, minGap: 0, maxGap: 48, maxCount: 48);

            var expected = SubtractByPage(sparseRegions, flushedPageRanges);

            var actual = new List<(long Start, long Count)>(sparseRegions);
            WriteAheadJournal.JournalApplicator.SubtractRanges(actual, flushedPageRanges);

            Assert.Equal(expected, actual);
        }

        static List<(long Start, long Count)> GenerateSortedRanges(Random random, int maxEntries, int minGap, int maxGap, int maxCount)
        {
            var result = new List<(long Start, long Count)>();
            long cursor = random.Next(0, 64);
            int entries = random.Next(0, maxEntries + 1);
            for (int i = 0; i < entries; i++)
            {
                long start = cursor + random.Next(minGap, maxGap + 1);
                long count = random.Next(1, maxCount + 1);
                result.Add((start, count));
                cursor = start + count;
            }
            return result;
        }

        static List<(long Start, long Count)> SubtractByPage(List<(long Start, long Count)> regions, List<(long Start, long Count)> ranges)
        {
            var pages = new HashSet<long>();
            foreach (var (start, count) in regions)
                for (long page = start; page < start + count; page++)
                    pages.Add(page);

            foreach (var (start, count) in ranges)
                for (long page = start; page < start + count; page++)
                    pages.Remove(page);

            var sorted = new List<long>(pages);
            sorted.Sort();

            var result = new List<(long Start, long Count)>();
            foreach (long page in sorted)
            {
                if (result.Count > 0 && result[^1].Start + result[^1].Count == page)
                    result[^1] = (result[^1].Start, result[^1].Count + 1);
                else
                    result.Add((page, 1));
            }
            return result;
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void StorageReport_ShouldReflectPhysicalDiskSpaceAfterHolePunching()
    {
        Options.ManualFlushing = true;
        Options.ManualSyncing = true;
        var pages = new List<long>();
        using (var wtx = Env.WriteTransaction())
        {
            for (int i = 0; i < 32; i++)
            {
                // 2MB
                Page allocatePage = wtx.LowLevelTransaction.AllocatePage(256);
                allocatePage.Flags = PageFlags.Overflow | PageFlags.Single;
                allocatePage.OverflowSize = (256 * Constants.Storage.PageSize) - PageHeader.SizeOf;
                Memory.Set(allocatePage.DataPointer, 1, allocatePage.OverflowSize);
                pages.Add(allocatePage.PageNumber);
            }
            wtx.Commit();
        }

        Env.FlushLogToDataFile();

        using (var wtx = Env.WriteTransaction())
        {
            // delete ~36MB of data (pages 7-24), which will create sparse regions after flush
            for (int i = 7; i < 25; i++)
            {
                for (int j = 0; j < 256; j++)
                {
                    wtx.LowLevelTransaction.FreePage(pages[i] + j);
                }
            }
            wtx.Commit();
        }

        Env.FlushLogToDataFile();

        // RavenDB-26910: hole-punching is deferred to the post-sync phase, so force a sync to actually reclaim the space
        using (var syncOperation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            Assert.True(syncOperation.SyncDataFile());
        }

        using var rtx = Env.ReadTransaction();
        var report = Env.GenerateReport(rtx);

        // AllocatedSpaceInBytes reflects the total logical file size (128MB)
        Assert.Equal(128 * 1024 * 1024, report.DataFile.AllocatedSpaceInBytes);

        if (PlatformDetails.RunningOnMacOsx is false)
        {
            // After hole punching, PhysicalSpaceInBytes (physical disk space) must be less than AllocatedSpaceInBytes
            Assert.True(report.DataFile.PhysicalSpaceInBytes < report.DataFile.AllocatedSpaceInBytes,
                $"PhysicalSpaceInBytes ({new Size(report.DataFile.PhysicalSpaceInBytes, SizeUnit.Bytes)}) should be less than " +
                $"AllocatedSpaceInBytes ({new Size(report.DataFile.AllocatedSpaceInBytes, SizeUnit.Bytes)}) after hole punching");
        }
    }
}
