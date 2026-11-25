using System.Collections.Generic;
using FastTests.Voron;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27844(ITestOutputHelper output) : StorageTest(output)
{
    private const long Mb = 1024 * 1024;

    [RavenFact(RavenTestCategory.Voron)]
    public void GrowingTheDataFileMustNotReallocatePunchedHoles()
    {
        RequireFileBasedPager();
        Options.ManualFlushing = true;
        Options.ManualSyncing = true;

        var pages = new List<long>();
        using (var wtx = Env.WriteTransaction())
        {
            for (int i = 0; i < 32; i++)
            {
                // 2MB
                var page = wtx.LowLevelTransaction.AllocatePage(256);
                page.Flags = PageFlags.Overflow | PageFlags.Single;
                page.OverflowSize = 256 * Constants.Storage.PageSize - PageHeader.SizeOf;
                page.AsSpan(PageHeader.SizeOf, page.OverflowSize).Fill(1);
                pages.Add(page.PageNumber);
            }
            wtx.Commit();
        }
        Env.FlushLogToDataFile();

        using (var wtx = Env.WriteTransaction())
        {
            for (int i = 7; i < 25; i++)
            {
                for (int j = 0; j < 256; j++)
                    wtx.LowLevelTransaction.FreePage(pages[i] + j);
            }
            wtx.Commit();
        }
        Env.FlushLogToDataFile();
        using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            Assert.True(sync.SyncDataFile());

        var (allocatedBefore, physicalBefore) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);
        var sparseBefore = allocatedBefore - physicalBefore;
        Assert.True(sparseBefore >= 20 * Mb, $"precondition: the freed pages were not punched, {sparseBefore / Mb}MB sparse");

        // 128MB run does not fit into the holes, so the data file must grow
        using (var wtx = Env.WriteTransaction())
        {
            var page = wtx.LowLevelTransaction.AllocatePage(16 * 1024);
            page.Flags = PageFlags.Overflow | PageFlags.Single;
            page.OverflowSize = 16 * 1024 * Constants.Storage.PageSize - PageHeader.SizeOf;
            page.AsSpan(PageHeader.SizeOf, page.OverflowSize).Fill(2);
            wtx.Commit();
        }
        Env.FlushLogToDataFile();
        using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            Assert.True(sync.SyncDataFile());

        var (allocatedAfter, physicalAfter) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);
        var sparseAfter = allocatedAfter - physicalAfter;
        Assert.True(allocatedAfter > allocatedBefore, "precondition: the data file did not grow");
        if (PlatformDetails.RunningOnMacOsx is false)
            Assert.True(sparseAfter >= sparseBefore - 8192 * 2, $"punched holes were re-allocated: {sparseBefore / Mb}MB sparse before growth, {sparseAfter / Mb}MB after");
    }
}
