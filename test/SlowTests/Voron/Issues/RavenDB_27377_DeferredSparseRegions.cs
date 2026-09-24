using System.Collections.Generic;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.Journal;
using Xunit;
using Constants = Voron.Global.Constants;

namespace SlowTests.Voron.Issues;

// On Windows a hole punch costs time proportional to the size of the mapped section, not to the size of the hole, and
// NTFS holds the file's paging resource for the whole call - so punching inside the sync cycle stalls every reader of
// the data file. With PunchSparseRegionsOnIdleOnly the regions are accumulated instead and punched from the idle timer.
public class RavenDB_27377_DeferredSparseRegions : StorageTest
{
    public RavenDB_27377_DeferredSparseRegions(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.PunchSparseRegionsOnIdleOnly = true;
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void SyncDoesNotPunchWhenPunchingIsDeferredToIdle()
    {
        RequireFileBasedPager();

        FreeALargeRangeOfPages();

        Env.FlushLogToDataFile();

        (long allocatedBefore, long physicalBefore) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);

        using (var syncOperation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            Assert.True(syncOperation.SyncDataFile());
        }

        (long allocatedAfterSync, long physicalAfterSync) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);

        Assert.Equal(allocatedBefore, allocatedAfterSync);
        Assert.Equal(physicalBefore, physicalAfterSync);
        Assert.True(Env.Journal.Applicator.HasPendingSparseRegions, "the freed regions should be waiting for the idle punch");

        // the idle timer only punches once the environment has been quiet for long enough
        Env.Options.TimeToPunchSparseRegionsAfterIdle = System.TimeSpan.Zero;
        Assert.True(Env.HasBeenIdleLongEnoughToPunchSparseRegions());

        while (Env.Journal.Applicator.HasPendingSparseRegions)
            Env.Journal.Applicator.PunchPendingSparseRegionsOnIdle();

        (long allocatedAfterIdle, long physicalAfterIdle) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);

        Assert.Equal(allocatedBefore, allocatedAfterIdle);
        Assert.True(physicalAfterIdle < allocatedAfterIdle - (32L * Constants.Size.Megabyte),
            $"Expected the idle punch to reclaim at least 32MB, but allocated={new Size(allocatedAfterIdle, SizeUnit.Bytes)}, " +
            $"physical={new Size(physicalAfterIdle, SizeUnit.Bytes)}");
    }

    private unsafe void FreeALargeRangeOfPages()
    {
        var pages = new List<long>();

        using (var txw = Env.WriteTransaction())
        {
            for (int i = 0; i < 32; i++)
            {
                Page allocatePage = txw.LowLevelTransaction.AllocatePage(256);
                allocatePage.Flags = PageFlags.Overflow | PageFlags.Single;
                allocatePage.OverflowSize = (256 * Constants.Storage.PageSize) - PageHeader.SizeOf;

                Memory.Set(allocatePage.DataPointer, 1, allocatePage.OverflowSize);
                pages.Add(allocatePage.PageNumber);
            }

            txw.Commit();
        }

        Env.FlushLogToDataFile();

        using (var txw = Env.WriteTransaction())
        {
            for (int i = 0; i < 32; i++)
            {
                for (int j = 0; j < 256; j++)
                    txw.LowLevelTransaction.FreePage(pages[i] + j);
            }

            txw.Commit();
        }
    }
}
