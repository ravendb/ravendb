using System.Collections.Generic;
using FastTests.Voron;
using Sparrow;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_26344 : StorageTest
{
    public RavenDB_26344(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        Options.ManualFlushing = true;
        Options.ManualSyncing = true;
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void ShouldApplySparseRegionsOnDatabaseLoad()
    {
        RequireFileBasedPager();
        Options.DisableSparseRegions = true; // initially disable sparse regions to produce a data file without hole-punching

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
                {
                    txw.LowLevelTransaction.FreePage(pages[i] + j);
                }
            }
            txw.Commit();
        }

        Env.FlushLogToDataFile();

        Assert.Null(Env.CurrentStateRecord.SparseRegions);

        (long allocatedBefore, long physicalBefore) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);
        
        Assert.Equal(allocatedBefore, physicalBefore);

        Options.DisableSparseRegions = false; // enable sparse regions before the restart
        
        RestartDatabase();

        Assert.NotNull(Env.CurrentStateRecord.SparseRegions);
        Assert.NotEmpty(Env.CurrentStateRecord.SparseRegions);
        Assert.True(Env.HasAdditionalTransactionsToFlush);

        Env.FlushLogToDataFile();

        (long allocatedAfter, long physicalAfter) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);
        Assert.Equal(allocatedBefore, allocatedAfter);

        Assert.True(physicalAfter < allocatedAfter - (32L * 1024 * 1024),
            $"Expected physical size to drop by at least 32MB after reload, " +
            $"but allocated={new Size(allocatedAfter, SizeUnit.Bytes)}, physical={new Size(physicalAfter, SizeUnit.Bytes)}");
    }

}
