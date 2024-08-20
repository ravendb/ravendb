using System.Collections.Generic;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.NextGenPagers;

public class SparseRegions(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CanReleaseDiskSpaceBackToTheOperatingSystem()
    {
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
            wtx.Commit();
        }

        using (Env.ReadTransaction())
        {
            
        }
        // Before flushing
        Assert.Equal(64*1024, Env.CurrentStateRecord.DataPagerState.TotalAllocatedSize);
        Env.FlushLogToDataFile();
        // We allocated 64 MB + 2 pages early on, so we expand to 128MB
        Assert.Equal(128*1024*1024, Env.CurrentStateRecord.DataPagerState.TotalAllocatedSize);
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
        Assert.Equal([2048], Env.CurrentStateRecord.SparsePageRanges);
        
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
        Assert.Equal([4096], Env.CurrentStateRecord.SparsePageRanges);
        
        Env.FlushLogToDataFile();
        Assert.Equal(128*1024*1024, Env.CurrentStateRecord.DataPagerState.TotalAllocatedSize);

        (long allocatedSize, long physicalSize) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);
        
        Assert.Equal(allocatedSize, Env.CurrentStateRecord.DataPagerState.TotalAllocatedSize);
        Assert.Equal(allocatedSize - (32 * 1024 * 1024), physicalSize);
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
        
        Assert.Equal([2048, 4096, 6144], Env.CurrentStateRecord.SparsePageRanges);
    }
}
