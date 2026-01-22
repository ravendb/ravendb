using System;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.NextGenPagers;

public class SparseRegions(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void CanReleaseDiskSpaceBackToTheOperatingSystem()
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
}
