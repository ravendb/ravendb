using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Sparrow;
using Sparrow.Backups;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Backup;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;
using Voron.Util.Settings;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27443 : StorageTest
{
    public RavenDB_27443(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.BackupExportImport)]
    public unsafe void SnapshotRestoreShouldPreserveHolesSmallerThanSectionCandidacyThreshold()
    {
        RequireFileBasedPager();
        Options.ManualFlushing = true;
        Options.ManualSyncing = true;

        // each freed run is above the punch granularity but below the section candidacy gate that the restore used to reuse as its threshold
        const int overflowPages = 256;
        Assert.True(overflowPages >= FreeSpaceHandling.MinNumberOfContiguousFreePagesForSparseRegion);
        Assert.True(overflowPages < FreeSpaceHandling.MinNumberOfFreePagesInSectionForSparseConsideration);

        const int numberOfOverflows = 32; // 64 MB, 4 free-space sections

        var pages = new List<long>();

        using (var txw = Env.WriteTransaction())
        {
            for (int i = 0; i < numberOfOverflows; i++)
            {
                Page page = txw.LowLevelTransaction.AllocatePage(overflowPages);
                page.Flags = PageFlags.Overflow | PageFlags.Single;
                page.OverflowSize = (overflowPages * Constants.Storage.PageSize) - PageHeader.SizeOf;

                Memory.Set(page.DataPointer, (byte)(i + 1), page.OverflowSize);
                pages.Add(page.PageNumber);
            }

            txw.Commit();
        }

        Env.FlushLogToDataFile();

        // free every other overflow: 16 separate 2 MB runs, every section ends up with 1024 free pages so it passes the candidacy gate and gets punched
        using (var txw = Env.WriteTransaction())
        {
            for (int i = 0; i < numberOfOverflows; i += 2)
            {
                for (int j = 0; j < overflowPages; j++)
                {
                    txw.LowLevelTransaction.FreePage(pages[i] + j);
                }
            }

            txw.Commit();
        }

        List<(long Start, long Count)> punchedRegions = Env.CurrentStateRecord.SparseRegions;
        Assert.NotNull(punchedRegions);
        Assert.True(punchedRegions.Sum(r => r.Count) >= (numberOfOverflows / 2) * overflowPages);
        Assert.All(punchedRegions, r => Assert.True(r.Count < FreeSpaceHandling.MinNumberOfFreePagesInSectionForSparseConsideration));

        Env.FlushLogToDataFile();

        using (var syncOperation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            Assert.True(syncOperation.SyncDataFile());
        }

        (long originalAllocated, long originalPhysical) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);

        var voronDataDir = new VoronPathSetting(DataDir);
        var backupPath = voronDataDir.Combine("voron-test.backup");

        BackupMethods.Full.ToFile(Env, backupPath, SnapshotBackupCompressionAlgorithm.Zstd);

        var restoreDir = voronDataDir.Combine("restored");
        BackupMethods.Full.Restore(backupPath, restoreDir);

        var options = StorageEnvironmentOptions.ForPathForTests(restoreDir.FullPath);
        options.MaxLogFileSize = Env.Options.MaxLogFileSize;
        options.DisableSparseRegions = true; // the load-time re-punch must not mask what the restore itself produced

        using (var restoredEnv = new StorageEnvironment(options))
        {
            (long restoredAllocated, long restoredPhysical) = restoredEnv.DataPager.GetFileSize(restoredEnv.CurrentStateRecord.DataPagerState);

            Assert.Equal(originalAllocated, restoredAllocated);

            if (PlatformDetails.RunningOnMacOsx)
                return;

            // the file may have grown past the 32 overflows; that zero tail is a hole either way, so bound the physical size by the live data instead of comparing to the source
            const long liveDataBytes = (numberOfOverflows / 2) * overflowPages * (long)Constants.Storage.PageSize;
            const long tolerance = 2L * Constants.Size.Megabyte;

            Assert.True(restoredPhysical >= liveDataBytes && restoredPhysical <= liveDataBytes + tolerance,
                $"Expected the restored data file to physically hold only the {new Size(liveDataBytes, SizeUnit.Bytes)} of live pages, " +
                $"but physical={new Size(restoredPhysical, SizeUnit.Bytes)}, allocated={new Size(restoredAllocated, SizeUnit.Bytes)}, " +
                $"source physical={new Size(originalPhysical, SizeUnit.Bytes)}, source allocated={new Size(originalAllocated, SizeUnit.Bytes)}");
        }
    }
}
