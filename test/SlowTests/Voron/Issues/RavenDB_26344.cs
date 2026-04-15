using System;
using System.Collections.Generic;
using System.IO;
using FastTests.Voron;
using Sparrow;
using Sparrow.Backups;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Backup;
using Voron.Util.Settings;
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

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.BackupExportImport)]
    [InlineData(SnapshotBackupCompressionAlgorithm.Deflate)]
    [InlineData(SnapshotBackupCompressionAlgorithm.Zstd)]
    public unsafe void SnapshotRestoreShouldPreserveSparsity(SnapshotBackupCompressionAlgorithm compressionAlgorithm)
    {
        RequireFileBasedPager();
        Options.ManualFlushing = true;
        Options.ManualSyncing = true;

        var pages = new List<long>();

        // 1. Create data: 32 overflow pages of 2MB each = 64MB of data
        using (var txw = Env.WriteTransaction())
        {
            for (int i = 0; i < 32; i++)
            {
                Page allocatePage = txw.LowLevelTransaction.AllocatePage(256);
                allocatePage.Flags = PageFlags.Overflow | PageFlags.Single;
                allocatePage.OverflowSize = (256 * Constants.Storage.PageSize) - PageHeader.SizeOf;

                // Write non-zero data so pages are physically allocated
                Memory.Set(allocatePage.DataPointer, (byte)(i + 1), allocatePage.OverflowSize);
                pages.Add(allocatePage.PageNumber);
            }
            txw.Commit();
        }

        Env.FlushLogToDataFile();

        // 2. Free most pages (keep first 2 and last 2 so we can verify data integrity after restore)
        using (var txw = Env.WriteTransaction())
        {
            for (int i = 2; i < 30; i++)
            {
                for (int j = 0; j < 256; j++)
                {
                    txw.LowLevelTransaction.FreePage(pages[i] + j);
                }
            }
            txw.Commit();
        }

        Env.FlushLogToDataFile();

        // 3. Get the original file sizes for reference
        (long originalAllocated, long originalPhysical) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);

        // The data file should be large (the freed pages are zeroed but still allocated)
        Assert.True(originalAllocated >= 64 * 1024 * 1024,
            $"Expected allocated size >= 64MB, but got {new Size(originalAllocated, SizeUnit.Bytes)}");

        // 4. Create a snapshot backup
        var voronDataDir = new VoronPathSetting(DataDir);
        var backupPath = voronDataDir.Combine("voron-test.backup");

        BackupMethods.Full.ToFile(Env, backupPath, compressionAlgorithm);

        // 5. Restore with sparse-aware copy (default behavior)
        var sparseRestoreDir = voronDataDir.Combine("restored-sparse");
        BackupMethods.Full.Restore(backupPath, sparseRestoreDir);

        // 6. Open restored database and verify sizes
        var sparseOptions = StorageEnvironmentOptions.ForPathForTests(sparseRestoreDir.FullPath);
        sparseOptions.MaxLogFileSize = Env.Options.MaxLogFileSize;

        using (var restoredEnv = new StorageEnvironment(sparseOptions))
        {
            (long restoredAllocated, long restoredPhysical) = restoredEnv.DataPager.GetFileSize(restoredEnv.CurrentStateRecord.DataPagerState);

            // The logical (allocated) size should match the original
            Assert.Equal(originalAllocated, restoredAllocated);

            if (PlatformDetails.RunningOnMacOsx == false)
            {
                Assert.True(originalPhysical < originalAllocated - (40L * 1024 * 1024),
                    $"Expected source database to be sparse before backup, but allocated={new Size(originalAllocated, SizeUnit.Bytes)}, physical={new Size(originalPhysical, SizeUnit.Bytes)}");

                const long allowedRestoreOverhead = 16L * 1024 * 1024;
                Assert.True(restoredPhysical <= originalPhysical + allowedRestoreOverhead,
                    $"Expected restored physical size to stay close to the source sparse file after restore, " +
                    $"but source physical={new Size(originalPhysical, SizeUnit.Bytes)}, restored physical={new Size(restoredPhysical, SizeUnit.Bytes)}, allocated={new Size(restoredAllocated, SizeUnit.Bytes)}");
            }
        }
    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.BackupExportImport)]
    [InlineData(SnapshotBackupCompressionAlgorithm.Deflate)]
    [InlineData(SnapshotBackupCompressionAlgorithm.Zstd)]
    public unsafe void SnapshotRestoreWithDisabledSparseRegionsShouldNotCreateSparseFile(SnapshotBackupCompressionAlgorithm compressionAlgorithm)
    {
        RequireFileBasedPager();
        Options.ManualFlushing = true;
        Options.ManualSyncing = true;

        var pages = new List<long>();

        using (var txw = Env.WriteTransaction())
        {
            for (int i = 0; i < 32; i++)
            {
                Page allocatePage = txw.LowLevelTransaction.AllocatePage(256);
                allocatePage.Flags = PageFlags.Overflow | PageFlags.Single;
                allocatePage.OverflowSize = (256 * Constants.Storage.PageSize) - PageHeader.SizeOf;

                Memory.Set(allocatePage.DataPointer, (byte)(i + 1), allocatePage.OverflowSize);
                pages.Add(allocatePage.PageNumber);
            }
            txw.Commit();
        }

        Env.FlushLogToDataFile();

        using (var txw = Env.WriteTransaction())
        {
            for (int i = 2; i < 30; i++)
            {
                for (int j = 0; j < 256; j++)
                {
                    txw.LowLevelTransaction.FreePage(pages[i] + j);
                }
            }
            txw.Commit();
        }

        Env.FlushLogToDataFile();

        (long originalAllocated, _) = Env.DataPager.GetFileSize(Env.CurrentStateRecord.DataPagerState);

        var voronDataDir = new VoronPathSetting(DataDir);
        var backupPath = voronDataDir.Combine("voron-test.backup");

        BackupMethods.Full.ToFile(Env, backupPath, compressionAlgorithm);

        // Restore with sparse regions disabled
        var restoreDir = voronDataDir.Combine("restored-no-sparse");
        BackupMethods.Full.Restore(backupPath, restoreDir, sparseRegionsSupported: false);

        var options = StorageEnvironmentOptions.ForPathForTests(restoreDir.FullPath);
        options.MaxLogFileSize = Env.Options.MaxLogFileSize;

        using (var restoredEnv = new StorageEnvironment(options))
        {
            (long restoredAllocated, long restoredPhysical) = restoredEnv.DataPager.GetFileSize(restoredEnv.CurrentStateRecord.DataPagerState);

            Assert.Equal(originalAllocated, restoredAllocated);

            // When sparse is disabled, physical size should equal allocated size (fully materialized file)
            Assert.Equal(restoredAllocated, restoredPhysical);
        }
    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.BackupExportImport)]
    [InlineData(SnapshotBackupCompressionAlgorithm.Deflate)]
    [InlineData(SnapshotBackupCompressionAlgorithm.Zstd)]
    public unsafe void SnapshotRestoreWithSparseRegionsShouldPreserveDataIntegrity(SnapshotBackupCompressionAlgorithm compressionAlgorithm)
    {
        RequireFileBasedPager();
        Options.ManualFlushing = true;

        // Create a tree with real data that we can verify after restore
        using (var txw = Env.WriteTransaction())
        {
            var tree = txw.CreateTree("test-tree");

            for (int i = 0; i < 100; i++)
            {
                var data = new byte[8192];
                Array.Fill(data, (byte)((i % 255) + 1));
                tree.Add("items/" + i, new MemoryStream(data));
            }

            txw.Commit();
        }

        Env.FlushLogToDataFile();

        // Create large free space by allocating and freeing overflow pages
        var pages = new List<long>();
        using (var txw = Env.WriteTransaction())
        {
            for (int i = 0; i < 16; i++)
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
            for (int i = 0; i < 16; i++)
            {
                for (int j = 0; j < 256; j++)
                {
                    txw.LowLevelTransaction.FreePage(pages[i] + j);
                }
            }
            txw.Commit();
        }

        Env.FlushLogToDataFile();

        var voronDataDir = new VoronPathSetting(DataDir);
        var backupPath = voronDataDir.Combine("voron-test.backup");

        BackupMethods.Full.ToFile(Env, backupPath, compressionAlgorithm);

        // Restore with sparse-aware copy
        var restoreDir = voronDataDir.Combine("restored-data-integrity");
        BackupMethods.Full.Restore(backupPath, restoreDir);

        // Verify all data is intact after sparse restore
        var options = StorageEnvironmentOptions.ForPathForTests(restoreDir.FullPath);
        options.MaxLogFileSize = Env.Options.MaxLogFileSize;

        using (var restoredEnv = new StorageEnvironment(options))
        {
            using (var tx = restoredEnv.ReadTransaction())
            {
                var tree = tx.ReadTree("test-tree");
                Assert.NotNull(tree);

                for (int i = 0; i < 100; i++)
                {
                    var readResult = tree.Read("items/" + i);
                    Assert.NotNull(readResult);

                    byte expectedByte = (byte)((i % 255) + 1);
                    var reader = readResult.Reader;
                    var buffer = new byte[reader.Length];
                    reader.Read(buffer, 0, buffer.Length);

                    for (int b = 0; b < buffer.Length; b++)
                    {
                        Assert.True(buffer[b] == expectedByte,
                            $"Data corruption at item {i}, byte {b}: expected {expectedByte}, got {buffer[b]}");
                    }
                }
            }
        }
    }
}
