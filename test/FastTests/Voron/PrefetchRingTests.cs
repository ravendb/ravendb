using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Sparrow.Server.Platform.Posix;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron;

public unsafe class PrefetchRingTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const int PosixFadviseDontNeed = 4;

    [RavenFact(RavenTestCategory.Voron)]
    public void DataFilesReleaseTheirRingRegistrationWhenClosed()
    {
        const int maxFiles = 64;
        using var ring = new PrefetchRing(ringEntries: 8, maxFiles, queueSize: 64);
        if (ring.IsAvailable == false)
            return; // Windows / macOS

        var path = RavenTestHelper.NewDataPath(nameof(DataFilesReleaseTheirRingRegistrationWhenClosed), 0, forceCreateDir: true);
        var open = new List<(Pager Pager, Pager.State State)>();
        try
        {
            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.PrefetchRing = ring;
                (Pager, Pager.State) Open(int i) => Pager.Create(options, Path.Combine(path, $"{i}.voron"), 64 * 1024, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                void Close((Pager Pager, Pager.State State) p)
                {
                    p.State.Dispose();
                    p.Pager.Dispose();
                }

                for (var i = 0; i < maxFiles; i++)
                    open.Add(Open(i));

                var full = Assert.Throws<InvalidOperationException>(() => Open(maxFiles));
                Assert.Contains("Storage.PrefetchRingMaxFiles", full.Message);
                Assert.Contains(maxFiles.ToString(), full.Message);
                Assert.ThrowsAny<Exception>(ring.Dispose); // files are still registered

                Close(open[^1]);
                open.RemoveAt(open.Count - 1);
                open.Add(Open(maxFiles)); // the released slot is reused

                foreach (var p in open)
                    Close(p);
                open.Clear();

                // more open / close cycles than the ring has slots
                for (var i = 0; i < 100; i++)
                    Close(Open(i % 3));
            }
        }
        finally
        {
            foreach (var p in open)
            {
                p.State.Dispose();
                p.Pager.Dispose();
            }
            IOExtensions.DeleteDirectory(path);
        }

        ring.Dispose(); // no file is registered any more
        Assert.False(ring.IsAvailable);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ReadPagesAheadCompletesOnceThePagesAreRead()
    {
        using var ring = new PrefetchRing(ringEntries: 8, maxFiles: 4, queueSize: 64);
        if (ring.IsAvailable == false)
            return; // Windows / macOS

        const int pages = 32;
        var path = RavenTestHelper.NewDataPath(nameof(ReadPagesAheadCompletesOnceThePagesAreRead), 0, forceCreateDir: true);
        try
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            var content = new byte[pages * Constants.Storage.PageSize];
            Random.Shared.NextBytes(content);
            File.WriteAllBytes(file, content);

            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.PrefetchRing = ring;
                var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                using (pager)
                using (state)
                {
                    DropFromPageCache(file);
                    var usesIoUring = ring.GetStats()!.Value.UsesIoUring;
                    Output.WriteLine($"io_uring: {usesIoUring}");

                    var read = Pager.ReadPagesAhead(state, [3, 9, 17, 9]);
                    Assert.True(read.AsTask().Wait(TimeSpan.FromSeconds(10)));
                    var stats = ring.GetStats()!.Value;
                    Assert.Equal(usesIoUring ? 1 : 0, stats.ReadGroups); // without io_uring nothing waits for the reads
                    if (usesIoUring)
                    {
                        // completed means read: every 4 KB of each page is in memory now
                        Assert.Equal(3, stats.Reads);
                        foreach (var page in new long[] { 3, 9, 17 })
                            Assert.Equal(Constants.Storage.PageSize / Environment.SystemPageSize, ResidentPages(state.ReadAddress + page * Constants.Storage.PageSize, Constants.Storage.PageSize));
                    }

                    // read a moment ago: no group and no wait
                    Assert.True(Pager.ReadPagesAhead(state, [3, 17]).IsCompletedSuccessfully);
                    Assert.Equal(usesIoUring ? 1 : 0, ring.GetStats()!.Value.ReadGroups);

                    // only the new pages are read, in one group
                    Assert.True(Pager.ReadPagesAhead(state, [3, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]).AsTask().Wait(TimeSpan.FromSeconds(10)));
                    stats = ring.GetStats()!.Value;
                    if (usesIoUring)
                    {
                        Assert.Equal(2, stats.ReadGroups);
                        Assert.Equal(3 + 10, stats.Reads);
                    }
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AGroupLargerThanTheRingIsReadWhole()
    {
        using var ring = new PrefetchRing(ringEntries: 8, maxFiles: 4, queueSize: 64);
        if (ring.IsAvailable == false)
            return;

        const int pages = 256; // 32 times the ring's 8 entries
        var path = RavenTestHelper.NewDataPath(nameof(AGroupLargerThanTheRingIsReadWhole), 0, forceCreateDir: true);
        try
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            var content = new byte[pages * Constants.Storage.PageSize];
            Random.Shared.NextBytes(content);
            File.WriteAllBytes(file, content);

            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.PrefetchRing = ring;
                var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                using (pager)
                using (state)
                {
                    DropFromPageCache(file);
                    var pageNumbers = Enumerable.Range(0, pages).Select(x => (long)x).ToArray();
                    Assert.True(Pager.ReadPagesAhead(state, pageNumbers).AsTask().Wait(TimeSpan.FromSeconds(10)));
                    var stats = ring.GetStats()!.Value;
                    var expected = content.Length / Environment.SystemPageSize;
                    if (stats.UsesIoUring)
                    {
                        Assert.Equal(1, stats.ReadGroups);
                        Assert.Equal(pages, stats.Reads);
                        Assert.Equal(expected, ResidentPages(state.ReadAddress, content.Length));
                    }
                    else
                    {
                        // nothing waits for the reads, but they were started
                        Assert.Equal(0, stats.ReadGroups);
                        var sw = Stopwatch.StartNew();
                        int resident;
                        while ((resident = ResidentPages(state.ReadAddress, content.Length)) < expected && sw.Elapsed < TimeSpan.FromSeconds(5))
                            Thread.Sleep(10);
                        Assert.Equal(expected, resident);
                    }
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ReadPagesAheadCompletesWhenTheFileIsClosed()
    {
        using var ring = new PrefetchRing(ringEntries: 8, maxFiles: 4, queueSize: 64);
        if (ring.IsAvailable == false)
            return;

        var path = RavenTestHelper.NewDataPath(nameof(ReadPagesAheadCompletesWhenTheFileIsClosed), 0, forceCreateDir: true);
        try
        {
            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.PrefetchRing = ring;
                var reads = new List<Task>();
                for (var i = 0; i < 20; i++)
                {
                    var (pager, state) = Pager.Create(options, Path.Combine(path, $"{i}.voron"), 64 * 1024, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                    reads.Add(Pager.ReadPagesAhead(state, [1, 2, 3]).AsTask());
                    state.Dispose();
                    pager.Dispose(); // racing the submitter: the group is read, or dropped as stale, but always completes
                }
                Assert.True(Task.WaitAll(reads.ToArray(), TimeSpan.FromSeconds(10)));
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void APageAskedForAMomentAgoIsSkipped()
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        const int pages = 16;
        var path = RavenTestHelper.NewDataPath(nameof(APageAskedForAMomentAgoIsSkipped), 0, forceCreateDir: true);
        try
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            var content = new byte[pages * Constants.Storage.PageSize];
            Random.Shared.NextBytes(content);
            File.WriteAllBytes(file, content);

            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                using (pager)
                using (state)
                {
                    var page = state.ReadAddress + 5 * Constants.Storage.PageSize;
                    var expected = Constants.Storage.PageSize / Environment.SystemPageSize;
                    DropFromPageCache(file);
                    if (ResidentPages(page, Constants.Storage.PageSize) == expected)
                    {
                        Output.WriteLine("this file system keeps the pages in memory (tmpfs?): nothing to check");
                        return;
                    }

                    Pager.WillNeedPages(state, 5);
                    Assert.Equal(expected, WaitForResident(page, expected));

                    // asked for a moment ago: skipped, so it stays out of memory after we drop it
                    DropFromPageCache(file);
                    Pager.WillNeedPages(state, 5);
                    Thread.Sleep(200);
                    Assert.Equal(0, ResidentPages(page, Constants.Storage.PageSize));

                    // another page is not affected
                    Pager.WillNeedPages(state, 6);
                    Assert.Equal(expected, WaitForResident(state.ReadAddress + 6 * Constants.Storage.PageSize, expected));
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }

    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PrefetchedPagesReachThePageCache()
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        const int pages = 64;
        var path = RavenTestHelper.NewDataPath(nameof(PrefetchedPagesReachThePageCache), 0, forceCreateDir: true);
        try
        {
            PrefetchAndCheck(path, pages);
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    private void PrefetchAndCheck(string path, int pages)
    {
        var file = Path.Combine(path, Constants.DatabaseFilename);
        var content = new byte[pages * Constants.Storage.PageSize];
        Random.Shared.NextBytes(content);
        File.WriteAllBytes(file, content);

        using (var options = StorageEnvironmentOptions.ForPathForTests(path))
        {
            var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
            using (pager)
            using (state)
            {
                var ringAvailable = PrefetchRing.Default.IsAvailable;

                var expected = content.Length / Environment.SystemPageSize;
                DropFromPageCache(file);
                var before = ResidentPages(state.ReadAddress, content.Length);
                Output.WriteLine($"resident before the prefetch: {before} of {expected}");
                if (before == expected)
                {
                    Output.WriteLine("this file system keeps the pages in memory (tmpfs?): nothing to prefetch");
                    return;
                }

                Assert.Equal(1, Pal.rvn_pager_prefetch(state.Handle, 0, (long)pages * Constants.Storage.PageSize));

                var sw = Stopwatch.StartNew();
                int resident;
                while ((resident = ResidentPages(state.ReadAddress, content.Length)) < expected && sw.Elapsed < TimeSpan.FromSeconds(5))
                    Thread.Sleep(10);
                Assert.Equal(expected, resident);

                if (ringAvailable)
                {
                    var stats = PrefetchRing.Default.GetStats();
                    Assert.NotNull(stats);
                    Assert.True(stats.Value.Submitted > 0);
                    Assert.Equal(0, stats.Value.Failed);
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WillNeedPagesReadsTheWholePageBeforeItIsTouched()
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        const int pages = 16;
        var path = RavenTestHelper.NewDataPath(nameof(WillNeedPagesReadsTheWholePageBeforeItIsTouched), 0, forceCreateDir: true);
        try
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            var content = new byte[pages * Constants.Storage.PageSize];
            Random.Shared.NextBytes(content);
            File.WriteAllBytes(file, content);

            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                using (pager)
                using (state)
                {
                    DropFromPageCache(file);
                    var page = state.ReadAddress + 5 * Constants.Storage.PageSize;
                    var expected = Constants.Storage.PageSize / Environment.SystemPageSize;
                    if (ResidentPages(page, Constants.Storage.PageSize) == expected)
                    {
                        Output.WriteLine("this file system keeps the pages in memory (tmpfs?): nothing to check");
                        return;
                    }

                    Pager.WillNeedPages(state, 5);

                    var sw = Stopwatch.StartNew();
                    int resident;
                    while ((resident = ResidentPages(page, Constants.Storage.PageSize)) < expected && sw.Elapsed < TimeSpan.FromSeconds(5))
                        Thread.Sleep(5);
                    Assert.Equal(expected, resident);
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ThePrefetchRequestSizeComesFromTheDevice()
    {
        var path = RavenTestHelper.NewDataPath(nameof(ThePrefetchRequestSizeComesFromTheDevice), 0, forceCreateDir: true);
        try
        {
            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                var (pager, state) = options.InitializeDataPager();
                using (pager)
                using (state)
                {
                    Assert.Equal(PalFlags.FailCodes.Success, Pal.rvn_pager_get_prefetch_info(state.Handle, out var info));
                    var requestSize = info.RequestSize;
                    var expected = PlatformDetails.RunningOnLinux ? ExpectedRequestSize(state) : PlatformDetails.RunningOnWindows ? 0 : 128 * 1024;
                    Output.WriteLine($"prefetch request size: {requestSize / 1024} KB (expected {expected / 1024} KB)");
                    Assert.Equal(expected, requestSize);
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }

        // max(read_ahead_kb, max_sectors_kb) of the file's device (a partition keeps them on its parent), else 128 KB
        static long ExpectedRequestSize(Pager.State state)
        {
            Assert.Equal(PalFlags.FailCodes.Success, Pal.rvn_pager_get_device_id(state.Handle, out var deviceId, out _));
            var major = ((deviceId & 0x00000000000fff00UL) >> 8) | ((deviceId & 0xfffff00000000000UL) >> 32);
            var minor = (deviceId & 0x00000000000000ffUL) | ((deviceId & 0x00000ffffff00000UL) >> 12);
            // /sys/dev/block/major:minor is a link; resolve it before going up to a partition's parent (.NET collapses '..' by text)
            var link = $"/sys/dev/block/{major}:{minor}";
            var device = Directory.Exists(link) ? Path.GetFullPath(new DirectoryInfo(link).LinkTarget ?? link, "/sys/dev/block") : null;
            long Limit(string name)
            {
                if (device == null)
                    return 0;
                foreach (var dir in new[] { device, Path.GetDirectoryName(device) })
                {
                    var file = Path.Combine(dir, "queue", name);
                    if (File.Exists(file) && long.TryParse(File.ReadAllText(file).Trim(), out var kb))
                        return kb;
                }
                return 0;
            }
            var limit = Math.Max(Limit("read_ahead_kb"), Limit("max_sectors_kb"));
            return limit > 0 ? limit * 1024 : 128 * 1024;
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void StartReadsReportsAndReadsOnlyThePagesNotInMemory()
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        const int pages = 16;
        var path = RavenTestHelper.NewDataPath(nameof(StartReadsReportsAndReadsOnlyThePagesNotInMemory), 0, forceCreateDir: true);
        try
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            var content = new byte[pages * Constants.Storage.PageSize];
            Random.Shared.NextBytes(content);
            File.WriteAllBytes(file, content);

            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                using (pager)
                using (state)
                {
                    var expected = Constants.Storage.PageSize / Environment.SystemPageSize;
                    byte* Page(long n) => state.ReadAddress + n * Constants.Storage.PageSize;
                    DropFromPageCache(file);
                    if (ResidentPages(Page(5), Constants.Storage.PageSize) == expected)
                    {
                        Output.WriteLine("this file system keeps the pages in memory (tmpfs?): nothing to check");
                        return;
                    }
                    TouchPage(Page(5));

                    var offsets = stackalloc long[] { 3 * Constants.Storage.PageSize, 5 * Constants.Storage.PageSize, 7 * Constants.Storage.PageSize };
                    var missing = Pal.rvn_pager_start_reads(state.Handle, offsets, 3);
                    if (missing == 3)
                    {
                        Output.WriteLine("this file system has no RWF_NOWAIT: every page counts as missing");
                    }
                    else
                    {
                        Assert.Equal(2, missing);
                        Assert.Equal(3 * Constants.Storage.PageSize, offsets[0]);
                        Assert.Equal(7 * Constants.Storage.PageSize, offsets[1]);
                    }

                    // the reads were started: whole pages, and nothing around them
                    Assert.Equal(expected, WaitForResident(Page(3), expected));
                    Assert.Equal(expected, WaitForResident(Page(7), expected));
                    Assert.Equal(0, ResidentPages(Page(4), Constants.Storage.PageSize));
                    Assert.Equal(0, ResidentPages(Page(6), Constants.Storage.PageSize));
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ReadPagesAheadDoesNotWaitForPagesInMemory()
    {
        using var ring = new PrefetchRing(ringEntries: 8, maxFiles: 4, queueSize: 64);
        if (ring.IsAvailable == false)
            return;

        const int pages = 16;
        var path = RavenTestHelper.NewDataPath(nameof(ReadPagesAheadDoesNotWaitForPagesInMemory), 0, forceCreateDir: true);
        try
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            var content = new byte[pages * Constants.Storage.PageSize];
            Random.Shared.NextBytes(content);
            File.WriteAllBytes(file, content);

            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.PrefetchRing = ring;
                var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                using (pager)
                using (state)
                {
                    for (var page = 0; page < pages; page++)
                        TouchPage(state.ReadAddress + page * Constants.Storage.PageSize);

                    var probe = stackalloc long[] { 0 };
                    if (Pal.rvn_pager_start_reads(state.Handle, probe, 1) != 0)
                    {
                        Output.WriteLine("this file system has no RWF_NOWAIT: pages in memory still count as missing");
                        return;
                    }

                    Assert.True(Pager.ReadPagesAhead(state, [2, 4, 8]).IsCompletedSuccessfully);
                    Assert.Equal(0, ring.GetStats()!.Value.ReadGroups);
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ALargeGroupInMemoryCompletes()
    {
        using var ring = new PrefetchRing(ringEntries: 8, maxFiles: 4, queueSize: 64);
        if (ring.IsAvailable == false)
            return;

        const int pages = 64; // above the limit: no check on the caller's thread
        var path = RavenTestHelper.NewDataPath(nameof(ALargeGroupInMemoryCompletes), 0, forceCreateDir: true);
        try
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            var content = new byte[pages * Constants.Storage.PageSize];
            Random.Shared.NextBytes(content);
            File.WriteAllBytes(file, content);

            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.PrefetchRing = ring;
                var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                using (pager)
                using (state)
                {
                    for (var page = 0; page < pages; page++)
                        TouchPage(state.ReadAddress + page * Constants.Storage.PageSize);

                    var read = Pager.ReadPagesAhead(state, Enumerable.Range(0, pages).Select(x => (long)x).ToArray());
                    var stats = ring.GetStats()!.Value;
                    if (stats.UsesIoUring)
                    {
                        Assert.True(read.AsTask().Wait(TimeSpan.FromSeconds(10)));
                        stats = ring.GetStats()!.Value;
                        Assert.Equal(1, stats.ReadGroups);
                        Assert.Equal(pages, stats.Reads);
                    }
                    else
                    {
                        Assert.True(read.IsCompletedSuccessfully);
                        Assert.Equal(0, stats.ReadGroups);
                    }
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AGroupLargerThanTheFreeQueueWaitsForRoom()
    {
        using var ring = new PrefetchRing(ringEntries: 8, maxFiles: 4, queueSize: 2); // two cells, 12 pages each
        if (ring.IsAvailable == false)
            return;

        const int pages = 100;
        var path = RavenTestHelper.NewDataPath(nameof(AGroupLargerThanTheFreeQueueWaitsForRoom), 0, forceCreateDir: true);
        try
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            var content = new byte[pages * Constants.Storage.PageSize];
            Random.Shared.NextBytes(content);
            File.WriteAllBytes(file, content);

            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.PrefetchRing = ring;
                var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                using (pager)
                using (state)
                {
                    var offsets = Enumerable.Range(0, pages).Select(x => (long)x * Constants.Storage.PageSize).ToArray();
                    Assert.Equal(PalFlags.FailCodes.Success, Pal.rvn_pager_get_prefetch_info(state.Handle, out var info));
                    Assert.NotEqual(-1, info.Ticket);
                    if (ring.CanWaitForPages)
                    {
                        Assert.True(ring.TryRentSlot(out var slot));
                        Assert.True(ring.WaitForPages(slot, info.Ticket, offsets).AsTask().Wait(TimeSpan.FromSeconds(10)));
                        Assert.Equal(pages, ring.GetStats()!.Value.Reads);
                    }
                    else
                    {
                        fixed (long* p = offsets)
                            Assert.Equal(1, Pal.rvn_prefetch_ring_read_pages(ring.Handle, info.Ticket, p, pages, 0)); // waits for the submitter to free cells
                        var sw = Stopwatch.StartNew();
                        while (ring.GetStats()!.Value.ReadGroups == 0 && sw.Elapsed < TimeSpan.FromSeconds(5))
                            Thread.Sleep(5);
                    }
                    var stats = ring.GetStats()!.Value;
                    Assert.Equal(1, stats.ReadGroups);
                    Assert.Equal(0, stats.DroppedFull);
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ReadPagesAheadWaitsForAFreeSlot()
    {
        using var ring = new PrefetchRing(ringEntries: 8, maxFiles: 4, queueSize: 64);
        if (ring.IsAvailable == false || ring.CanWaitForPages == false)
            return; // only io_uring waits for the reads

        const int pages = 16;
        var path = RavenTestHelper.NewDataPath(nameof(ReadPagesAheadWaitsForAFreeSlot), 0, forceCreateDir: true);
        var taken = new List<int>();
        try
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            var content = new byte[pages * Constants.Storage.PageSize];
            Random.Shared.NextBytes(content);
            File.WriteAllBytes(file, content);

            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.PrefetchRing = ring;
                var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                using (pager)
                using (state)
                {
                    DropFromPageCache(file);
                    while (ring.TryRentSlot(out var slot))
                        taken.Add(slot);

                    var read = Pager.ReadPagesAhead(state, [3, 5]).AsTask();
                    Assert.False(read.Wait(TimeSpan.FromMilliseconds(200))); // no slot: it waits

                    ring.ReturnSlot(taken[^1]);
                    taken.RemoveAt(taken.Count - 1);
                    Assert.True(read.Wait(TimeSpan.FromSeconds(10)));
                    Assert.Equal(1, ring.GetStats()!.Value.ReadGroups);
                }
            }
        }
        finally
        {
            foreach (var slot in taken)
                ring.ReturnSlot(slot);
            IOExtensions.DeleteDirectory(path);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WaitSlotsAreReused()
    {
        using var ring = new PrefetchRing(ringEntries: 8, maxFiles: 4, queueSize: 64);
        if (ring.IsAvailable == false || ring.GetStats()!.Value.UsesIoUring == false)
            return; // only io_uring waits for the reads

        const int pages = 2048 + 64; // more groups than the ring has wait slots, one after the other
        var path = RavenTestHelper.NewDataPath(nameof(WaitSlotsAreReused), 0, forceCreateDir: true);
        try
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            var content = new byte[pages * Constants.Storage.PageSize];
            Random.Shared.NextBytes(content);
            File.WriteAllBytes(file, content);

            using (var options = StorageEnvironmentOptions.ForPathForTests(path))
            {
                options.PrefetchRing = ring;
                var (pager, state) = Pager.Create(options, file, content.Length, Pal.OpenFileFlags.RandomAccess | Pal.OpenFileFlags.Prefetch);
                using (pager)
                using (state)
                {
                    DropFromPageCache(file);
                    for (long page = 0; page < pages; page++)
                        Assert.True(Pager.ReadPagesAhead(state, [page]).AsTask().Wait(TimeSpan.FromSeconds(10)));
                    Assert.Equal(pages, ring.GetStats()!.Value.ReadGroups);
                }
            }
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    private static void TouchPage(byte* page)
    {
        for (var offset = 0; offset < Constants.Storage.PageSize; offset += Environment.SystemPageSize)
            _ = Volatile.Read(ref page[offset]);
    }

    private static int WaitForResident(byte* address, int expected)
    {
        var sw = Stopwatch.StartNew();
        int resident;
        while ((resident = ResidentPages(address, Constants.Storage.PageSize)) < expected && sw.Elapsed < TimeSpan.FromSeconds(5))
            Thread.Sleep(5);
        return resident;
    }

    private static void DropFromPageCache(string file)
    {
        using var stream = new FileStream(file, FileMode.Open, FileAccess.ReadWrite);
        stream.Flush(flushToDisk: true); // DONTNEED keeps dirty pages
        var fd = stream.SafeFileHandle.DangerousGetHandle().ToInt32();
        Assert.Equal(0, Syscall.posix_fadvise(fd, 0, 0, PosixFadviseDontNeed));
    }

    private static int ResidentPages(byte* address, long length)
    {
        var count = (int)(length / Environment.SystemPageSize);
        var vec = stackalloc char[count];
        Assert.Equal(0, Syscall.mincore(address, new IntPtr(length), vec));
        var resident = 0;
        var bytes = (byte*)vec;
        for (var i = 0; i < count; i++)
            resident += bytes[i] & 1;
        return resident;
    }
}
