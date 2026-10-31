using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
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

        static int WaitForResident(byte* address, int expected)
        {
            var sw = Stopwatch.StartNew();
            int resident;
            while ((resident = ResidentPages(address, Constants.Storage.PageSize)) < expected && sw.Elapsed < TimeSpan.FromSeconds(5))
                Thread.Sleep(5);
            return resident;
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
                    Assert.Equal(PalFlags.FailCodes.Success, Pal.rvn_pager_get_prefetch_request_size(state.Handle, out var requestSize));
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
