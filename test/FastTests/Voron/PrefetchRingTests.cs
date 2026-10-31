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
