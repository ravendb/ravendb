using System.IO;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron;

public class PageCacheEvictionsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void TheFileEvictionsSincePreviousReadAndTheFilePagesAreRead()
    {
        if (PlatformDetails.RunningOnPosix == false)
            return;

        var file = Path.GetTempFileName();
        try
        {
            using var evictions = new PageCacheEvictions(file);
            File.WriteAllText(file, "nr_free_pages 1000\nnr_inactive_file 300\nnr_active_file 700\npgsteal_kswapd 90\npgsteal_direct 10\npgsteal_anon 40\npgsteal_file 60\n");
            Assert.True(evictions.TryRead(out var evicted, out var filePages));
            Assert.Equal(0, evicted); // the first read
            Assert.Equal(1000, filePages);

            File.WriteAllText(file, "nr_free_pages 1000\nnr_inactive_file 400\nnr_active_file 700\npgsteal_kswapd 95\npgsteal_direct 10\npgsteal_anon 40\npgsteal_file 85\n");
            Assert.True(evictions.TryRead(out evicted, out filePages));
            Assert.Equal(25, evicted);
            Assert.Equal(1100, filePages);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void BeforeLinux58AllEvictionsCount()
    {
        if (PlatformDetails.RunningOnPosix == false)
            return;

        var file = Path.GetTempFileName();
        try
        {
            using var evictions = new PageCacheEvictions(file);
            File.WriteAllText(file, "nr_inactive_file 300\nnr_active_file 700\npgsteal_kswapd 90\npgsteal_direct 10\n");
            Assert.True(evictions.TryRead(out _, out _));
            File.WriteAllText(file, "nr_inactive_file 300\nnr_active_file 700\npgsteal_kswapd 120\npgsteal_direct 30\n");
            Assert.True(evictions.TryRead(out var evicted, out _));
            Assert.Equal(50, evicted);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WithoutTheFilePagesThereIsNoReading()
    {
        if (PlatformDetails.RunningOnPosix == false)
            return;

        var file = Path.GetTempFileName();
        try
        {
            using var evictions = new PageCacheEvictions(file);
            File.WriteAllText(file, "pgsteal_file 60\n");
            Assert.False(evictions.TryRead(out _, out _));
        }
        finally
        {
            File.Delete(file);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void TheKernelFileIsRead()
    {
        if (PlatformDetails.RunningOnLinux == false || File.Exists("/proc/vmstat") == false)
            return;

        using var evictions = PageCacheEvictions.TryCreate();
        Assert.NotNull(evictions);
        Assert.True(evictions.TryRead(out var evicted, out var filePages));
        Assert.Equal(0, evicted);
        Assert.True(filePages > 0);
        Assert.True(evictions.TryRead(out evicted, out _));
        Assert.True(evicted >= 0);
    }
}
