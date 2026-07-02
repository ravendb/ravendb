using System.Runtime.CompilerServices;
using FastTests.Voron;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron.Impl.Paging;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26929(ITestOutputHelper output) : StorageTest(output)
{
    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_canPrefetch")]
    private static extern ref bool GetCanPrefetchValue(Pager pager);

    [RavenFact(RavenTestCategory.Voron)]
    public void PrefetchingShouldBeEnabledWhenPlatformSupportsItAndOptionsAllowIt()
    {
        Assert.True(Options.EnablePrefetching);
        Assert.Equal(PlatformDetails.CanPrefetch, GetCanPrefetchValue(Env.DataPager));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PrefetchingShouldBeDisabledWhenDisabledInOptions()
    {
        Options.EnablePrefetching = false;
        // Env is lazy initialized.
        Assert.False(GetCanPrefetchValue(Env.DataPager));
    }
}
