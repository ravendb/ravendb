using System;
using System.Collections.Generic;
using SlowTests.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Corax.Bugs;

public class PostingListTestsExtended(ITestOutputHelper output) : NoDisposalNoOutputNeeded(output)
{
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Voron)]
    [InlineData(1337, 200000)]
    [InlineData(1064156071, 796)]
    [InlineData(511767612, 4172)]
    [InlineData(439188321, 502627)]
    [InlineData(506431817, 2)]
    public void CanDeleteAndInsertInRandomOrder(int seed, int size) => CanDeleteAndInsertInRandomOrderBase(seed, size);

    [RavenMultiplatformTheory(RavenTestCategory.Corax | RavenTestCategory.Voron, RavenArchitecture.X64)]
    [InlineData(1477187726, true, 1828658)]
    [InlineDataWithRandomSeed(20000000, false)]
    [InlineDataWithRandomSeed(2000000, false)]
    [InlineDataWithRandomSeed(200000, false)]
    [InlineDataWithRandomSeed(20000, false)]
    public void CanDeleteAndInsertInRandomOrderX64Only(int maxSize, bool maxSizeFinal, int seed)
    {
        var random = new Random(seed);
        maxSize = maxSizeFinal ? maxSize : random.Next(maxSize);
        
        CanDeleteAndInsertInRandomOrderBase(seed, maxSize);
    }

    [RavenMultiplatformTheory(RavenTestCategory.Corax | RavenTestCategory.Voron, RavenPlatform.Windows, RavenArchitecture.X64)]
    [InlineData(391060845, 31707323)]
    public void CanDeleteAndInsertInRandomOrderWindows(int seed, int size) => CanDeleteAndInsertInRandomOrderBase(seed, size);

    private void CanDeleteAndInsertInRandomOrderBase(int seed, int size)
    {
        using var testClass = new FastTests.Voron.PostingLists.PostingListTests(Output);
        testClass.CanDeleteAndInsertInRandomOrder(seed, size, 10);
    }
}
