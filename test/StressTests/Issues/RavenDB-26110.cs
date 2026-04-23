using System;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace StressTests.Issues;

public class RavenDB_26110(ITestOutputHelper output) : SlowTests.Issues.RavenDB_26110(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    public void MemoizationMatchProviderCanMemoizeLargeResultSet()
    {
        using var fields = CreateFields(Allocator);
        using var searcher = new IndexSearcher(Env, fields);

        searcher.MaxMemoizationSizeInBytes = long.MaxValue;

        var mock = new DummyBigMatch();
        var memoizer = searcher.Memoize(mock).Replay();
        var results = memoizer.FillAndRetrieve();
        Assert.Equal(DummyBigMatch.TotalItemsToProduce, results.Length);
        for (int i = 1; i < Math.Min(1000, results.Length); i++)
        {
            Assert.True(results[i] > results[i - 1]);
        }
    }
}
