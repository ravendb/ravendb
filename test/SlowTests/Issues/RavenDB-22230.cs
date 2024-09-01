using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CompareExchange;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;
public class RavenDB_22230 : RavenTestBase
{
    public RavenDB_22230(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.CompareExchange)]
    public async Task TryCreateCompareExchangeWithNonZeroIndex()
    {
        using var store = GetDocumentStore();

        var result0 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("key/0", "test", 0));
        Assert.True(result0.Successful);
        result0 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("key/0", "test0", result0.Index));
        Assert.True(result0.Successful);
        var value0 = (await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("key/0"))).Value;
        Assert.Equal("test0", value0);

        var result1 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("key/1", "test", 0));
        Assert.True(result1.Successful);
        result1 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("key/1", "test1", result1.Index+1));
        Assert.False(result1.Successful);
        var value1 = (await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("key/1"))).Value;
        Assert.Equal("test", value1);

        var result2 = await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("key/2", "test", 123));
        Assert.False(result2.Successful);
        var value2 = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("key/2"));
        Assert.Equal(null, value2); // does not exist (Failed)
    }
}

