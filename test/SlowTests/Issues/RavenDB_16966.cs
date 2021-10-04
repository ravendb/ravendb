using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Exceptions;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16966 : RavenTestBase
    {
        public RavenDB_16966(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task KeepCompareExchangeContextEvenUponFailure()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            DoNotReuseServer();

            using var store = GetDocumentStore();
            Server.ServerStore.ForTestingPurposesOnly().SimulateCompareExchangeRequestDrop = true;

            var ex = await Assert.ThrowsAsync<RavenException>(()=>store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("test", "Karmel", 0)));
            Assert.Contains("Simulate Request Drop", ex.Message);

            await AssertWaitForNotNullAsync(() => store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("test")));
            var res = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("test"));
            Assert.Equal("Karmel", res.Value);
        }
    }
}
