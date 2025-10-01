using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19559 : RavenTestBase
{
    public RavenDB_19559(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.CompareExchange | RavenTestCategory.ClientApi)]
    public async Task Can_Use_Arrays_In_CompareExchange()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("key2", new[] { "1", "2", "3" });

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var value = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string[]>("key2");

                Assert.Equal(value.Value, new[] { "1", "2", "3" });

                value.Value[2] = "4";

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var value = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string[]>("key2");

                Assert.Equal(value.Value, new[] { "1", "2", "4" });
            }

            var result1 = store.Operations.Send(new PutCompareExchangeValueOperation<string[]>("key1", new[] { "1", "2", "3" }, 0));

            Assert.Equal(result1.Value, new[] { "1", "2", "3" });

            var result2 = store.Operations.Send(new GetCompareExchangeValueOperation<string[]>("key1"));

            Assert.Equal(result2.Value, new[] { "1", "2", "3" });
        }
    }
}
