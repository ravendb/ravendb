using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19559 : RavenTestBase
{
    public RavenDB_19559(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.CompareExchange | RavenTestCategory.ClientApi)]
    public async Task T1()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue("key2", new[] { "1", "2", "3" });

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                var x = store.Operations.Send(new GetCompareExchangeValueOperation<IEnumerable<string>>("key2"));

                var value = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string[]>("key2");
            }

            var result = store.Operations.Send(new PutCompareExchangeValueOperation<string[]>("key1", new[] { "1", "2", "3" }, 0));
        }
    }
}
