using System.Threading.Tasks;
using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.Documents.Session;
using System.Threading;
using FastTests.Graph;
using Raven.Client.Documents;

namespace SlowTests.Issues;

public class RavenDB_19474 : RavenTestBase
{
    public RavenDB_19474(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task CanCreateCompareExchangeThenDatabaseUnloadThenCreateCompareExchange()
    {
        var value = new User { Id = Guid.NewGuid().ToString(), Name = "Lev" };
        const string key1 = "The first";
        const string key2 = "The second";
        
        using (var store = GetDocumentStore())
        {
            await CreateCompareExchangeValue(store, key1, value);

            (await Server.ServerStore.DatabasesLandlord.UnloadAndLockDatabase(store.Database, "Forcibly unloading the database to trigger its subsequent initialization")).Dispose();

            await CreateCompareExchangeValue(store, key2, value);
        }
    }

    internal async Task CreateCompareExchangeValue(DocumentStore store, string key, object value)
    {
        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
        {
            session.Advanced.ClusterTransaction.CreateCompareExchangeValue(key, value);
            await session.SaveChangesAsync(cts.Token);
        }
    }
}
