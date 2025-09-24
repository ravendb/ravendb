using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22040 : ReplicationTestBase
{
    public RavenDB_22040(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Indexes)]
    public async Task ReplicatedTombstonesWillSendNotificationToIndexes()
    {
        var order1 = new Order() { Company = "Company1" };
        var order2 = new Order() { Company = "Company2" };
        var defaultTimeout = TimeSpan.FromSeconds(15);

        var cluster = await CreateRaftCluster(2);

        var store = GetDocumentStore(new Options { ReplicationFactor = 2, Server = cluster.Leader, });

        var database = store.Database;

        var node1 = await cluster.Nodes[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
        var node2 = await cluster.Nodes[1].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);

        var t1 = await BreakReplication(cluster.Nodes[0].ServerStore, database);
        var t2 = await BreakReplication(cluster.Nodes[1].ServerStore, database);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(order1);
            await session.SaveChangesAsync();
            await new Index().ExecuteAsync(store);
            Indexes.WaitForIndexing(store);
        }

        t1.Mend();
        t2.Mend();

        await WaitForDocumentInClusterAsync<Order>(cluster.Nodes, database, order1.Id, predicate: null, defaultTimeout);

        t1 = await BreakReplication(cluster.Nodes[0].ServerStore, database);
        t2 = await BreakReplication(cluster.Nodes[1].ServerStore, database);
        var markerDocument = new Employee() { FirstName = "MARKER" };
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(order2);
            await session.SaveChangesAsync();
            Indexes.WaitForIndexing(store);

            session.Delete(order2.Id);
            await session.SaveChangesAsync();

            await session.StoreAsync(markerDocument);
            await session.SaveChangesAsync();
        }

        t1.Mend();
        t2.Mend();

        await WaitForDocumentInClusterAsync<Employee>(cluster.Nodes, database, markerDocument.Id, null, defaultTimeout);
        Indexes.WaitForIndexing(store, database, timeout: defaultTimeout, nodeTag: "A");
        Indexes.WaitForIndexing(store, database, timeout: defaultTimeout, nodeTag: "B");
    }

    private class Index : AbstractIndexCreationTask<Order>
    {
        public Index()
        {
            Map = orders => orders.Select(x => new { x.Company });
        }
    }
}
