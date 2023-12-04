using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;
public class RavenDB_21780 : ClusterTestBase
{
    public RavenDB_21780(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Revisions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public async Task EnforceRevisionsConfigurationOnShardedDB(Options options)
    {
        var (nodes, leader) = await CreateRaftCluster(numberOfNodes: 2, watcherCluster: true);

        options.Server = leader;
        options.ReplicationFactor = nodes.Count;

        using var store = GetDocumentStore(options);

        var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 } };
        await SetupRevisionsConfiguration(options.DatabaseMode, store, configuration);

        var user = new SamplesTestBase.User() { Id = "Users/1", Name = "Shahar" };

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 10; i++)
            {
                user.Name += i;
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }

            var revisionsCount = await session.Advanced.Revisions.GetCountForAsync(user.Id);
            Assert.Equal(10, revisionsCount);
        }


        configuration.Default.MinimumRevisionsToKeep = 2;
        await SetupRevisionsConfiguration(options.DatabaseMode, store, configuration);


        // enforce
        var parameters = new EnforceRevisionsConfigurationOperation.Parameters { Collections = new[] { "Users" } };
        var result = await store.Operations.SendAsync(new EnforceRevisionsConfigurationOperation(parameters));
        await result.WaitForCompletionAsync();


        using (var session = store.OpenAsyncSession())
        {
            var revisionsCount = await session.Advanced.Revisions.GetCountForAsync(user.Id);
            Assert.Equal(2, revisionsCount);
        }
    }

    private async Task SetupRevisionsConfiguration(RavenDatabaseMode databaseMode, DocumentStore store, RevisionsConfiguration configuration)
    {
        if (databaseMode == RavenDatabaseMode.Sharded)
        {
            var shards = Sharding.GetShardsDocumentDatabaseInstancesFor(store);
            await RevisionsHelper.SetupRevisionsOnShardedDatabaseAsync(store, Server.ServerStore, configuration: configuration, shards);
        }
        else
        {
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);
        }
    }
}

