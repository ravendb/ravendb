using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18762 : ClusterTestBase
{
    public RavenDB_18762(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Replication)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task WaitForReplicationAfterSaveChanges_ShouldThrow_OnTimeout(Options options)
    {
        var database = GetDatabaseName();
        var cluster = await CreateRaftCluster(3, leaderIndex: 0);

        if (options.DatabaseMode == RavenDatabaseMode.Sharded)
        {
            await CreateDatabaseInCluster(new DatabaseRecord(database)
            {
                Shards = new[]
                {
                    new DatabaseTopology
                    {
                        Members = new List<string> { "A", "B", "C" }
                    },
                    new DatabaseTopology
                    {
                        Members = new List<string> { "A", "B", "C" }
                    },
                    new DatabaseTopology
                    {
                        Members = new List<string> { "A", "B", "C" }
                    }
                }
            }, 3, cluster.Leader.WebUrl);
        }

        using (var store = GetDocumentStore(new Options
        {
            Server = cluster.Leader,
            ModifyDatabaseName = _ => database,
            CreateDatabase = options.DatabaseMode != RavenDatabaseMode.Sharded,
            ReplicationFactor = 3,
            ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true
        }))
        {
            //WaitForUserToContinueTheTest(store, debug: false);

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Id = "users/1" };
                await session.StoreAsync(user);

                session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(5), throwOnTimeout: true, replicas: 2);
                await session.SaveChangesAsync();
            }

            await DisposeServerAsync(cluster.Nodes[1]);

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Id = "users/3" };
                await session.StoreAsync(user);

                session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(5), throwOnTimeout: true, replicas: 2);

                var error = await Assert.ThrowsAsync<TimeoutException>(() => session.SaveChangesAsync());
                Assert.StartsWith("System.TimeoutException", error.Message);
                Assert.Contains("Could not verify that", error.Message);
                Assert.Contains("was replicated to 2 servers", error.Message);
            }
        }
    }
}
