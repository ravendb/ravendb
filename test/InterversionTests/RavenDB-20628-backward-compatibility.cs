using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Sparrow.Json;
using Tests.Infrastructure;
using Tests.Infrastructure.InterversionTest;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class RavenDB_20628_backward_compatibility : MixedClusterTestBase
    {
        public RavenDB_20628_backward_compatibility(ITestOutputHelper output) : base(output)
        {
        }

        [MultiplatformTheory(RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData("5.4.107")]
        [InlineData("5.4.109")]
        public async Task UpgradeLeaderToLatest(string latest)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var initialVersions = new[] { latest, latest };

            var nodes = await CreateCluster(initialVersions, watcherCluster: true);
            var database = GetDatabaseName();
            var result = await GetStores(database, nodes);
            var leaderUrl = await GetLeaderUrl(result.Stores[0]);
            var leaderProc = nodes.Single(n => n.Url == leaderUrl);
            var followerUrl = nodes.Single(n => n.Url != leaderUrl).Url;

            using (result.Disposable)
            {
                var stores = result.Stores;
                await CreateDatabase(stores[0], replicationFactor: nodes.Count, dbName: database, record: new DatabaseRecord(database));

                await UpgradeServerAsync("current", leaderProc);

                using var followerStore = new DocumentStore()
                {
                    Urls = new[] { followerUrl },
                    Conventions =
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize();

                var user1 = new User() { Id = "Users/1-A", Name = "Alice" };

                using (var session = followerStore.OpenAsyncSession(new SessionOptions
                {
                    Database = database,
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(user1);
                    await session.SaveChangesAsync();
                }
            }
        }

        [MultiplatformTheory(RavenPlatform.Windows | RavenPlatform.Linux)]
        [InlineData("5.4.107")]
        [InlineData("5.4.109")]
        public async Task UpgradeFollowerToLatest(string latest)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var initialVersions = new[] { latest, latest };

            var nodes = await CreateCluster(initialVersions, watcherCluster: true);
            var database = GetDatabaseName();
            var result = await GetStores(database, nodes);
            var leaderUrl = await GetLeaderUrl(result.Stores[0]);
            var followerProc = nodes.Single(n => n.Url != leaderUrl);
            var followerUrl = followerProc.Url;

            using (result.Disposable)
            {
                var stores = result.Stores;
                await CreateDatabase(stores[0], replicationFactor: nodes.Count, dbName: database, record: new DatabaseRecord(database));
                await UpgradeServerAsync("current", followerProc);

                using var followerStore = new DocumentStore()
                {
                    Urls = new[] { followerUrl },
                    Conventions =
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize();

                var user1 = new User() { Id = "Users/1-A", Name = "Alice" };

                using (var session = followerStore.OpenAsyncSession(new SessionOptions
                {
                    Database = database,
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    await session.StoreAsync(user1);
                    await session.SaveChangesAsync();
                }
            }
        }

        private async Task<string> GetLeaderUrl(IDocumentStore store)
        {
            var clusterTopology = await store.Maintenance.Server.SendAsync(new GetClusterTopologyOperation());
            var leaderTag = clusterTopology.Leader;
            var leaderUrl = clusterTopology.Topology.Members[leaderTag];
            return leaderUrl;
        }

        private class GetClusterTopologyOperation : IServerOperation<ClusterTopologyResponse>
        {
            public GetClusterTopologyOperation()
            {
            }

            public RavenCommand<ClusterTopologyResponse> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetClusterTopologyCommand();
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
