using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Client.Attachments;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Security;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Server.Config.Attributes;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class ReplicationTests : ReplicationTestsBase
    {
        [Fact]
        public async Task EnsureDocumentsReplication()
        {
            var clusterSize = 5;
            var databaseName = "ReplicationTestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllReplicationNodes().Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.ETag ?? -1);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Karmel"}, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5)));
            }
        }

        [Fact]
        public async Task DoNotReplicateBack()
        {
            var clusterSize = 5;
            var databaseName = "ReplicationTestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                var topology = databaseResult.Topology;
                Assert.Equal(clusterSize, topology.AllReplicationNodes().Count());

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Karmel"}, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5)));

                topology.RemoveFromTopology(leader.ServerStore.NodeTag);

                await Assert.ThrowsAsync<Exception>(async () =>
                {
                    await WaitForValueOnGroupAsync(topology, (s) =>
                    {
                        var replicationPerformance = s.Admin.Send(new GetReplicationPerformanceStatisticsOperation());
                        return replicationPerformance.Outgoing.Any(o=>o.Performance.Any(p=> p.SendLastEtag > 0));
                    }, true);
                });

            }
        }

        [Fact]
        public async Task AddGlobalChangeVectorToNewDocument()
        {
            var clusterSize = 3;
            var databaseName = "ReplicationTestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0);
            var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = databaseName,
                
            }.Initialize())
            {
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                var topology = databaseResult.Topology;
                Assert.Equal(clusterSize, topology.AllReplicationNodes().Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.ETag ?? -1);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Karmel"}, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    topology,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5)));
            }

            using (var store = new DocumentStore()
            {
                Url = Servers[1].WebUrls[0],
                DefaultDatabase = databaseName
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Indych"}, "users/2");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/2");
                    Assert.Equal(2, session.Advanced.GetChangeVectorFor(user).Length);
                }
            }
        }

        private readonly ApiKeyDefinition _apiKey = new ApiKeyDefinition
        {
            Enabled = true,
            Secret = "secret",            
        };

        [Theory]
        [InlineData("secret")]
        [InlineData("bad")]
        public async Task ReplicateToWatcherWithAuth(string api)
        {
            //DoNotReuseServer();

            Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            using (var store1 = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            using (var store2 = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                _apiKey.ResourcesAccessMode[store1.DefaultDatabase] = AccessModes.Admin;
                _apiKey.ResourcesAccessMode[store2.DefaultDatabase] = AccessModes.ReadWrite;
                store2.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                var doc = store2.Admin.Server.Send(new GetApiKeyOperation("super"));
                Assert.NotNull(doc);
                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                var watchers = new List<DatabaseWatcher>
                {
                    new DatabaseWatcher
                    {
                        Database = store2.DefaultDatabase,
                        Url = store2.Url,
                        ApiKey = "super/" + api
                    }
                };
                await UpdateReplicationTopology(store1, watchers);
  
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Karmel"}, "users/1");
                    await session.SaveChangesAsync();
                }

                if (api.Equals(_apiKey.Secret))
                {
                    Assert.True(WaitForDocument<User>(store2, "users/1", (u) => u.Name == "Karmel"));
                }
                else
                {
                    Assert.False(WaitForDocument<User>(store2, "users/1", (u) => u.Name == "Karmel"));
                }
            }
        }
    }
}
