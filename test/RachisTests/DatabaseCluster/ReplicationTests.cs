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
using Sparrow.Logging;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class ReplicationTests : ReplicationTestsBase
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EnsureDocumentsReplication(bool useSsl)
        {
            var clusterSize = 5;
            var databaseName = "ReplicationTestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: useSsl);
            CreateDatabaseResult databaseResult;
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                Database = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
            }
            Assert.Equal(clusterSize, databaseResult.Topology.AllReplicationNodes().Count());
            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.ETag ?? -1);
            }
            foreach (var server in Servers.Where(s=> databaseResult.NodesAddedTo.Any(n=> n == s.WebUrls[0])))
            {
                await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            }

            using (var store = new DocumentStore()
            {
                Url = databaseResult.NodesAddedTo[0],
                Database = databaseName
            }.Initialize())
            {
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

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EnsureReplicationToWatchers(bool useSsl)
        {
            var clusterSize = 3;
            var databaseName = "ReplicationTestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, useSsl:useSsl);
            var watchers = new List<DatabaseWatcher>();

            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                Database = databaseName
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
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5)));

                for (var i = 0; i < 5; i++)
                {
                    doc = MultiDatabase.CreateDatabaseDocument($"Watcher{i}");
                    var res = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc));
                    var server = Servers.Single(x => x.WebUrls[0] == res.NodesAddedTo[0]);
                    await server.ServerStore.Cluster.WaitForIndexNotification(res.ETag ?? -1);
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore($"Watcher{i}");

                    watchers.Add(new DatabaseWatcher
                    {
                        Database = $"Watcher{i}",
                        Url = res.NodesAddedTo[0]
                    });
                }

                await UpdateReplicationTopology((DocumentStore)store, watchers);
            }

            foreach (var watcher in watchers)
            {
                using (var store = new DocumentStore()
                {
                    Url = watcher.Url,
                    Database = watcher.Database
                }.Initialize())
                {
                    Assert.True(WaitForDocument<User>(store, "users/1", u => u.Name == "Karmel"));
                }
            }
        }

        [Fact]
        public async Task CanAddSingleWatcher()
        {
            var clusterSize = 3;
            var databaseName = "ReplicationTestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            DatabaseWatcher watcher;

            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                Database = databaseName
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
                    await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5)));


                doc = MultiDatabase.CreateDatabaseDocument("Watcher");
                var res = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc));
                var node = Servers.Single(x => x.WebUrls[0] == res.NodesAddedTo[0]);
                await node.ServerStore.Cluster.WaitForIndexNotification(res.ETag ?? -1);
                await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("Watcher");

                watcher = new DatabaseWatcher
                {
                    Database = "Watcher",
                    Url = res.NodesAddedTo[0]
                };

                await AddWatcherToReplicationTopology((DocumentStore)store, watcher);
            }

            using (var store = new DocumentStore()
            {
                Url = watcher.Url,
                Database = watcher.Database
            }.Initialize())
            {
                Assert.True(WaitForDocument<User>(store, "users/1", u => u.Name == "Karmel"));
            }
        }


        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DoNotReplicateBack(bool useSsl)
        {
            var clusterSize = 5;
            var databaseName = "ReplicationTestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, useSsl:useSsl);
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                Database = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                var topology = databaseResult.Topology;
                Assert.Equal(clusterSize, topology.AllReplicationNodes().Count());

                await WaitForValueOnGroupAsync(topology,  s =>
                {
                    var db = s.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).Result;
                    return db.ReplicationLoader?.OutgoingConnections.Count();
                }, clusterSize - 1);

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
                await Task.Delay(200); // twice the heartbeat
                await Assert.ThrowsAsync<Exception>(async () =>
                {
                    await WaitForValueOnGroupAsync(topology, (s) =>
                    {
                        var db = s.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).Result;
                        return db.ReplicationLoader?.OutgoingHandlers.Any(o=>o.GetReplicationPerformance().Any(p=>p.Network.DocumentOutputCount > 0)) ?? false;
                    }, true);
                });
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AddGlobalChangeVectorToNewDocument(bool useSsl)
        {
            var clusterSize = 3;
            var databaseName = "ReplicationTestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0, useSsl: useSsl);
            var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                Database = databaseName,
                
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
                Database = databaseName
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
                _apiKey.ResourcesAccessMode[store1.Database] = AccessModes.Admin;
                _apiKey.ResourcesAccessMode[store2.Database] = AccessModes.ReadWrite;
                store2.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                var doc = store2.Admin.Server.Send(new GetApiKeyOperation("super"));
                Assert.NotNull(doc);
                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                var watchers = new List<DatabaseWatcher>
                {
                    new DatabaseWatcher
                    {
                        Database = store2.Database,
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
