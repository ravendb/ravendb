using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace InterversionTests
{
    public class ClusterTests : MixedClusterTestBase
    {
        [Fact]
        public async Task ReplicationInMixedCluster_41Leader()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.1.0-rc-41000",
                "4.1.0-rc-41000"
            });

            leader.ServerStore.Engine.CurrentLeader.StepDown();
            await leader.ServerStore.Engine.WaitForState(RachisState.Follower, CancellationToken.None);

            var stores = await GetStores(leader, peers);
            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];

                var dbName = await CreateDatabase(storeA, 3);

                using (var session = storeA.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        [Fact]
        public async Task ReplicationInMixedCluster_40Leader()
        {
            var (leader, peers, local) = await CreateMixedCluster(new[]
            {
                "4.1.0-rc-41000",
                "4.1.0-rc-41000"
            });

            var stores = await GetStores(leader, peers);
            using (stores.Disposable)
            {
                var storeA = stores.Stores[0];

                var dbName = await CreateDatabase(storeA, 3);

                using (var session = storeA.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    stores.Stores,
                    dbName));

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        [Fact]
        public async Task ReplicationInCluster_v41Nodes_v40Client()
        {
            (var urlA, var serverA) = await GetServerAsync("4.1.0-rc-41000");
            (var urlB, var serverB) = await GetServerAsync("4.1.0-rc-41000");
            (var urlc, var serverC) = await GetServerAsync("4.1.0-rc-41000");

            using (var storeA = await GetStore(urlA, serverA, null, new InterversionTestOptions
            {
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            using (var storeB = await GetStore(urlB, serverB, null, new InterversionTestOptions
            {
                CreateDatabase = false,
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            using (var storeC = await GetStore(urlc, serverC, null, new InterversionTestOptions
            {
                CreateDatabase = false,
                ModifyDocumentStore = store => store.Conventions.DisableTopologyUpdates = true
            }))
            {
                await AddNodeToCluster(storeA, storeB.Urls[0]);
                await Task.Delay(2500);
                await AddNodeToCluster(storeA, storeC.Urls[0]);
                await Task.Delay(1000);

                var dbName = await CreateDatabase(storeA, 3);

                using (var session = storeA.OpenSession(dbName))
                {
                    session.Store(new User
                    {
                        Name = "aviv"
                    }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    "users/1",
                    u => u.Name.Equals("aviv"),
                    TimeSpan.FromSeconds(10),
                    new List<DocumentStore>
                    {
                        storeA, storeB, storeC
                    },
                    dbName));

                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(storeA.Database, true));
                storeA.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, true));
            }
        }

        private static async Task AddNodeToCluster(DocumentStore store, string url)
        {
            var addNodeRequest = await store.GetRequestExecutor().HttpClient.SendAsync(
                new HttpRequestMessage(HttpMethod.Put, $"{store.Urls[0]}/admin/cluster/node?url={url}"));
            Assert.True(addNodeRequest.IsSuccessStatusCode);
        }

    }
}
