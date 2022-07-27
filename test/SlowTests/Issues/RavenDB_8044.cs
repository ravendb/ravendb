using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8044 : ClusterTestBase
    {
        public RavenDB_8044(ITestOutputHelper output) : base(output)
        {
        }

        private readonly string _database = "Client_cant_handle_topology_changes_that_it_wasnt_notified_about" + Guid.NewGuid();

        [Fact]
        public async Task Client_cant_handle_topology_changes_that_it_wasnt_notified_about()
        {
            const int clusterSize = 3;

            await CreateRaftCluster(clusterSize);

            var serverA = GetNodeServer("A");
            var serverB = GetNodeServer("B");

            using (var storeB = GetDocumentStoreForServer(serverB))
            {
                await CreateDatabaseInCluster(storeB, "B");
                await EnsureDatabaseOnNode(storeB, "B");

                using (var session = storeB.OpenSession())
                {
                    session.Store(new User { Name = "Joe" });
                    session.SaveChanges();
                }

                await DeleteDatabaseInCluster(_database, storeB, "B");

                using (var storeA = GetDocumentStoreForServer(serverA))
                {
                    await CreateDatabaseInCluster(storeA, "A");
                    await EnsureDatabaseOnNode(storeA, "A");

                    using (var session = storeA.OpenSession())
                    {
                        session.Store(new User { Name = "James" });
                        session.SaveChanges();
                    }

                    using (var session = storeB.OpenSession())
                    {
                        session.Store(new User { Name = "Doe" });
                        session.SaveChanges();
                    }
                }
            }
        }

        private async Task EnsureDatabaseOnNode(DocumentStore storeB, string node)
        {
            await WaitForValueAsync(async () => await GetMembersCount(storeB, _database), 1);

            var record = await storeB.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(_database));

            Assert.Equal(node, record.Topology.Members[0]);
        }

        private static async Task<int> GetMembersCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Members.Count;
        }

        private RavenServer GetNodeServer(string nodeTag)
        {
            return Servers.First(x => x.ServerStore.NodeTag == nodeTag);
        }

        private DocumentStore GetDocumentStoreForServer(RavenServer server)
        {
            var store = new DocumentStore
            {
                Urls = new[] {server.WebUrl},
                Database = _database
            };

            return (DocumentStore)store.Initialize();
        }

        private async Task CreateDatabaseInCluster(IDocumentStore store, string node)
        {
            var record = new DatabaseRecord(_database)
            {
                Topology = new DatabaseTopology
                {
                    Members = new List<string>
                    {
                        node
                    }
                }
            };


            var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(record));

            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
            }
        }

        private async Task DeleteDatabaseInCluster(string databaseName, IDocumentStore store, string fromNode)
        {
            var databaseResult = await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, true, fromNode));

            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
            }
        }
    }
}
