using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Identities;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7059 : ClusterTestBase
    {
        private readonly string _fileName = Path.Combine(RavenTestHelper.NewDataPath(nameof(RavenDB_7059), 0, forceCreateDir: true), $"{Guid.NewGuid()}.ravendump");

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public async Task Cluster_identity_should_work_with_smuggler()
        {
            const int clusterSize = 3;
            const string databaseName = "Cluster_identity_for_single_document_should_work";
            var leaderServer = await CreateRaftClusterAndGetLeader(clusterSize);
            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Database = databaseName
            })
            {
                leaderStore.Initialize();

                await CreateDatabasesInCluster(clusterSize, databaseName, leaderStore);
                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User { Name = "John Dow" }, "users|");
                    session.Store(new User { Name = "Jake Dow" }, "users|");
                    session.Store(new User { Name = "Jessie Dow" }, "users|");
                    session.SaveChanges();
                }

                WaitForIdentity(leaderStore, "users", 3);

                var operation = await leaderStore.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), _fileName);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }

            foreach (var server in Servers)
            {
                server.Dispose();
            }
            Servers.Clear();

            leaderServer = await CreateRaftClusterAndGetLeader(clusterSize);
            using (var leaderStore = new DocumentStore
            {
                Urls = new[] { leaderServer.WebUrl },
                Database = databaseName
            })
            {
                leaderStore.Initialize();

                await CreateDatabasesInCluster(clusterSize, databaseName, leaderStore);
                var operation = await leaderStore.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), _fileName);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User { Name = "Julie Dow" }, "users|");
                    session.SaveChanges();
                }

                using (var session = leaderStore.OpenSession())
                {
                    var julieDow = session.Query<User>().First(u => u.Name.StartsWith("Julie"));
                    Assert.Equal("users/4", julieDow.Id);

                }
            }
        }

        private async Task CreateDatabasesInCluster(int clusterSize, string databaseName, IDocumentStore store)
        {
            var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(databaseName), clusterSize));
            Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
            }
        }

        public override void Dispose()
        {
            base.Dispose();
            IOExtensions.DeleteFile(_fileName);
        }

        public bool WaitForIdentity(DocumentStore store, string collection, long identityToWaitFor, int timeout = 2000)
        {
            var sw = Stopwatch.StartNew();
            while (true)
            {
                if (sw.ElapsedMilliseconds >= timeout)
                    return false;


                var identities = store.Maintenance.Send(new GetIdentitiesOperation());
                if (identities.TryGetValue(collection, out long value) && identityToWaitFor >= value)
                {
                    break;
                }
                Thread.Sleep(250);
            }
            return true;
        }
    }
}
