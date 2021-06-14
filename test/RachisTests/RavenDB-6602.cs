using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    // ReSharper disable once InconsistentNaming
    public class RavenDB_6602 : ClusterTestBase
    {
        public RavenDB_6602(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task RequestExecutor_failover_with_only_one_database_should_properly_fail()
        {
            var (_, leader) = await CreateRaftCluster(1);
            const int replicationFactor = 1;
            const string databaseName = "RequestExecutor_failover_with_only_one_database_should_properly_fail";
            using (var store = new DocumentStore
            {
                Database = databaseName,
                Urls = new[] {leader.WebUrl}
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = store.Maintenance.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));

                Assert.True(databaseResult.RaftCommandIndex > 0); //sanity check                
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(5));

                //before dispose there is such a document
                using (var session = store.OpenSession(databaseName))
                {
                    session.Store(new User { Name = "John Doe" }, "users/1");
                    session.SaveChanges();
                }

                await DisposeServerAndWaitForFinishOfDisposalAsync(leader);

                using (var session = store.OpenSession(databaseName))
                {
                    Assert.Throws<RavenException>(() => session.Load<User>("users/1"));
                }
            }
        }

        [Fact]
        public async Task RequestExecutor_failover_to_database_topology_should_work()
        {
            var (_, leader) = await CreateRaftCluster(3);
            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 2,
                DeleteDatabaseOnDispose = false // we bring one node down, so we can't delete the entire database, but we run in mem, so we don't care
            }))
            {
                using (var session = (DocumentSession)store.OpenSession())
                {
                    session.Store(new User { Name = "John Doe" }, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        session,
                        "users/1",
                        u => u.Name.Equals("John Doe"),
                        TimeSpan.FromSeconds(10)));
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }

                var requestExecutor = store.GetRequestExecutor();
                var serverToDispose = Servers.FirstOrDefault(
                    srv => srv.ServerStore.NodeTag.Equals(requestExecutor.TopologyNodes[0].ClusterTag, StringComparison.OrdinalIgnoreCase));
                Assert.NotNull(serverToDispose); //precaution

                //dispose the first topology node, forcing the requestExecutor to failover to the next one                
                await DisposeServerAndWaitForFinishOfDisposalAsync(serverToDispose);

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }
            }
        }
    }
}
