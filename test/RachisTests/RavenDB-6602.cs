using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{
    // ReSharper disable once InconsistentNaming
    public class RavenDB_6602 : ClusterTestBase
    {
        public class User
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task RequestExecutor_failover_with_only_one_database_should_properly_fail()
        {
            var leader = await CreateRaftClusterAndGetLeader(1);
            const int replicationFactor = 1;
            const string databaseName = "RequestExecutor_failover_with_only_one_database_should_properly_fail";
            using (var store = new DocumentStore
            {
                Database = databaseName,
                Urls = leader.WebUrls
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));

                Assert.True((databaseResult.ETag ?? 0) > 0); //sanity check                
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.ETag ?? 0, TimeSpan.FromSeconds(5));

                //before dispose there is such a document
                using (var session = store.OpenSession(databaseName))
                {
                    session.Store(new User { Name = "John Doe" }, "users/1");
                    session.SaveChanges();
                }

                DisposeServerAndWaitForFinishOfDisposal(leader);

                using (var session = store.OpenSession(databaseName))
                {
                    Assert.Throws<AllTopologyNodesDownException>(() => session.Load<User>("users/1"));
                }
            }
        }

        [Fact]
        public async Task RequestExecutor_failover_to_database_topology_should_work()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
            const int replicationFactor = 2;
            const string databaseName = nameof(RequestExecutor_failover_to_database_topology_should_work);
            using (var store = new DocumentStore
            {
                Database = databaseName,
                Urls = leader.WebUrls
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));

                Assert.True((databaseResult.ETag ?? 0) > 0); //sanity check                
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.ETag ?? 0, TimeSpan.FromSeconds(5));

                await ((DocumentStore)store).ForceUpdateTopologyFor(databaseName);
                var requestExecutor = ((DocumentStore)store).GetRequestExecutor(databaseName);

                using (var session = store.OpenSession(databaseName))
                {
                    session.Store(new User { Name = "John Doe" }, "users/1");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    requestExecutor.TopologyNodes,
                    "users/1",
                    u => u.Name.Equals("John Doe"),
                    TimeSpan.FromSeconds(10)));

                using (var session = store.OpenSession(databaseName))
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }

                var serverToDispose = Servers.FirstOrDefault(
                    srv => srv.ServerStore.NodeTag.Equals(requestExecutor.TopologyNodes[0].ClusterTag, StringComparison.OrdinalIgnoreCase));
                Assert.NotNull(serverToDispose); //precaution

                //dispose the first topology node, forcing the requestExecutor to failover to the next one                
                DisposeServerAndWaitForFinishOfDisposal(serverToDispose);

                using (var session = store.OpenSession(databaseName))
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }
            }
        }


    }
}
