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

namespace FastTests.Server.Replication
{
    public class DisableDatabasePropagationInRaftCluster : ClusterTestBase
    {
        private class User
        {
            public string Name;
        }

        [Fact]
        public async Task DisableDatabaseToggleOperation_should_propagate_through_raft_cluster()
        {
            var leaderServer = await CreateRaftClusterAndGetLeader(2, shouldRunInMemory:false);
            var slaveServer = Servers.First(srv => ReferenceEquals(srv, leaderServer) == false);

            const string databaseName = "DisableDatabaseToggleOperation_should_propagate_through_raft_cluster";
            using (var master = new DocumentStore
            {
                Urls = leaderServer.WebUrls,
                Database = databaseName
            })
            using (var slave = new DocumentStore
            {
                Urls = leaderServer.WebUrls,
                Database = databaseName
            })
            {
                master.Initialize();
                slave.Initialize();

                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);

                //since we have only Raft clusters, it is enough to create database only on one server
                var databaseResult = master.Admin.Server.Send(new CreateDatabaseOperation(doc, 2));
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.ETag ?? 0, TimeSpan.FromSeconds(5));

                var requestExecutor = master.GetRequestExecutor();
                //await Task.WhenAny(requestExecutor.UpdateTopologyAsync(), Task.Delay(TimeSpan.FromSeconds(10)));
                
                using (var session = master.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges();
                    session.Store(new User {Name = "Idan"}, "users/1");
                    session.Store(new User {Name = "Shalom"}, "users/2");
                    session.SaveChanges();
                }

                Assert.True(await WaitForDocumentInClusterAsync<User>(requestExecutor.TopologyNodes, "users/1", user => true, TimeSpan.FromSeconds(10)));
                Assert.True(await WaitForDocumentInClusterAsync<User>(requestExecutor.TopologyNodes, "users/2", user => true, TimeSpan.FromSeconds(10)));

                using (var session = master.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    Assert.NotNull(user1);
                    Assert.Equal("Idan", user1.Name);
                    var user2 = session.Load<User>("users/2");
                    Assert.NotNull(user2);
                    Assert.Equal("Shalom", user2.Name);
                }

                var result = master.Admin.Server.Send(new DisableDatabaseToggleOperation(master.Database, true));

                Assert.True(result.Success);
                Assert.True(result.Disabled);
                Assert.Equal(master.Database, result.Name);

                //wait until disabled databases unload, this is an immediate operation
                Assert.True(await WaitUntilDatabaseHasState(master, TimeSpan.FromSeconds(10), isLoaded: false));
                Assert.True(await WaitUntilDatabaseHasState(slave, TimeSpan.FromSeconds(10), isLoaded: false));

                using (var session = master.OpenSession())
                {
                    //disable database is propagated through cluster, so both master and slave would be disabled after 
                    //sending DisableDatabaseToggleOperation
                    //note: the handler that receives DisableDatabaseToggleOperation "waits" until the cluster has a quorum
                    //thus, session.Load() operation would fail now

                    var e = Assert.Throws<AllTopologyNodesDownException>(() => session.Load<User>("users/1"));
                    Assert.IsType<AggregateException>(e.InnerException);
                    var ae = e.InnerException as AggregateException;
                    foreach (var ie in ae.InnerExceptions)
                        Assert.IsType<UnsuccessfulRequestException>(ie);
                }

                //now we enable all databases, so it should propagate as well and make them available for requests
                result = master.Admin.Server.Send(new DisableDatabaseToggleOperation(master.Database, false));
                
                Assert.True(result.Success);
                Assert.False(result.Disabled);
                Assert.Equal(master.Database, result.Name);
                using (var session = master.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    Assert.NotNull(user1);
                    Assert.Equal("Idan", user1.Name);
                    var user2 = session.Load<User>("users/2");
                    Assert.NotNull(user2);
                    Assert.Equal("Shalom", user2.Name);
                }
            }
        }     
    }
}