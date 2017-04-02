using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationWithFailover : ClusterTestBase
    {
        private class User
        {
            public string Name;
        }

        [Fact(Skip = "WIP RavenDB-6602")]
        public async Task LoadDocumentsWithFailOver()
        {
            var leaderServer = await CreateRaftClusterAndGetLeader(2);
            var slaveServer = Servers.First(srv => ReferenceEquals(srv, leaderServer) == false);

            const string databaseName = "LoadDocumentsWithFailOver";
            using (var master = new DocumentStore
            {
                Url = leaderServer.WebUrls[0],
                DefaultDatabase = databaseName
            })
            using (var slave = new DocumentStore
            {
                Url = slaveServer.WebUrls[0],
                DefaultDatabase = databaseName
            })
            {
                master.Initialize();
                slave.Initialize();

                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);

                //since we have only Raft clusters, it is enough to create database only on one server
                var databaseResult = master.Admin.Server.Send(new CreateDatabaseOperation(doc, 2));
                await WaitForEtagInCluster(databaseResult.ETag ?? 0, TimeSpan.FromSeconds(5));

                var requestExecutor = master.GetRequestExecuter();
                await Task.WhenAny(requestExecutor.UpdateTopologyAsync(), Task.Delay(TimeSpan.FromSeconds(10)));

                //TODO for Karmel: refactor this test so it uses replication when raft based topology replication is implemented
                SetupReplicationOnDatabaseTopology(requestExecutor.TopologyNodes);
              
                using (var session = master.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges();
                    session.Store(new User { Name = "Idan" }, "users/1");
                    session.Store(new User { Name = "Shalom" }, "users/2");
                    session.SaveChanges();
                }

                using (var session = master.OpenSession())
                {
                    var user1 = session.Load<User>("users/1");
                    Assert.NotNull(user1);
                    Assert.Equal("Idan", user1.Name);
                    var user2 = session.Load<User>("users/2");
                    Assert.NotNull(user2);
                    Assert.Equal("Shalom", user2.Name);
                }

                var result = master.Admin.Server.Send(new DisableDatabaseToggleOperation(master.DefaultDatabase, true));
                
                Assert.True(result.Disabled);
                Assert.Equal(master.DefaultDatabase, result.Name);

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