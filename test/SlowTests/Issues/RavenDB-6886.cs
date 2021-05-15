using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    // ReSharper disable once InconsistentNaming
    public class RavenDB_6886 : ClusterTestBase
    {
        public RavenDB_6886(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public async Task Cluster_identity_for_single_document_should_work()
        {
            const int clusterSize = 3;
            var cluster = await CreateRaftCluster(clusterSize, watcherCluster: true);
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                using (var session = leaderStore.OpenSession())
                {
                    //id ending with "/" should trigger cluster identity id so
                    //after tx commit, the id would be "users/1"
                    session.Store(new User { Name = "John Dow" }, "users|");
                    session.SaveChanges();
                }

                using (var session = leaderStore.OpenSession())
                {
                    var users = session.Query<User>().Where(x => x.Name.StartsWith("John")).ToList();

                    Assert.Equal(1, users.Count);
                    Assert.NotNull(users[0].Id);
                    Assert.Equal("users/1", users[0].Id);
                }
            }
        }

        [Fact]
        public async Task Cluster_identity_for_multiple_documents_on_different_nodes_should_work()
        {
            const int clusterSize = 3;
            const int docsInEachNode = 10;
            const string databaseName = "Cluster_identity_for_multiple_documents_on_different_nodes_should_work";
            var leaderServer = await CreateRaftClusterAndGetLeader(clusterSize);
            var followers = Servers.Where(s => s != leaderServer).ToList();
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = leaderServer,
                ModifyDatabaseName = s => databaseName,
                DeleteDatabaseOnDispose = false,
                CreateDatabase = false
            }))
            using (var followerA = GetDocumentStore(new Options
            {
                Server = followers[0],
                ModifyDatabaseName = s => databaseName,
                DeleteDatabaseOnDispose = false,
                CreateDatabase = false
            }))
            using (var followerB = GetDocumentStore(new Options
            {
                Server = followers[1],
                ModifyDatabaseName = s => databaseName,
                DeleteDatabaseOnDispose = false,
                CreateDatabase = false
            }))
            {
                await CreateDatabasesInCluster(clusterSize, databaseName, leaderStore);

                var leaderInput = Task.Run(() =>
                {
                    using (var session = leaderStore.OpenSession())
                    {
                        //id ending with "/" should trigger cluster identity id so
                        //after tx commit, the id would be "users/1"
                        for (int i = 0; i < docsInEachNode; i++)
                        {
                            session.Store(new User { Name = "John Dow" }, "users|");
                        }
                        session.SaveChanges();
                    }
                });

                var followerAInput = Task.Run(() =>
                {
                    using (var session = followerA.OpenSession())
                    {
                        for (int i = 0; i < docsInEachNode; i++)
                        {
                            session.Store(new User { Name = "Jane Dow" }, "users|");
                        }
                        session.SaveChanges();
                    }
                });

                var followerBInput = Task.Run(() =>
                {
                    using (var session = followerB.OpenSession())
                    {
                        for (int i = 0; i < docsInEachNode; i++)
                        {
                            session.Store(new User { Name = "Jake Dow" }, "users|");
                        }
                        session.SaveChanges();
                    }
                });

                await Task.WhenAll(leaderInput, followerAInput, followerBInput);

                //now add markers to test when the replication finishes..
                using (var session = followerA.OpenSession())
                {
                    session.Store(new User { Name = "foobar1" }, "marker A");
                    session.SaveChanges();
                }
                using (var session = followerB.OpenSession())
                {
                    session.Store(new User { Name = "foobar2" }, "marker B");
                    session.SaveChanges();
                }

                //make sure all replications that need to be done is done...
                Assert.True(WaitForDocument<User>(leaderStore, "marker A", doc => true));
                Assert.True(WaitForDocument<User>(leaderStore, "marker B", doc => true));

                await WaitForRollingAutoIndexDeployment(leaderStore,async () =>
                {
                    using (var session = leaderStore.OpenAsyncSession())
                    {
                        var users = await session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Name.StartsWith("J"))
                            .ToListAsync();

                        Assert.Equal(docsInEachNode * 3, users.Count);
                        for (var i = 1; i <= docsInEachNode * 3; i++)
                        {
                            Assert.True(users.Any(u => u.Id == "users/" + i));
                        }
                    }
                });
            }
        }

        public async Task WaitForRollingAutoIndexDeployment(IDocumentStore store, Func<Task> act, TimeSpan? timeout = null)
        {
            try
            {
                await act();
            }
            catch
            {
                // expected

                await WaitForRollingIndexDeployment(store, timeout);
                await act();
            }

            await WaitForRollingIndexDeployment(store, timeout);
        }

        public async Task WaitForRollingIndexDeployment(IDocumentStore store, TimeSpan? timeout = null)
        {
            timeout ??= TimeSpan.FromSeconds(15);
            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout)
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                if (record.RollingIndexes == null)
                    return;

                var any = record.RollingIndexes.Values.Any(i => i.ActiveDeployments.Values.Any(d => d.State != RollingIndexState.Done));
                if (any == false)
                    return;

                await Task.Delay(250);
            }

            throw new TimeoutException("Waited too long for rolling index deployment");
        }

        [Fact]
        public async Task Cluster_identity_for_single_document_on_different_nodes_should_work()
        {
            const int clusterSize = 3;
            const string databaseName = "Cluster_identity_for_multiple_documents_on_different_nodes_should_work";
            var leaderServer = await CreateRaftClusterAndGetLeader(clusterSize);
            var followers = Servers.Where(s => s != leaderServer).ToList();
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = leaderServer,
                ModifyDatabaseName = s => databaseName,
                DeleteDatabaseOnDispose = false,
                CreateDatabase = false
            }))
            using (var followerA = GetDocumentStore(new Options
            {
                Server = followers[0],
                ModifyDatabaseName = s => databaseName,
                DeleteDatabaseOnDispose = false,
                CreateDatabase = false
            }))
            using (var followerB = GetDocumentStore(new Options
            {
                Server = followers[1],
                ModifyDatabaseName = s => databaseName,
                DeleteDatabaseOnDispose = false,
                CreateDatabase = false
            }))
            {
                await CreateDatabasesInCluster(clusterSize, databaseName, leaderStore);
                using (var session = leaderStore.OpenSession())
                {
                    //id ending with "/" should trigger cluster identity id so
                    //after tx commit, the id would be "users/1"
                    session.Store(new User { Name = "John Dow" }, "users|");
                    session.SaveChanges();
                }

                using (var session = followerA.OpenSession())
                {
                    session.Store(new User { Name = "Jane Dow" }, "users|");
                    session.SaveChanges();
                }

                using (var session = followerB.OpenSession())
                {
                    session.Store(new User { Name = "Jake Dow" }, "users|");
                    session.SaveChanges();
                }

                using (var session = followerA.OpenSession())
                {
                    session.Store(new User { Name = "foobar1" }, "marker A");
                    session.SaveChanges();
                }

                using (var session = followerB.OpenSession())
                {
                    session.Store(new User { Name = "foobar2" }, "marker B");
                    session.SaveChanges();
                }

                //make sure all replications that need to be done are done...
                Assert.True(WaitForDocument<User>(leaderStore, "marker A", doc => true, timeout: 5000));
                Assert.True(WaitForDocument<User>(leaderStore, "marker B", doc => true, timeout: 5000));

                await WaitForRollingAutoIndexDeployment(leaderStore,() =>
                {
                    using (var session = leaderStore.OpenSession())
                    {
                        var users = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Name.StartsWith("J"))
                            .OrderBy(x => x.Id)
                            .ToList();

                        Assert.Equal(3, users.Count);
                        Assert.Equal("users/1", users[0].Id);
                        Assert.Equal("users/2", users[1].Id);
                        Assert.Equal("users/3", users[2].Id);
                    }

                    return Task.CompletedTask;
                });
            }
        }

        [Fact]
        public async Task Cluster_identity_for_single_document_in_parallel_on_different_nodes_should_work()
        {
            //LoggingSource.Instance.SetupLogMode(LogMode.Information, "D:\\raven-test-log");
            const int clusterSize = 3;
            const string databaseName = "Cluster_identity_for_multiple_documents_on_different_nodes_should_work";
            var leaderServer = await CreateRaftClusterAndGetLeader(clusterSize, leaderIndex: 2);
            var followers = Servers.Where(s => s != leaderServer).ToList();
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = leaderServer,
                ModifyDatabaseName = s => databaseName,
                DeleteDatabaseOnDispose = false,
                CreateDatabase = false
            }))
            using (var followerA = GetDocumentStore(new Options
            {
                Server = followers[0],
                ModifyDatabaseName = s => databaseName,
                DeleteDatabaseOnDispose = false,
                CreateDatabase = false
            }))
            using (var followerB = GetDocumentStore(new Options
            {
                Server = followers[1],
                ModifyDatabaseName = s => databaseName,
                DeleteDatabaseOnDispose = false,
                CreateDatabase = false
            }))
            {
                await CreateDatabasesInCluster(clusterSize, databaseName, leaderStore);

                Parallel.For(0, 5, _ =>
                {
                    Parallel.Invoke(() =>
                        {
                            using (var session = leaderStore.OpenSession())
                            {
                                //id ending with "/" should trigger cluster identity id so
                                //after tx commit, the id would be "users/1"
                                session.Store(new User { Name = "John Dow" }, "users|");
                                session.SaveChanges();
                            }
                        },
                        () =>
                        {
                            using (var session = followerA.OpenSession())
                            {
                                session.Store(new User { Name = "Jane Dow" }, "users|");
                                session.SaveChanges();
                            }
                        },
                        () =>
                        {
                            using (var session = followerB.OpenSession())
                            {
                                session.Store(new User { Name = "Jake Dow" }, "users|");
                                session.SaveChanges();
                            }
                        });
                });

                using (var session = followerA.OpenSession())
                {
                    session.Store(new User { Name = "foobar1" }, "marker A");
                    session.SaveChanges();
                }

                using (var session = followerB.OpenSession())
                {
                    session.Store(new User { Name = "foobar2" }, "marker B");
                    session.SaveChanges();
                }

                //make sure all replications that need to be done are done...
                Assert.True(WaitForDocument<User>(leaderStore, "marker A", doc => true, timeout: 5000));
                Assert.True(WaitForDocument<User>(leaderStore, "marker B", doc => true, timeout: 5000));

                await WaitForRollingAutoIndexDeployment(leaderStore,() =>
                {
                    using (var session = leaderStore.OpenSession())
                    {
                        var users = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Name.StartsWith("J"))
                            .OrderBy(x => x.Id)
                            .ToList();

                        Assert.Equal(15, users.Count);
                        for (int i = 1; i <= 15; i++)
                            Assert.True(users.Any(x => x.Id == "users/" + i));
                    }

                    return Task.CompletedTask;
                });
            }
        }


        [Fact]
        public async Task Cluster_identity_for_multiple_documents_on_leader_should_work()
        {
            const int clusterSize = 3;
            var cluster = await CreateRaftCluster(clusterSize, watcherCluster: true);
            using (var leaderStore = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                using (var session = leaderStore.OpenSession())
                {
                    //id ending with "/" should trigger cluster identity id so
                    //after tx commit, the id would be "users/1"
                    session.Store(new User { Name = "John Dow" }, "users|");
                    session.SaveChanges();
                }
                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User { Name = "Jane Dow" }, "users|");
                    session.SaveChanges();
                }
                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User { Name = "Jake Dow" }, "users|");
                    session.SaveChanges();
                }

                await WaitForRollingAutoIndexDeployment(leaderStore,() =>
                {
                    using (var session = leaderStore.OpenSession())
                    {
                        var users = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Name.StartsWith("J"))
                            .OrderBy(x => x.Id)
                            .ToList();

                        Assert.Equal(3, users.Count);
                        Assert.Equal("users/1", users[0].Id);
                        Assert.Equal("users/2", users[1].Id);
                        Assert.Equal("users/3", users[2].Id);
                    }

                    return Task.CompletedTask;
                });
            }
        }

        private async Task CreateDatabasesInCluster(int clusterSize, string databaseName, IDocumentStore store)
        {
            try
            {
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(databaseName), clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.RaftCommandIndex);
                }
            }
            catch (TimeoutException te)
            {
                throw new TimeoutException($"{te.Message} {GetLastStatesFromAllServersOrderedByTime() }");
            }
        }
    }
}
