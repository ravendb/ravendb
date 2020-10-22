using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.ServerWide;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests.DatabaseCluster
{
    public class EtlFailover : ReplicationTestBase
    {
        public EtlFailover(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReplicateFromSingleSource()
        {
            var srcDb = "ReplicateFromSingleSourceSrc";
            var dstDb = "ReplicateFromSingleSourceDst";
            var srcRaft = await CreateRaftClusterAndGetLeader(3);
            var dstRaft = await CreateRaftClusterAndGetLeader(1);
            var srcNodes = await CreateDatabaseInCluster(srcDb, 3, srcRaft.WebUrl);
            var destNode = await CreateDatabaseInCluster(dstDb, 1, dstRaft.WebUrl);
            var node = srcNodes.Servers.First(x => x.ServerStore.NodeTag != srcRaft.ServerStore.NodeTag).ServerStore.NodeTag;

            using (var src = new DocumentStore
            {
                Urls = srcNodes.Servers.Select(s => s.WebUrl).ToArray(),
                Database = srcDb,
            }.Initialize())
            using (var dest = new DocumentStore
            {
                Urls = new[] { destNode.Servers[0].WebUrl },
                Database = dstDb,
            }.Initialize())
            {
                var connectionStringName = "EtlFailover";
                var urls = new[] { destNode.Servers[0].WebUrl };
                var config = new RavenEtlConfiguration()
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>(new[] {"Users"}),
                            Script = null,
                            ApplyToAllDocuments = false,
                            Disabled = false
                        }
                    },
                    LoadRequestTimeoutInSec = 30,
                    MentorNode = node
                };
                var connectionString = new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dest.Database,
                    TopologyDiscoveryUrls = urls,
                };

                var result = src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                Assert.NotNull(result.RaftCommandIndex);

                src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(config));
                var originalTaskNode = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == node);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Joe Doe", 30_000));
                await srcRaft.ServerStore.RemoveFromClusterAsync(node);
                await originalTaskNode.ServerStore.WaitForState(RachisState.Passive, CancellationToken.None);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe2"
                    }, "users/2");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "Joe Doe2", 30_000));
                Assert.Throws<NodeIsPassiveException>(() =>
                {
                    using (var originalSrc = new DocumentStore
                    {
                        Urls = new[] { originalTaskNode.WebUrl },
                        Database = srcDb,
                        Conventions = new DocumentConventions
                        {
                            DisableTopologyUpdates = true
                        }
                    }.Initialize())
                    {
                        using (var session = originalSrc.OpenSession())
                        {
                            session.Store(new User()
                            {
                                Name = "Joe Doe3"
                            }, "users/3");

                            session.SaveChanges();
                        }
                    }
                });
            }
        }

        [Fact]
        public async Task EtlDestinationFailoverBetweenNodesWithinSameCluster()
        {
            var srcDb = "EtlDestinationFailoverBetweenNodesWithinSameClusterSrc";
            var dstDb = "EtlDestinationFailoverBetweenNodesWithinSameClusterDst";
            var srcRaft = await CreateRaftClusterAndGetLeader(3, leaderIndex: 0, allServers: false);
            var dstRaft = await CreateRaftClusterAndGetLeader(3, allServers: false);
            var srcNodes = await CreateDatabaseInCluster(srcDb, 3, srcRaft.WebUrl);
            var destNode = await CreateDatabaseInCluster(dstDb, 3, dstRaft.WebUrl);

            using (var src = new DocumentStore
            {
                Urls = new[] { srcRaft.WebUrl },
                Database = srcDb,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            using (var dest = new DocumentStore
            {
                Urls = destNode.Servers.Select(s => s.WebUrl).ToArray(),
                Database = dstDb,
            }.Initialize())
            {
                var myTag = srcRaft.ServerStore.NodeTag;
                var connectionStringName = "EtlFailover";
                var urls = new[] { "http://google.com", "http://localhost:1232", destNode.Servers[0].WebUrl };
                var conflig = new RavenEtlConfiguration()
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>(new[] { "Users" }),
                            Script = null,
                            ApplyToAllDocuments = false,
                            Disabled = false
                        }
                    },
                    LoadRequestTimeoutInSec = 10,
                    MentorNode = myTag
                };
                var connectionString = new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dest.Database,
                    TopologyDiscoveryUrls = urls,
                };

                var result = src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                Assert.NotNull(result.RaftCommandIndex);

                var etlResult = src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(conflig));
                var database = await srcNodes.Servers.Single(s => s.ServerStore.NodeTag == myTag)
                    .ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(srcDb);

                var etlDone = new ManualResetEventSlim();
                database.EtlLoader.BatchCompleted += x =>
                {
                    if (x.Statistics.LoadSuccesses > 0)
                        etlDone.Set();
                };

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users|");
                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));
                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Joe Doe", 30_000));

                // BEFORE THE FIX: this will change the database record and restart the ETL, which would fail the test.
                // we leave this here to make sure that such issue in future will fail the test immediately
                await src.Maintenance.SendAsync(new PutClientConfigurationOperation(new ClientConfiguration()));

                var taskInfo = (OngoingTaskRavenEtlDetails)src.Maintenance.Send(new GetOngoingTaskInfoOperation(etlResult.TaskId, OngoingTaskType.RavenEtl));
                Assert.NotNull(taskInfo);
                Assert.NotNull(taskInfo.DestinationUrl);
                Assert.Equal(myTag, taskInfo.ResponsibleNode.NodeTag);
                Assert.Null(taskInfo.Error);
                Assert.Equal(OngoingTaskConnectionStatus.Active, taskInfo.TaskConnectionStatus);

                etlDone.Reset();
                DisposeServerAndWaitForFinishOfDisposal(destNode.Servers.Single(s => s.WebUrl == taskInfo.DestinationUrl));

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Joe Doe2"
                    }, "users/2");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));
                Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "Joe Doe2", 30_000));
            }
        }

        [Fact]
        public async Task WillWorkAfterResponsibleNodeRestart_RavenDB_13237()
        {
            var srcDb = "ETL-src";
            var dstDb = "ETL-dst";

            var srcRaft = await CreateRaftClusterAndGetLeader(3, shouldRunInMemory: false);
            var dstRaft = await CreateRaftClusterAndGetLeader(1);
            var srcNodes = await CreateDatabaseInCluster(srcDb, 2, srcRaft.WebUrl);
            var destNode = await CreateDatabaseInCluster(dstDb, 1, dstRaft.WebUrl);

            using (var src = new DocumentStore
            {
                Urls = srcNodes.Servers.Select(s => s.WebUrl).ToArray(),
                Database = srcDb,
            }.Initialize())
            using (var dest = new DocumentStore
            {
                Urls = new[] { destNode.Servers[0].WebUrl },
                Database = dstDb,
            }.Initialize())
            {
                var name = "FailoverAfterRestart";
                var urls = new[] { destNode.Servers[0].WebUrl };
                var config = new RavenEtlConfiguration()
                {
                    Name = name,
                    ConnectionStringName = name,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {name}",
                            Collections = new List<string>(new[] {"Users"}),
                            Script = null,
                            ApplyToAllDocuments = false,
                            Disabled = false
    }
                    },
                    LoadRequestTimeoutInSec = 30,
                };
                var connectionString = new RavenConnectionString
                {
                    Name = name,
                    Database = dest.Database,
                    TopologyDiscoveryUrls = urls,
                };

                var result = src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                Assert.NotNull(result.RaftCommandIndex);

                src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(config));

                var ongoingTask = src.Maintenance.Send(new GetOngoingTaskInfoOperation(name, OngoingTaskType.RavenEtl));

                var responsibleNodeNodeTag = ongoingTask.ResponsibleNode.NodeTag;
                var originalTaskNodeServer = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == responsibleNodeNodeTag);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Joe Doe", 30_000));

                var originalResult = DisposeServerAndWaitForFinishOfDisposal(originalTaskNodeServer);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe2"
                    }, "users/2");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "Joe Doe2", 30_000));

                ongoingTask = src.Maintenance.Send(new GetOngoingTaskInfoOperation(name, OngoingTaskType.RavenEtl));

                var currentNodeNodeTag = ongoingTask.ResponsibleNode.NodeTag;
                var currentTaskNodeServer = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == currentNodeNodeTag);

                // start server which originally was handling ETL task
                GetNewServer(new ServerCreationOptions
                {
                    CustomSettings = new Dictionary<string, string>
                        {
                            {RavenConfiguration.GetKey(x => x.Core.ServerUrls), originalResult.Url}
                        },
                    RunInMemory = false,
                    DeletePrevious = false,
                    DataDirectory = originalResult.DataDirectory
                });

                using (var store = new DocumentStore
                {
                    Urls = new[] { originalResult.Url },
                    Database = srcDb,
                    Conventions =
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User()
                        {
                            Name = "Joe Doe3"
                        }, "users/3");

                        session.SaveChanges();
                    }

                    Assert.True(WaitForDocument<User>(dest, "users/3", u => u.Name == "Joe Doe3", 30_000));

                    // force disposing second node to ensure the original node is reponsible for ETL task again
                    DisposeServerAndWaitForFinishOfDisposal(currentTaskNodeServer);

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User()
                        {
                            Name = "Joe Doe4"
                        }, "users/4");

                        session.SaveChanges();
                    }

                    Assert.True(WaitForDocument<User>(dest, "users/4", u => u.Name == "Joe Doe4", 30_000));
                }
            }
        }
    }
}
