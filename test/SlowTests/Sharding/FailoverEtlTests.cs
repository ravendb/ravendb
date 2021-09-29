using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Sharding;
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
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding
{
    public class FailoverEtlTests : ShardedClusterTestBase
    {
        public FailoverEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReplicateFromSingleSource()
        {
            var srcDb = "ReplicateFromSingleSourceSrc";
            var dstDb = "ReplicateFromSingleSourceDst";
            var srcCluster = await CreateRaftCluster(3);
            var dstCluster = await CreateRaftCluster(1);

            var srcNodes = await CreateShardedDatabaseInCluster(srcDb, replicationFactor: 3, srcCluster, certificate: null);
            var destNode = await CreateShardedDatabaseInCluster(dstDb, replicationFactor: 1, dstCluster, certificate: null);
            var node = srcNodes.Servers.First(x => x.ServerStore.NodeTag != srcCluster.Leader.ServerStore.NodeTag).ServerStore.NodeTag;

            using (var src = new DocumentStore()
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
                await ActionWithLeader((l) => l.ServerStore.RemoveFromClusterAsync(node), srcNodes.Servers);
                //Thread.Sleep(100000);
                await originalTaskNode.ServerStore.WaitForState(RachisState.Passive, CancellationToken.None);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe2"
                    }, "users/2");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "Joe Doe2", 60_000));
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
            var srcCluster = await CreateRaftCluster(3, leaderIndex: 0);
            var dstCluster = await CreateRaftCluster(3);

            var srcNodes = await CreateShardedDatabaseInCluster(srcDb, replicationFactor: 3, srcCluster, certificate: null);
            var destNode = await CreateShardedDatabaseInCluster(dstDb, replicationFactor: 3, dstCluster, certificate: null);
            var node = srcNodes.Servers.First(x => x.ServerStore.NodeTag != srcCluster.Leader.ServerStore.NodeTag).ServerStore.NodeTag;

            using (var src = new DocumentStore
            {
                Urls = new[] { srcCluster.Leader.WebUrl },
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
                var myTag = srcCluster.Leader.ServerStore.NodeTag;
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
        public async Task WillWorkAfterResponsibleNodeRestart()
        {
            var srcDb = "ETL-src";
            var dstDb = "ETL-dst";

            var srcCluster = await CreateRaftCluster(3, shouldRunInMemory: false);
            var dstCluster = await CreateRaftCluster(1);
            var srcNodes = await CreateShardedDatabaseInCluster(srcDb, replicationFactor: 2, srcCluster, certificate: null);
            var destNode = await CreateShardedDatabaseInCluster(dstDb, replicationFactor: 1, dstCluster, certificate: null);
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

        [Fact]
        public async Task ShouldSendCounterChangeMadeInCluster()
        {
            var srcDb = "13288-src";
            var dstDb = "13288-dst";

            var srcCluster = await CreateRaftCluster(2);
            var dstCluster = await CreateRaftCluster(1);
            var srcNodes = await CreateShardedDatabaseInCluster(srcDb, 2, srcCluster);
            var destNode = await CreateShardedDatabaseInCluster(dstDb, 1, dstCluster);

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
                var connectionStringName = "my-etl";
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
                    MentorNode = "A"
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

                var aNode = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == "A");
                var bNode = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == "B");

                // modify counter on A node (mentor of ETL task)

                using (var aSrc = new DocumentStore
                {
                    Urls = new[] { aNode.WebUrl },
                    Database = srcDb,
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    using (var session = aSrc.OpenSession())
                    {
                        session.Store(new User()
                        {
                            Name = "Joe Doe"
                        }, "users/1");

                        session.CountersFor("users/1").Increment("likes");

                        session.SaveChanges();
                    }
                }

                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Joe Doe", 30_000));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);

                    var counter = session.CountersFor("users/1").Get("likes");

                    Assert.NotNull(counter);
                    Assert.Equal(1, counter.Value);
                }

                // modify counter on B node (not mentor)

                using (var bSrc = new DocumentStore
                {
                    Urls = new[] { bNode.WebUrl },
                    Database = srcDb,
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    using (var session = bSrc.OpenSession())
                    {
                        session.CountersFor("users/1").Increment("likes");

                        session.SaveChanges();
                    }
                }

                Assert.True(WaitForCounterReplication(new List<IDocumentStore>
                {
                    dest
                }, "users/1", "likes", 2, TimeSpan.FromSeconds(60)));
            }
        }
    }
}
