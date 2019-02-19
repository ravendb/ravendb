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
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class EtlFailover : ReplicationTestBase
    {
        [Fact]
        public async Task ReplicateFromSingleSource()
        {
            var srcDb = "ReplicateFromSingleSourceSrc";
            var dstDb = "ReplicateFromSingleSourceDst";
            var srcRaft = await CreateRaftClusterAndGetLeader(3);
            var dstRaft = await CreateRaftClusterAndGetLeader(1);
            var srcNodes = await CreateDatabaseInCluster(srcDb, 3, srcRaft.WebUrl);
            var destNode = await CreateDatabaseInCluster(dstDb, 1, dstRaft.WebUrl);

            using (var src = new DocumentStore
            {
                Urls = srcNodes.Servers.Select(s=>s.WebUrl).ToArray(),
                Database = srcDb,
            }.Initialize())
            using (var dest = new DocumentStore
            {
                Urls = new []{destNode.Servers[0].WebUrl},
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
                    MentorNode = "B"
                };
                var connectionString = new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dest.Database,
                    TopologyDiscoveryUrls = urls,
                };

                src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(config));
                var originalTaskNode = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == "B");
                
                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    },"users/1");

                    session.SaveChanges();
                }
                
                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Joe Doe", 30_000));
                await srcRaft.ServerStore.RemoveFromClusterAsync("B");
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
            var srcRaft = await CreateRaftClusterAndGetLeader(3, leaderIndex: 0);
            var dstRaft = await CreateRaftClusterAndGetLeader(3);
            var srcNodes = await CreateDatabaseInCluster(srcDb, 3, srcRaft.WebUrl);
            var destNode = await CreateDatabaseInCluster(dstDb, 3, dstRaft.WebUrl);

            using (var src = new DocumentStore
            {
                Urls = new[]{ srcRaft.WebUrl},
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

                src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
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
    }
}
