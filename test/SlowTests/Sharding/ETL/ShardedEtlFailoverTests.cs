using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.ETL.Olap;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.ETL
{
    public class FailoverEtlTests : ClusterTestBase
    {
        public FailoverEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        [InlineData(2)]
        [InlineData(3)]
        public async Task ReplicateFromSingleSource(int replicationFactor)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var srcDb = "ReplicateFromSingleSourceSrc";
            var dstDb = "ReplicateFromSingleSourceDst";
            var srcCluster = await CreateRaftCluster(3);
            var dstCluster = await CreateRaftCluster(1);

            var srcNodes = await ShardingCluster.CreateShardedDatabaseInCluster(srcDb, replicationFactor: replicationFactor, srcCluster, certificate: null);
            var destNode = await CreateDatabaseInCluster(dstDb, replicationFactor: 1, dstCluster.Leader.WebUrl, certificate: null);
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
                    LoadRequestTimeoutInSec = 30
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

                await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(node), srcCluster.Nodes);

                await originalTaskNode.ServerStore.WaitForState(RachisState.Passive, CancellationToken.None);

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Joe Doe2"
                    }, "users/2");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "Joe Doe2", 60_000));

                // add debug info to investigate RavenDB-20934
                // can revert this back to Assert.Throws when the issue is resolved

                Exception ex = null;
                try
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
                            session.Store(new User() { Name = "Joe Doe3" }, "users/3");

                            session.SaveChanges();
                        }
                    }
                }
                catch (Exception e)
                {
                    ex = e;
                }

                Assert.True(ex is NodeIsPassiveException, await AddDebugInfo(ex, originalTaskNode));

                /*Assert.Throws<NodeIsPassiveException>(() =>
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
                });*/
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task ReplicateFromSingleSource_Sharded_Destination()
        {
            var srcDb = "ReplicateFromSingleSourceSrc";
            var dstDb = "ReplicateFromSingleSourceDst";
            var srcCluster = await CreateRaftCluster(3);
            var dstCluster = await CreateRaftCluster(3);

            var srcNodes = await ShardingCluster.CreateShardedDatabaseInCluster(srcDb, replicationFactor: 3, srcCluster, certificate: null);
            var destNode = await ShardingCluster.CreateShardedDatabaseInCluster(dstDb, replicationFactor: 2, dstCluster, certificate: null);
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
                    LoadRequestTimeoutInSec = 30
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

                await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(node), srcNodes.Servers);
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

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task EtlDestinationFailoverBetweenNodesWithinSameCluster()
        {
            var srcDb = "EtlDestinationFailoverBetweenNodesWithinSameClusterSrc";
            var dstDb = "EtlDestinationFailoverBetweenNodesWithinSameClusterDst";
            var srcCluster = await CreateRaftCluster(3, leaderIndex: 0);
            var dstCluster = await CreateRaftCluster(3);

            var srcNodes = await ShardingCluster.CreateShardedDatabaseInCluster(srcDb, replicationFactor: 3, srcCluster, certificate: null);
            var destNode = await CreateDatabaseInCluster(dstDb, replicationFactor: 3, dstCluster.Leader.WebUrl, certificate: null);
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
                    LoadRequestTimeoutInSec = 10
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

                var databases = Sharding.GetShardsDocumentDatabaseInstancesFor(srcDb, srcNodes.Servers);

                var etlDone = new ManualResetEventSlim();
                await foreach (var db in databases)
                {
                    db.EtlLoader.BatchCompleted += x =>
                    {
                        if (x.Statistics.LoadSuccesses > 0)
                            etlDone.Set();
                    };
                }

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

                var taskInfo = (OngoingTaskRavenEtl)src.Maintenance.Send(new GetOngoingTaskInfoOperation(etlResult.TaskId, OngoingTaskType.RavenEtl));
                Assert.NotNull(taskInfo);

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Minor,
                    "uncomment this when RavenDB-19069 is fixed");
                /*
                Assert.NotNull(taskInfo.DestinationUrl);
                Assert.Equal(myTag, taskInfo.ResponsibleNode.NodeTag);
                Assert.Null(taskInfo.Error);
                Assert.Equal(OngoingTaskConnectionStatus.Active, taskInfo.TaskConnectionStatus);
                
                etlDone.Reset();
                DisposeServerAndWaitForFinishOfDisposal(destNode.Servers.Single(s => s.WebUrl == taskInfo.DestinationUrl));
                */

                etlDone.Reset();
                await DisposeServerAndWaitForFinishOfDisposalAsync(destNode.Servers.Single(s => s.WebUrl == destNode.Servers[0].WebUrl));

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

        [Fact(Skip = "RavenDB-19069 Sharded ETL OngoingTaskInfo is not implemented")]
        public async Task WillWorkAfterResponsibleNodeRestart()
        {
            var srcDb = "ETL-src";
            var dstDb = "ETL-dst";

            var srcCluster = await CreateRaftCluster(3, shouldRunInMemory: false);
            var dstCluster = await CreateRaftCluster(1);
            var srcNodes = await ShardingCluster.CreateShardedDatabaseInCluster(srcDb, replicationFactor: 2, srcCluster, certificate: null);
            var destNode = await CreateDatabaseInCluster(dstDb, replicationFactor: 1, dstCluster.Leader.WebUrl, certificate: null);
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

                var id = "users/1";
                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, id);

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, id, u => u.Name == "Joe Doe", 30_000));

                var dbRecord = await src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database));
                var shardedCtx = new ShardedDatabaseContext(srcNodes.Servers[0].ServerStore, dbRecord);
                var shardNumber = 0;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    shardNumber = shardedCtx.GetShardNumberFor(context, id);
                }

                var ongoingTask = src.Maintenance.Send(new GetOngoingTaskInfoOperation($"{name}${shardNumber}", OngoingTaskType.RavenEtl));

                var responsibleNodeNodeTag = ongoingTask.ResponsibleNode.NodeTag;

                Assert.NotNull(responsibleNodeNodeTag);

                var originalTaskNodeServer = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == responsibleNodeNodeTag);

                var originalResult = await DisposeServerAndWaitForFinishOfDisposalAsync(originalTaskNodeServer);

                id = "users/5";
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    Assert.Equal(shardNumber, shardedCtx.GetShardNumberFor(context, id));
                }

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe2"
                    }, id);

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, id, u => u.Name == "Joe Doe2", 30_000));

                /*                ongoingTask = src.Maintenance.Send(new GetOngoingTaskInfoOperation(name, OngoingTaskType.RavenEtl));

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
                                }*/
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Counters | RavenTestCategory.Sharding)]
        public async Task ShouldSendCounterChangeMadeInCluster()
        {
            var srcDb = "13288-src";
            var dstDb = "13288-dst";

            var srcCluster = await CreateRaftCluster(2);
            var dstCluster = await CreateRaftCluster(1);
            var srcNodes = await ShardingCluster.CreateShardedDatabaseInCluster(srcDb, 2, srcCluster, shards: 2);
            var destNode = await CreateDatabaseInCluster(dstDb, 1, dstCluster.Leader.WebUrl);

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
                    LoadRequestTimeoutInSec = 30
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

                Assert.True(Replication.WaitForCounterReplication(new List<IDocumentStore>
                {
                    dest
                }, "users/1", "likes", 2, TimeSpan.FromSeconds(60)));
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task OlapTaskShouldBeHighlyAvailable()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var leader = cluster.Leader;
            var followers = cluster.Nodes.Where(n => n != leader).Select(n => n.ServerStore.NodeTag).ToList();

            var options = new Options
            {
                DatabaseMode = RavenDatabaseMode.Sharded,
                ModifyDatabaseRecord = r =>
                {
                    r.Sharding = new ShardingConfiguration
                    {
                        Shards = new Dictionary<int, DatabaseTopology>(3),
                        Orchestrator = new OrchestratorConfiguration
                        {
                            Topology = new OrchestratorTopology
                            {
                                ReplicationFactor = 3
                            }
                        }
                    };

                    for (int shardNumber = 0; shardNumber < 3; shardNumber++)
                    {
                        r.Sharding.Shards[shardNumber] = new DatabaseTopology
                        {
                            ReplicationFactor = 2,
                            Members = followers
                        };
                    }
                },
                Server = leader
            };

            using var store = GetDocumentStore(options);

            var baseline = new DateTime(2020, 1, 1);
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 31; i++)
                {
                    await session.StoreAsync(new Query.Order
                    {
                        Id = $"orders/{i}",
                        OrderedAt = baseline.AddDays(i),
                        ShipVia = $"shippers/{i}",
                        Company = $"companies/{i}"
                    });
                }

                await session.SaveChangesAsync();
            }

            var etlsDone = Sharding.Etl.WaitForEtlOnAllShardsInCluster(store.Database, (n, s) => s.LoadSuccesses > 0);

            var script = @"
var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key),
    {
        OrderId: id(this),
        Company : this.Company,
        ShipVia : this.ShipVia
    });
";

            var connectionStringName = $"{store.Database} to Local";
            var configName = "olap-s3";
            var transformationName = "MonthlyOrders";
            var path = NewDataPath(forceCreateDir: true);

            var configuration = new OlapEtlConfiguration
            {
                Name = configName,
                ConnectionStringName = connectionStringName,
                RunFrequency = LocalTests.DefaultFrequency,
                Transforms =
                {
                    new Transformation
                    {
                        Name = transformationName,
                        Collections = new List<string> {"Orders"},
                        Script = script
                    }
                }
            };


            Etl.AddEtl(store, configuration, new OlapConnectionString
            {
                Name = connectionStringName,
                LocalSettings = new LocalSettings
                {
                    FolderPath = path
                }
            });

            var timeout = TimeSpan.FromMinutes(2);
            var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
            Assert.True(WaitHandle.WaitAll(waitHandles, timeout), await Etl.GetEtlDebugInfo(store.Database, timeout, RavenDatabaseMode.Sharded));

            var files = Directory.GetFiles(path, "*.*", SearchOption.AllDirectories);
            var count = files.Length;
            Assert.True(count > 0);

            string serverToRemove = null;
            foreach (var node in cluster.Nodes)
            {
                // select one of the responsible nodes as the node to remove
                if (serverToRemove != null)
                    break;

                foreach (var task in node.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database))
                {
                    var databaseInstance = await task;
                    if (databaseInstance.EtlLoader.Processes.Length > 0)
                    {
                        serverToRemove = node.ServerStore.NodeTag;
                        break;
                    }
                }
            }

            Assert.NotNull(serverToRemove);
            Assert.Contains(serverToRemove, followers);
            await DisposeAndRemoveServer(cluster.Nodes.Single(n => n.ServerStore.NodeTag == serverToRemove));

            await WaitForValueAsync(async () =>
            {
                var shardingConfig = await Sharding.GetShardingConfigurationAsync(store);
                return shardingConfig.Orchestrator.Topology.Members.Contains(serverToRemove);
            }, expectedVal: false);


            etlsDone = Sharding.Etl.WaitForEtlOnAllShardsInCluster(store.Database, (n, s) => s.LoadSuccesses > 0);

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 28; i++)
                {
                    await session.StoreAsync(new Query.Order
                    {
                        Id = $"orders/{i + 31}",
                        OrderedAt = baseline.AddMonths(1).AddDays(i),
                        ShipVia = $"shippers/{i + 31}",
                        Company = $"companies/{i + 31}"
                    });
                }

                await session.SaveChangesAsync();
            }

            waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
            Assert.True(WaitHandle.WaitAll(waitHandles, timeout), await Etl.GetEtlDebugInfo(store.Database, timeout, RavenDatabaseMode.Sharded));

            files = Directory.GetFiles(path, "*.*", SearchOption.AllDirectories);
            Assert.True(files.Length > count);
        }

        private async Task<string> AddDebugInfo(Exception ex, RavenServer node)
        {
            var sb = new StringBuilder()
                .AppendLine($"Failed. Expected NodeIsPassiveException but {(ex == null ? "none" : ex.GetType())} was thrown.")
                .AppendLine("Cluster debug logs:");
            await GetClusterDebugLogsAsync(sb);

            sb.AppendLine().AppendLine($"Debug logs for removed node '{node.ServerStore.NodeTag}':");
            GetDebugLogsForNode(node, sb);

            return sb.ToString();
        }

    }
}
