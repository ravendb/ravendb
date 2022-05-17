using Tests.Infrastructure;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
using Raven.Server;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.ETL.Olap;
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

        [Fact(Skip = "Sharded PutClientConfigurationOperation not implemented")]
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
                var databases = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == myTag)
                    .ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(srcDb);

                var etlDone = new ManualResetEventSlim();
                foreach (var task in databases)
                {
                    var db = await task;
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

        [Fact(Skip = "Sharded failover on DisposeServerAndWaitForFinishOfDisposal is not implemented")]
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

                var dbRecord = src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database)).Result;
                var shardedCtx = new ShardedDatabaseContext(srcNodes.Servers[0].ServerStore, dbRecord);
                var shardNumber = 0;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    shardNumber = shardedCtx.GetShardNumber(context, id);
                }

                var ongoingTask = src.Maintenance.Send(new GetOngoingTaskInfoOperation($"{name}${shardNumber}", OngoingTaskType.RavenEtl));

                var responsibleNodeNodeTag = ongoingTask.ResponsibleNode.NodeTag;

                Assert.NotNull(responsibleNodeNodeTag);

                var originalTaskNodeServer = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == responsibleNodeNodeTag);

                var originalResult = DisposeServerAndWaitForFinishOfDisposal(originalTaskNodeServer);

                id = "users/5";
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    Assert.Equal(shardNumber, shardedCtx.GetShardNumber(context, id));
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

                Assert.True(Replication.WaitForCounterReplication(new List<IDocumentStore>
                {
                    dest
                }, "users/1", "likes", 2, TimeSpan.FromSeconds(60)));
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task OlapTaskShouldBeHighlyAvailable()
        {
            var nodes = await CreateRaftCluster(3, watcherCluster: true);
            var leader = nodes.Leader;
            var dbName = GetDatabaseName();
            var srcNodes = await ShardingCluster.CreateShardedDatabaseInCluster(dbName, replicationFactor: 2, nodes, shards: 3);

            var stores = srcNodes.Servers.Select(s => new DocumentStore
            {
                Database = dbName,
                Urls = new[] { s.WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
                .ToList();

            var server = srcNodes.Servers.First(s => s != leader);
            var store = stores.Single(s => s.Urls[0] == server.WebUrl);

            Assert.Equal(store.Database, dbName);

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
                },
                MentorNode = server.ServerStore.NodeTag
            };

            var result = store.Maintenance.Send(new PutConnectionStringOperation<OlapConnectionString>(new OlapConnectionString
            {
                Name = connectionStringName,
                LocalSettings = new LocalSettings
                {
                    FolderPath = path
                }
            }));
            Assert.NotNull(result.RaftCommandIndex);

            store.Maintenance.Send(new AddEtlOperation<OlapConnectionString>(configuration));
            var databases = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == server.ServerStore.NodeTag)
                .ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(dbName);

            var etlsDone = WaitForEtlOnAllShards(server, dbName, (n, s) => s.LoadSuccesses > 0);
            var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
            WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(2));

            var files = Directory.GetFiles(path, "*.*", SearchOption.AllDirectories);
            var count = files.Length;
            Assert.True(count > 0);

            await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(server.ServerStore.NodeTag), nodes.Nodes);

            var store2 = stores.First(s => s != store);
            using (var session = store2.OpenAsyncSession())
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

            etlsDone = WaitForEtlOnAllShards(leader, dbName, (n, s) => s.LoadSuccesses > 0);
            waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
            WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));

            files = Directory.GetFiles(path, "*.*", SearchOption.AllDirectories);
            Assert.True(files.Length > count);
        }

        private static IEnumerable<ManualResetEventSlim> WaitForEtlOnAllShards(RavenServer server, string database, Func<string, EtlProcessStatistics, bool> predicate)
        {
            var dbs = server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database).ToList();
            var list = new List<ManualResetEventSlim>(dbs.Count);
            foreach (var task in dbs)
            {
                var mre = new ManualResetEventSlim();
                list.Add(mre);

                var db = task.Result;
                db.EtlLoader.BatchCompleted += x =>
                {
                    if (predicate($"{x.ConfigurationName}/{x.TransformationName}", x.Statistics))
                        mre.Set();
                };
            }

            return list;
        }
    }
}
