using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class PullReplicationTests : ReplicationTestBase
    {
        public PullReplicationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanDefinePullReplication(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new PutPullReplicationAsHubOperation("test"));
            }
        }

        [Fact]
        public async Task PullReplicationShouldWork()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(name));
                using (var s2 = hub.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                await SetupPullReplicationAsync(name, sink, hub);

                var timeout = 3000;
                Assert.True(WaitForDocument(sink, "foo/bar", timeout), sink.Identifier);
            }
        }

        [Fact]
        public async Task PullReplicationShouldThrowForSharding()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var hub = Sharding.GetDocumentStore())
            {
               
                var exception = await Assert.ThrowsAnyAsync<NotSupportedInShardingException>(async () =>
                {
                    await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(name));
                });

                Assert.True(exception.Message.Contains("Update Pull Replication Definition Command is not supported in sharding"));
            }
        }

        [Fact]
        public async Task CollectPullReplicationOngoingTaskInfo()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                var hubTask = await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(name));
                using (var s2 = hub.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                var pullTasks = await SetupPullReplicationAsync(name, sink, hub);

                var timeout = 3000;
                Assert.True(WaitForDocument(sink, "foo/bar", timeout), sink.Identifier);

                var sinkResult = (OngoingTaskPullReplicationAsSink)await sink.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(pullTasks[0].TaskId, OngoingTaskType.PullReplicationAsSink));

                Assert.Equal(hub.Database, sinkResult.DestinationDatabase);
                Assert.Equal(hub.Urls[0], sinkResult.DestinationUrl);
                Assert.Equal(OngoingTaskConnectionStatus.Active, sinkResult.TaskConnectionStatus);

                var hubResult = await hub.Maintenance.SendAsync(new GetPullReplicationTasksInfoOperation(hubTask.TaskId));

                var ongoing = hubResult.OngoingTasks[0];
                Assert.Equal(sink.Database, ongoing.DestinationDatabase);
                Assert.Equal(sink.Urls[0], ongoing.DestinationUrl);
                Assert.Equal(OngoingTaskConnectionStatus.Active, ongoing.TaskConnectionStatus);
            }
        }

        [Fact]
        public async Task DeletePullReplicationFromHub()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                var hubResult = await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(name));
                using (var session = hub.OpenSession())
                {
                    session.Store(new User(), "foo/bar");
                    session.SaveChanges();
                }

                await SetupPullReplicationAsync(name, sink, hub);

                var timeout = 3000;
                Assert.True(WaitForDocument(sink, "foo/bar", timeout), sink.Identifier);

                await DeleteOngoingTask(hub, hubResult.TaskId, OngoingTaskType.PullReplicationAsHub);
                using (var session = hub.OpenSession())
                {
                    session.Store(new User(), "foo/bar2");
                    session.SaveChanges();
                }
                Assert.False(WaitForDocument(sink, "foo/bar2", timeout), sink.Identifier);
            }
        }

        [Fact]
        public async Task EnsureCantUseFilteredReplicationOnUnsecuredHub()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var hub = GetDocumentStore())
            {
                var error = await Assert.ThrowsAnyAsync<RavenException>(async () =>
                {
                    await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                    {
                        WithFiltering = true
                    }));
                });

                Assert.Contains("Server must be secured in order to use filtering in pull replication", error.Message);
            }
        }

        [Fact]
        public async Task EnsureCantUseSinkToHubReplicationOnUnsecuredHub()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var hub = GetDocumentStore())
            {
                var error = await Assert.ThrowsAnyAsync<RavenException>(async () =>
                {
                    await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                    {
                        Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink
                    }));
                });

                Assert.Contains($"Server must be secured in order to use Mode {nameof(PullReplicationMode.SinkToHub)} in pull replication {name}", error.Message);
            }
        }
        
        [Fact]
        public async Task DeletePullReplicationFromSink()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(name));
                using (var session = hub.OpenSession())
                {
                    session.Store(new User(), "foo/bar");
                    session.SaveChanges();
                }

                var sinkResult = await SetupPullReplicationAsync(name, sink, hub);

                var timeout = 3000;
                Assert.True(WaitForDocument(sink, "foo/bar", timeout), sink.Identifier);

                await DeleteOngoingTask(sink, sinkResult[0].TaskId, OngoingTaskType.PullReplicationAsSink);
                using (var session = hub.OpenSession())
                {
                    session.Store(new User(), "foo/bar2");
                    session.SaveChanges();
                }
                Assert.False(WaitForDocument(sink, "foo/bar2", timeout), sink.Identifier);
            }
        }

        [Fact]
        public async Task UpdatePullReplicationOnSink()
        {
            var definitionName1 = $"pull-replication {GetDatabaseName()}";
            var definitionName2 = $"pull-replication {GetDatabaseName()}";
            var timeout = 3000;

            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            using (var hub2 = GetDocumentStore())
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(definitionName1));
                await hub2.Maintenance.ForDatabase(hub2.Database).SendAsync(new PutPullReplicationAsHubOperation(definitionName2));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub1/1");
                    main.SaveChanges();
                }
                var pullTasks = await SetupPullReplicationAsync(definitionName1, sink, hub);
                Assert.True(WaitForDocument(sink, "hub1/1", timeout), sink.Identifier);

                var pull = new PullReplicationAsSink(hub2.Database, $"ConnectionString2-{sink.Database}", definitionName2)
                {
                    Url = sink.Urls[0],
                    TaskId = pullTasks[0].TaskId
                };
                await AddWatcherToReplicationTopology(sink, pull, hub2.Urls);

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub1/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(sink, "hub1/2", timeout), sink.Identifier);

                using (var main = hub2.OpenSession())
                {
                    main.Store(new User(), "hub2");
                    main.SaveChanges();
                }
                Assert.True(WaitForDocument(sink, "hub2", timeout), sink.Identifier);
            }
        }

        [Fact]
        public async Task UpdatePullReplicationOnHub()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var definitionName = $"pull-replication {GetDatabaseName()}";
            var timeout = 3_000;

            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                var saveResult = await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(definitionName));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "users/1");
                    main.SaveChanges();
                }

                await SetupPullReplicationAsync(definitionName, sink, hub);
                Assert.True(WaitForDocument(sink, "users/1", timeout), sink.Identifier);

                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(definitionName)
                {
                    DelayReplicationFor = TimeSpan.FromDays(1),
                    TaskId = saveResult.TaskId
                }));
                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "users/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(sink, "users/2", timeout), sink.Identifier);
                var res= await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(definitionName)
                {
                    TaskId = saveResult.TaskId
                }));
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    await hub.GetRequestExecutor().ExecuteAsync(new WaitForRaftIndexCommand(res.RaftCommandIndex), context);
                }
                var hubResult = await hub.Maintenance.SendAsync(new GetPullReplicationTasksInfoOperation(saveResult.TaskId));
                Assert.Equal(hubResult.Definition.Name, definitionName);
                Assert.Equal(hubResult.Definition.DelayReplicationFor, new TimeSpan());
                Assert.Equal(hubResult.Definition.Disabled, false);

                Assert.True(WaitForDocument(sink, "users/2", timeout * 2));
            }
        }

        [Fact]
        public async Task DisablePullReplicationOnSink()
        {
            var definitionName = $"pull-replication {GetDatabaseName()}";
            var timeout = 10_000;

            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(definitionName));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub/1");
                    main.SaveChanges();
                }
                var pullTasks = await SetupPullReplicationAsync(definitionName, sink, hub);
                Assert.True(WaitForDocument(sink, "hub/1", timeout), sink.Identifier);

                var pull = new PullReplicationAsSink(hub.Database, $"ConnectionString-{sink.Database}", definitionName)
                {
                    Url = sink.Urls[0],
                    Disabled = true,
                    TaskId = pullTasks[0].TaskId
                };
                await AddWatcherToReplicationTopology(sink, pull, hub.Urls);

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(sink, "hub/2", timeout), sink.Identifier);

                pull.Disabled = false;
                await AddWatcherToReplicationTopology(sink, pull, hub.Urls);

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub/3");
                    main.SaveChanges();
                }
                Assert.True(WaitForDocument(sink, "hub/2", timeout), sink.Identifier);
                Assert.True(WaitForDocument(sink, "hub/3", timeout), sink.Identifier);
            }
        }

        [Fact]
        public async Task DisablePullReplicationOnHub()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var definitionName = $"pull-replication {GetDatabaseName()}";
            var timeout = 10_000;

            using (var sink = GetDocumentStore())
            using (var hub = GetDocumentStore())
            {
                var pullDefinition = new PullReplicationDefinition(definitionName);
                var saveResult = await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(pullDefinition));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "users/1");
                    main.SaveChanges();
                }
                await SetupPullReplicationAsync(definitionName, sink, hub);
                Assert.True(WaitForDocument(sink, "users/1", timeout), sink.Identifier);

                var db = await Databases.GetDocumentDatabaseInstanceFor(sink);
                var removedOnSink = new AsyncManualResetEvent();
                db.ReplicationLoader.IncomingReplicationRemoved += _ => removedOnSink.Set();

                pullDefinition.Disabled = true;
                pullDefinition.TaskId = saveResult.TaskId;
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(pullDefinition));

                Assert.True(await removedOnSink.WaitAsync(TimeSpan.FromMilliseconds(timeout)));

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "users/2");
                    main.SaveChanges();
                }
                Assert.False(WaitForDocument(sink, "users/2", timeout), sink.Identifier);

                pullDefinition.Disabled = false;
                pullDefinition.TaskId = saveResult.TaskId;
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(pullDefinition));

                Assert.True(WaitForDocument(sink, "users/2", timeout), sink.Identifier);
            }
        }

        [Fact]
        public async Task MultiplePullExternalReplicationShouldWork()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var hub = GetDocumentStore())
            using (var sink1 = GetDocumentStore())
            using (var sink2 = GetDocumentStore())
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(name));
                using (var session = hub.OpenSession())
                {
                    session.Store(new User(), "foo/bar");
                    session.SaveChanges();
                }

                await SetupPullReplicationAsync(name, sink1, hub);
                await SetupPullReplicationAsync(name, sink2, hub);

                var timeout = 3000;
                Assert.True(WaitForDocument(sink1, "foo/bar", timeout), sink1.Identifier);
                Assert.True(WaitForDocument(sink2, "foo/bar", timeout), sink2.Identifier);
            }
        }

        [Fact]
        public async Task FailoverOnHubNodeFail()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var clusterSize = 3;
            var (_, hub) = await CreateRaftCluster(clusterSize);
            var (minionNodes, minion) = await CreateRaftCluster(clusterSize);

            var hubDB = GetDatabaseName();
            var minionDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(minionDB, clusterSize, minion.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(hubDB, clusterSize, hub.WebUrl);

            using (var hubStore = new DocumentStore
            {
                Urls = new[] { hub.WebUrl },
                Database = hubDB
            }.Initialize())
            using (var minionStore = new DocumentStore
            {
                Urls = new[] { minion.WebUrl },
                Database = minionDB
            }.Initialize())
            {
                using (var session = hubStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var name = $"pull-replication {GetDatabaseName()}";
                await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(new PutPullReplicationAsHubOperation(name));

                // add pull replication with invalid discovery url to test the failover on database topology discovery
                var pullReplication = new PullReplicationAsSink(hubDB, $"ConnectionString-{hubDB}", name)
                {
                    MentorNode = "B", // this is the node were the data will be replicated to.
                };
                var urls = new List<string>();
                foreach (var ravenServer in srcTopology.Servers)
                {
                    urls.Add(ravenServer.WebUrl);
                }
                await AddWatcherToReplicationTopology((DocumentStore)minionStore, pullReplication, urls.ToArray());

                using (var dstSession = minionStore.OpenSession())
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        minionNodes,
                        minionDB,
                        "users/1",
                        u => u.Name.Equals("Karmel"),
                        TimeSpan.FromSeconds(30)));
                }

                var minionUrl = minion.ServerStore.GetClusterTopology().GetUrlFromTag("B");
                var server = Servers.Single(s => s.WebUrl == minionUrl);
                using (var processor = await Databases.InstantiateOutgoingTaskProcessor(minionDB, server))
                {
                    Assert.True(WaitForValue(
                        () => ((OngoingTaskPullReplicationAsSink)processor.GetOngoingTasksInternal().OngoingTasks.Single(t => t is OngoingTaskPullReplicationAsSink)).DestinationUrl !=
                              null,
                        true));

                    var watcherTaskUrl = ((OngoingTaskPullReplicationAsSink)processor.GetOngoingTasksInternal().OngoingTasks.Single(t => t is OngoingTaskPullReplicationAsSink)).DestinationUrl;
                    // dispose the hub node, from which we are currently pulling
                    await DisposeServerAndWaitForFinishOfDisposalAsync(Servers.Single(s => s.WebUrl == watcherTaskUrl));
                }
               
                using (var session = hubStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 2);
                    session.Store(new User
                    {
                        Name = "Karmel2"
                    }, "users/2");
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(minionStore);

                using (var dstSession = minionStore.OpenSession())
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        minionNodes,
                        minionDB,
                        "users/2",
                        u => u.Name.Equals("Karmel2"),
                        TimeSpan.FromSeconds(30)));
                }
            }
        }

        [Fact]
        public async Task RavenDB_15855()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 3;
            var (_, hub) = await CreateRaftCluster(clusterSize);
            var (minionNodes, minion) = await CreateRaftCluster(clusterSize);

            var hubDB = GetDatabaseName();
            var minionDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(minionDB, clusterSize, minion.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(hubDB, clusterSize, hub.WebUrl);

            using (var hubStore = new DocumentStore
            {
                Urls = new[] { hub.WebUrl },
                Database = hubDB
            }.Initialize())
            using (var minionStore = new DocumentStore
            {
                Urls = new[] { minion.WebUrl },
                Database = minionDB
            }.Initialize())
            {
                using (var session = hubStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var name = $"pull-replication {GetDatabaseName()}";
                await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    MentorNode = "A"
                }));

                var pullReplication = new PullReplicationAsSink(hubDB, $"ConnectionString-{hubDB}", name)
                {
                    MentorNode = "B", // this is the node were the data will be replicated to.
                };
                await AddWatcherToReplicationTopology((DocumentStore)minionStore, pullReplication, new[] { hub.WebUrl });

                using (var dstSession = minionStore.OpenSession())
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        minionNodes,
                        minionDB,
                        "users/1",
                        u => u.Name.Equals("Karmel"),
                        TimeSpan.FromSeconds(30)));
                }

                var minionUrl = minion.ServerStore.GetClusterTopology().GetUrlFromTag("B");
                var minionServer = Servers.Single(s => s.WebUrl == minionUrl);

                using (var processor = await Databases.InstantiateOutgoingTaskProcessor(minionDB, minionServer))
                {
                    Assert.True(WaitForValue(
                        () => ((OngoingTaskPullReplicationAsSink)processor.GetOngoingTasksInternal().OngoingTasks.Single(t => t is OngoingTaskPullReplicationAsSink)).DestinationUrl != null,
                        true));
                }
               
                var mentorUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
                var mentor = Servers.Single(s => s.WebUrl == mentorUrl);
                var mentorDatabase = await mentor.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(hubDB);

                var connections = await WaitForValueAsync(() => mentorDatabase.ReplicationLoader.OutgoingConnections.Count(), 3);
                Assert.Equal(3, connections);

                minionServer.CpuCreditsBalance.BackgroundTasksAlertRaised.Raise();

                Assert.Equal(1,
                    await WaitForValueAsync(async () => (await minionStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(minionDB))).Topology.Rehabs.Count,
                        1));

                await EnsureReplicatingAsync((DocumentStore)hubStore, (DocumentStore)minionStore);

                connections = await WaitForValueAsync(() => mentorDatabase.ReplicationLoader.OutgoingConnections.Count(), 3);
                Assert.Equal(3, connections);
            }
        }

        [Fact]
        public async Task RavenDB_17124()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var hubServer = GetNewServer(new ServerCreationOptions() { NodeTag = "A" }))
            using (var sinkServer1 = GetNewServer(new ServerCreationOptions() { NodeTag = "B" }))
            using (var sinkServer2 = GetNewServer(new ServerCreationOptions() { NodeTag = "C" }))
            using (var hub = GetDocumentStore(new Options() { Server = hubServer }))
            using (var sink1 = GetDocumentStore(new Options() { Server = sinkServer1 }))
            using (var sink2 = GetDocumentStore(new Options() { Server = sinkServer2 }))
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(name));
                using (var session = hub.OpenSession())
                {
                    session.Store(new User(), "foo/bar");
                    session.SaveChanges();
                }

                await SetupPullReplicationAsync(name, sink1, hub);
                await SetupPullReplicationAsync(name, sink2, hub);

                using (var processor = await Databases.InstantiateOutgoingTaskProcessor(hub.Database, hubServer))
                {
                    await AssertWaitForTrueAsync(() => Task.FromResult(processor.GetOngoingTasksInternal().OngoingTasks.Exists(x =>
                        x is OngoingTaskPullReplicationAsHub t && t.DestinationDatabase.Equals(sink1.Database, StringComparison.OrdinalIgnoreCase) &&
                        t.DestinationUrl == sink1.Urls.FirstOrDefault())));
                    await AssertWaitForTrueAsync(() => Task.FromResult(processor.GetOngoingTasksInternal().OngoingTasks.Exists(x =>
                        x is OngoingTaskPullReplicationAsHub t && t.DestinationDatabase.Equals(sink2.Database, StringComparison.OrdinalIgnoreCase) &&
                        t.DestinationUrl == sink2.Urls.FirstOrDefault())));
                }
            }
        }

        [Fact]
        public async Task FailoverOnSinkNodeFail()
        {
            var clusterSize = 3;
            var (_, hub) = await CreateRaftCluster(clusterSize);
            var (minionNodes, minion) = await CreateRaftCluster(clusterSize);

            var hubDB = GetDatabaseName();
            var minionDB = GetDatabaseName();

            var dstTopology = await CreateDatabaseInCluster(minionDB, clusterSize, minion.WebUrl);
            var srcTopology = await CreateDatabaseInCluster(hubDB, clusterSize, hub.WebUrl);

            using (var hubStore = new DocumentStore
            {
                Urls = new[] { hub.WebUrl },
                Database = hubDB
            }.Initialize())
            using (var minionStore = new DocumentStore
            {
                Urls = new[] { minion.WebUrl },
                Database = minionDB
            }.Initialize())
            {
                using (var session = hubStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 1);
                    session.Store(new User
                    {
                        Name = "Karmel"
                    }, "users/1");
                    session.SaveChanges();
                }

                var name = $"pull-replication {GetDatabaseName()}";
                await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(new PutPullReplicationAsHubOperation(name));

                // add pull replication with invalid discovery url to test the failover on database topology discovery
                var pullReplication = new PullReplicationAsSink(hubDB, $"ConnectionString-{hubDB}", name)
                {
                    MentorNode = "B", // this is the node were the data will be replicated to.
                };
                await AddWatcherToReplicationTopology((DocumentStore)minionStore, pullReplication, new[] { "http://127.0.0.1:1234", hub.WebUrl });

                using (var dstSession = minionStore.OpenSession())
                {
                    Assert.True(await WaitForDocumentInClusterAsync<User>(
                        minionNodes,
                        minionDB,
                        "users/1",
                        u => u.Name.Equals("Karmel"),
                        TimeSpan.FromSeconds(30)));
                }

                var minionUrl = minion.ServerStore.GetClusterTopology().GetUrlFromTag("B");
                var server = Servers.Single(s => s.WebUrl == minionUrl);

                using (var processor = await Databases.InstantiateOutgoingTaskProcessor(minionDB, server))
                {
                    Assert.True(WaitForValue(
                        () => ((OngoingTaskPullReplicationAsSink)processor.GetOngoingTasksInternal().OngoingTasks.Single(t => t is OngoingTaskPullReplicationAsSink)).DestinationUrl != null,
                        true));
                }
               
                // dispose the minion node.
                await DisposeServerAndWaitForFinishOfDisposalAsync(server);

                using (var session = hubStore.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), replicas: clusterSize - 2);
                    session.Store(new User
                    {
                        Name = "Karmel2"
                    }, "users/2");
                    session.SaveChanges();
                }

                var user = WaitForDocumentToReplicate<User>(
                    minionStore,
                    "users/2",
                    30_000);

                Assert.Equal("Karmel2", user.Name);
            }
        }

        //TODO write test for deletion! - make sure replication is stopped after we delete hub!

        public static Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(string remoteName, DocumentStore sink, params DocumentStore[] hub)
        {
            return SetupPullReplicationAsync(remoteName, sink, null, hub);
        }

        public static async Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(string remoteName, DocumentStore sink, X509Certificate2 certificate, params DocumentStore[] hub)
        {
            var tasks = new List<Task<ModifyOngoingTaskResult>>();
            var resList = new List<ModifyOngoingTaskResult>();
            foreach (var store in hub)
            {
                var pull = new PullReplicationAsSink(store.Database, $"ConnectionString-{store.Database}", remoteName) { Url = sink.Urls[0] };
                if (certificate != null)
                {
                    pull.CertificateWithPrivateKey = Convert.ToBase64String(certificate.Export(X509ContentType.Pfx));
                }
                tasks.Add(AddWatcherToReplicationTopology(sink, pull, store.Urls));
            }
            await Task.WhenAll(tasks);
            foreach (var task in tasks)
            {
                resList.Add(await task);
            }
            return resList;
        }
    }
}
