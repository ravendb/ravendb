using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using static Raven.Server.Web.System.DatabasesDebugHandler;

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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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
        
        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Logging)]
        public async Task UpdatePullReplicationOnSink()
        {
            var definitionName1 = $"pull-replication {GetDatabaseName()}";
            var definitionName2 = $"pull-replication {GetDatabaseName()}";
            var timeout = 3000;
            var auditLogPath = NewDataPath(suffix: "AuditLog");
            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Security.AuditLogPath)] = auditLogPath
            };
            var certificates = Certificates.SetupServerAuthentication(settings);
            var serverCertificate = certificates.ServerCertificateForCommunication.Value;
            var pullReplicationCertificate = certificates.ClientCertificate1.Value;

            using (var sink = GetDocumentStore(SecuredOptions()))
            using (var hub = GetDocumentStore(SecuredOptions()))
            using (var hub2 = GetDocumentStore(SecuredOptions()))
            {
                await DefineHubAndRegisterAccess(hub, definitionName1);
                await DefineHubAndRegisterAccess(hub2, definitionName2);

                using (var main = hub.OpenSession())
                {
                    main.Store(new User(), "hub1/1");
                    main.SaveChanges();
                }
                var pullTasks = await SetupPullReplicationAsync(definitionName1, sink, pullReplicationCertificate, hub);
                Assert.True(WaitForDocument(sink, "hub1/1", timeout), sink.Identifier);

                var pull = new PullReplicationAsSink(hub2.Database, $"ConnectionString2-{sink.Database}", definitionName2)
                {
                    Url = sink.Urls[0],
                    TaskId = pullTasks[0].TaskId,
                    CertificateWithPrivateKey = Convert.ToBase64String(pullReplicationCertificate.Export(X509ContentType.Pfx))
                };
                await AddWatcherToReplicationTopology(sink, pull, hub2.Urls);
                await WaitForAssertionAsync(() =>
                {
                    var auditLog = ReadAuditLog();
                    var updateSinkAuditLines = auditLog
                        .Split(Environment.NewLine)
                        .Where(x => x.Contains("update-sink-pull-replication"));

                    Assert.Contains(updateSinkAuditLines, x => x.Contains(definitionName2));
                    return Task.CompletedTask;
                }, TimeSpan.FromSeconds(5));

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

            return;

            Options SecuredOptions()
            {
                return new Options
                {
                    ClientCertificate = serverCertificate,
                    AdminCertificate = serverCertificate
                };
            }

            async Task DefineHubAndRegisterAccess(DocumentStore hub, string definitionName)
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(definitionName));
                await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(definitionName,
                    new ReplicationHubAccess
                    {
                        Name = definitionName,
                        CertificateBase64 = Convert.ToBase64String(pullReplicationCertificate.Export(X509ContentType.Cert))
                    }));
            }

            string ReadAuditLog()
            {
                NLog.LogManager.Flush(TimeSpan.FromSeconds(1));

                return Directory.Exists(auditLogPath)
                    ? string.Join(Environment.NewLine, Directory.GetFiles(auditLogPath, "*.log").Select(ReadAuditLogFile))
                    : string.Empty;
            }

            static string ReadAuditLogFile(string path)
            {
                using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                using var reader = new StreamReader(stream);

                return reader.ReadToEnd();
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [RavenFact(RavenTestCategory.Replication)]
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

        [NightlyBuildFact]
        public async Task PullReplicationAsSinkToHubWithIdleShouldWork()
        {
            var name = $"pull-replication {GetDatabaseName()}";
            using (var hubServer = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Replication.RetryMaxTimeout)] = "1",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
                }
            }))
            using (var sinkServer = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                    [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                    [RavenConfiguration.GetKey(x => x.Replication.RetryMaxTimeout)] = "1",
                    [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
                }
            }))
            using (var sink = GetDocumentStore(new Options
            {
                Server = sinkServer,
                ModifyDatabaseName = s => $"Sink_{s}",
                RunInMemory = false,

            }))
            using (var hub = GetDocumentStore(new Options
            {
                Server = hubServer,
                ModifyDatabaseName = s => $"Hub_{s}",
                RunInMemory = false,

            }))
            {
                sinkServer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;
                hubServer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;

                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(name));
                using (var s2 = hub.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                await SetupPullReplicationAsync(name, sink, hub);
                var timeout = 3000;
                Assert.True(WaitForDocument(sink, "foo/bar", timeout), sink.Identifier);

                var now = DateTime.Now;
                var nextNow = now + TimeSpan.FromSeconds(60);

                var statistics = new IdleDatabaseStatistics
                {
                    Name = hub.Database.ToString()
                };

                while (now < nextNow && hubServer.ServerStore.IdleDatabases.Count < 1)
                {
                    await Task.Delay(1000);
                    var hubDb = hubServer.ServerStore.DatabasesLandlord.LastRecentlyUsed.FirstOrDefault();
                    hubServer.ServerStore.CanUnloadDatabase(hubDb.Key, hubDb.Value, statistics, out _);

                    now = DateTime.Now;
                }

                Assert.True(1 == hubServer.ServerStore.IdleDatabases.Count, string.Join(Environment.NewLine, statistics.Explanations));
                Assert.Equal(0, sinkServer.ServerStore.IdleDatabases.Count);

                var sinkDb = await GetDatabase(sinkServer, sink.Database);

                await WaitAndAssertForValueAsync(() =>
                {
                    if (sinkDb.ReplicationLoader.OutgoingFailureInfo.Count == 0)
                        return false;

                    var outgoingFailureInfos = sinkDb.ReplicationLoader.OutgoingFailureInfo.Values.ToList();

                    if (outgoingFailureInfos.Any(x => x.Errors.Count > 0) == false)
                        return false;

                    foreach (var failureInfo in outgoingFailureInfos)
                    {
                        foreach (var error in failureInfo.Errors)
                        {
                            if (error is not DatabaseIdleException idleException)
                                continue;

                            if (idleException.Message.Contains($"Raven.Client.Exceptions.Database.DatabaseIdleException: Cannot GetRemoteTaskTopology for PullReplicationAsSink connection because database '{hub.Database}' currently is idle."))
                                return true;
                        }
                    }

                    return false;
                }, true, 30_000, 322);

                using (var s2 = hub.OpenSession())
                {
                    s2.Store(new User() { Name = "EGOR" }, "foo/bar/322");
                    s2.SaveChanges();
                }

                Assert.Equal(0, WaitForValue(() => sinkServer.ServerStore.IdleDatabases.Count, 0, 60_000, 333));
                Assert.True(WaitForDocument(sink, "foo/bar/322", timeout * 5), sink.Identifier);
            }
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task PullReplicationAsHubToSinkWithIdleShouldWork()
        {
            var name = $"pull-replication {GetDatabaseName()}";

            var hubSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "10",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "3",
                [RavenConfiguration.GetKey(x => x.Replication.RetryMaxTimeout)] = "1",
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
            };
            var certificates = Certificates.SetupServerAuthentication(customSettings: hubSettings);
            using (var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = hubSettings
            }))
            using (var sink = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDatabaseName = s => $"Sink_{s}",
                RunInMemory = false,
                ClientCertificate = certificates.ServerCertificateForCommunication.Value,
                AdminCertificate = certificates.ServerCertificateForCommunication.Value
            }))
            using (var hub = GetDocumentStore(new Options
            {
                Server = server,
                ModifyDatabaseName = s => $"Hub_{s}",
                RunInMemory = false,
                ClientCertificate = certificates.ServerCertificateForCommunication.Value,
                AdminCertificate = certificates.ServerCertificateForCommunication.Value
            }))
            {
                await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Name = name,
                    Mode = PullReplicationMode.SinkToHub | PullReplicationMode.HubToSink
                }));

                await hub.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                    new ReplicationHubAccess
                    {
                        Name = name,
                        CertificateBase64 = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Cert)),
                    }));

                var conStrName = "PullReplicationAsSink";
                await sink.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Database = hub.Database,
                    Name = conStrName,
                    TopologyDiscoveryUrls = hub.Urls
                }));
                await sink.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                {
                    ConnectionStringName = conStrName,
                    CertificateWithPrivateKey = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Pfx)),
                    HubName = name,
                    Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub
                }));

                using (var s2 = hub.OpenSession())
                {
                    s2.Store(new User(), "foo/bar");
                    s2.SaveChanges();
                }

                var timeout = 3000;
                Assert.True(WaitForDocument(sink, "foo/bar", timeout * 5), sink.Identifier);

                using (var s2 = sink.OpenSession())
                {
                    s2.Store(new User(), "foo/bar/228");
                    s2.SaveChanges();
                }

                Assert.True(WaitForDocument(hub, "foo/bar/228", timeout * 5), hub.Identifier);

                server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().SkipIncreasingLastWorkTimeBasedOnDatabaseSize = true;

                var dic = new Dictionary<IdleDatabaseStatistics, int>();
                Assert.True(WaitForValue( () =>
                {
                    dic = new Dictionary<IdleDatabaseStatistics, int>();
                    foreach (var databaseKvp in server.ServerStore.DatabasesLandlord.LastRecentlyUsed.ForceEnumerateInThreadSafeManner())
                    {
                        var statistics = new IdleDatabaseStatistics
                        {
                            Name = databaseKvp.Key.ToString()
                        };

                        server.ServerStore.CanUnloadDatabase(databaseKvp.Key, databaseKvp.Value, statistics, out _);

                        if (statistics.CanUnload == false)
                            continue;

                        if (statistics.Explanations.Count > 1)
                        {
                            continue;
                        }

                        if (statistics.NumberOfActivePullReplicationAsSinkConnections == 0)
                            continue;

                        dic.Add(statistics, statistics.NumberOfActivePullReplicationAsSinkConnections);
                    }

                    if (dic.Count != 2)
                        return false;

                    return dic.All(x => x.Value == 1);
                }, true, 75_000, 1000), string.Join(Environment.NewLine, dic.Keys.Select(x =>
                {
                    using (var context = JsonOperationContext.ShortTermSingleUse())
                    {
                        return context.ReadObject(x.ToJson(), "json").ToString();
                    }
                })));

                // the hub & sink should be online  
                Assert.Equal(0, server.ServerStore.IdleDatabases.Count);
                Assert.All(dic.Keys, x => Assert.Contains($"Cannot unload database because number of active PullReplication as Sink Connections (1) is greater than 0", x.Explanations));

                using (var s2 = hub.OpenSession())
                {
                    s2.Store(new User(), "foo/bar/123");
                    s2.SaveChanges();
                }

                Assert.True(WaitForDocument(sink, "foo/bar/123", timeout * 5), sink.Identifier);

                using (var s2 = sink.OpenSession())
                {
                    s2.Store(new User(), "foo/bar/322");
                    s2.SaveChanges();
                }

                Assert.True(WaitForDocument(hub, "foo/bar/322", timeout * 5), hub.Identifier);
            }
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [InlineData(SecurityClearance.ClusterAdmin)]
        [InlineData(SecurityClearance.Operator)]
        [InlineData(SecurityClearance.ValidUser)]
        public async Task SinkToHubWithThisClearanceShouldWork(SecurityClearance clearance)
        {
            var settings = new Dictionary<string, string>();
            var certificates = Certificates.SetupServerAuthentication(settings);

            using (var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = settings }))
            using (var sinkServer = GetNewServer(new ServerCreationOptions { CustomSettings = settings }))
            {
                Certificates.RegisterClientCertificate(
                    certificates.ServerCertificate.Value,
                    certificates.ClientCertificate1.Value,
                    new Dictionary<string, DatabaseAccess>(),
                    SecurityClearance.ClusterAdmin,
                    server: hubServer);

                using (var hubStore = GetDocumentStore(new Options
                {
                    Server = hubServer,
                    ModifyDatabaseName = s => $"HubDB_{s}",
                    ClientCertificate = certificates.ServerCertificate.Value,
                    AdminCertificate = certificates.ClientCertificate1.Value,
                }))
                {
                    Certificates.RegisterClientCertificate(
                        certificates.ServerCertificate.Value,
                        certificates.ClientCertificate1.Value,
                        new Dictionary<string, DatabaseAccess>(),
                        SecurityClearance.ClusterAdmin,
                        server: sinkServer);

                    using (var sinkStore = GetDocumentStore(new Options
                    {
                        Server = sinkServer,
                        CreateDatabase = true,
                        ModifyDatabaseName = s => $"SinkDB_{s}",
                        ClientCertificate = certificates.ServerCertificate.Value,
                        AdminCertificate = certificates.ClientCertificate1.Value,
                    }))
                    {
                        Dictionary<string, DatabaseAccess> permissions = clearance == SecurityClearance.ValidUser
                            ? new()
                            {
                                [hubStore.Database] = DatabaseAccess.ReadWrite
                            }
                            : new();

                        // Registering certificate with ClusterAdmin permissions
                        Certificates.RegisterClientCertificate(
                            certificates.ServerCertificate.Value, 
                            certificates.ClientCertificate2.Value, 
                            permissions: permissions,
                            clearance,
                            server: hubServer);

                        await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition("pull-replication-task")
                        {
                            Mode = PullReplicationMode.SinkToHub
                        }));

                        await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("pull-replication-task", new ReplicationHubAccess
                        {
                            Name = "SinkUser",
                            CertificateBase64 = Convert.ToBase64String(certificates.ClientCertificate2.Value.Export(X509ContentType.Cert))
                        }));

                        const string connectionStringName = "ConnectToHub";
                        await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                        {
                            Name = connectionStringName,
                            Database = hubStore.Database,
                            TopologyDiscoveryUrls = hubStore.Urls
                        }));

                        await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
                        {
                            ConnectionStringName = connectionStringName,
                            HubName = "pull-replication-task",
                            Mode = PullReplicationMode.SinkToHub,
                            CertificateWithPrivateKey = Convert.ToBase64String(certificates.ClientCertificate2.Value.Export(X509ContentType.Pfx))
                        }));

                        // Add a document to sink and wait for it to replicate to hub
                        using (var session = sinkStore.OpenSession())
                        {
                            session.Store(new User { Name = "Test User" }, "users/1");
                            session.SaveChanges();
                        }

                        // Wait for the document to replicate to hub
                        var timeout = 10000;
                        Assert.True(WaitForDocument(hubStore, "users/1", timeout), hubStore.Identifier);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Certificates)]
        public async Task ShouldRejectPullReplicationSinkCertificateWithoutPrivateKey()
        {
            var customSettings = new Dictionary<string, string>();
            var certificates = Certificates.SetupServerAuthentication(customSettings: customSettings);
            using var server = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = customSettings
            });

            using var sinkStore = GetDocumentStore(new Options
            {
                Server = server,
                ClientCertificate = certificates.ServerCertificateForCommunication.Value,
                AdminCertificate = certificates.ServerCertificateForCommunication.Value
            });

            // Export certificate as public-only (no private key)
            var publicOnlyCert = Convert.ToBase64String(certificates.ClientCertificate1.Value.Export(X509ContentType.Cert));

            var pull = new PullReplicationAsSink
            {
                ConnectionStringName = "dummy",
                HubName = "hub",
                CertificateWithPrivateKey = publicOnlyCert
            };

            // Set up a dummy connection string so the server doesn't fail on that
            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = sinkStore.Database,
                Name = "dummy",
                TopologyDiscoveryUrls = sinkStore.Urls
            }));

            // Skip client-side validation to test the server-side validation in isolation
            var exception = await Assert.ThrowsAsync<RavenException>(async () =>
            {
                await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pull, skipClientCertificateValidation: true));
            });

            Assert.Contains("private key", exception.Message);
        }

        //TODO write test for deletion! - make sure replication is stopped after we delete hub!

        public static Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(string remoteName, DocumentStore sink, params DocumentStore[] hub)
        {
            return SetupPullReplicationAsync(remoteName, sink, null, hub);
        }

        private static async Task<List<ModifyOngoingTaskResult>> SetupPullReplicationAsync(string remoteName, DocumentStore sink, X509Certificate2 certificate, params DocumentStore[] hub)
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

        // Reproduces the "zombie pull connection" deadlock on a busy sink.
        //
        // RavenDB_25412 proved that when the hub stops sending, the sink's read-timeout
        // (Replication.ActiveConnectionTimeout) fires, the hung connection is reaped, and the sink reconnects.
        //
        // This test adds the one ingredient that was present in the production incident but missing from
        // that test: OTHER incoming replication traffic on the same sink node. Every time any other incoming
        // connection receives a batch, ReplicationLoader.OnIncomingReceiveSucceeded calls
        // OnReplicationFromAnotherSource() on EVERY other incoming handler. That wakes the handler's read loop
        // (the "notify" branch), which (a) bumps LastHeartbeatTicks so the connection keeps looking "fresh",
        // and (b) makes InterruptibleRead.ParseToMemory return Interrupted and start a brand-new read-timeout
        // window. On a busy node these wake-ups arrive faster than the timeout, so the timeout never elapses:
        // the hung connection becomes an immortal zombie that is never reaped and never reconnects, and only a
        // task restart (which disposes it via DropIncomingConnections) clears it.
        //
        // EXPECTED: this test FAILS on the current code (reproduces the bug) and passes once the read-timeout
        // is made to track real activity instead of being reset by notify wake-ups.
        [RavenFact(RavenTestCategory.Replication)]
        public async Task BusySink_ShouldStillTimeoutAndReconnect_HangingPullConnection()
        {
            DoNotReuseServer();

            // keep the read-timeout short so the test is fast
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Replication.ActiveConnectionTimeout)] = "5"
            };

            using var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings });
            using var sinkServer = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings });

            using var hubStore = GetDocumentStore(new Options
            {
                Server = hubServer
            });

            using var sinkStore = GetDocumentStore(new Options
            {
                Server = sinkServer
            });

            var pullReplicationName = $"{hubStore.Database}-pull";
            var connectionStringName = "ConnectionString-" + hubStore.Database;

            var sinkDb = await GetDatabase(sinkStore.Database, sinkServer);

            // Open = normal, Closed = reads hang (socket stays up, no data arrives).
            var networkGate = new AsyncGate();
            var connectionCounter = 0;

            var forTesting = sinkDb.ReplicationLoader.ForTestingPurposesOnly();
            forTesting.WrapIncomingReplicationStream = innerStream =>
            {
                // Hang ONLY the first connection (the one we will strangle). Any reconnection must read
                // normally, otherwise the global gate would block the recovered connection too and the
                // second document could never arrive -- on buggy AND fixed code alike.
                var n = Interlocked.Increment(ref connectionCounter);
                return n == 1
                    ? new SmartHangingStreamWrapper(innerStream, networkGate)
                    : innerStream;
            };

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(pullReplicationName)));
            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = connectionStringName,
                Database = hubStore.Database,
                TopologyDiscoveryUrls = hubStore.Urls
            }));
            await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                Name = pullReplicationName,
                HubName = pullReplicationName,
                ConnectionStringName = connectionStringName
            }));

            // 1. initial replication works
            using (var session = hubStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = "Users/1-A", Name = "Lev" });
                await session.SaveChangesAsync();
            }
            Assert.True(WaitForDocument<User>(sinkStore, "Users/1-A", u => u.Name == "Lev"), "Initial replication failed");

            var initialConnections = connectionCounter;

            // The incoming pull handler must be established before we strangle it.
            Assert.True(WaitForValue(() => sinkDb.ReplicationLoader.IncomingHandlers.OfType<IncomingReplicationHandler>().Any(), true),
                "Expected an incoming pull replication handler on the sink");

            // 2. Simulate a busy sink node: keep poking the notify event on every live incoming handler,
            // exactly as OnIncomingReceiveSucceeded would when sibling connections receive batches.
            using var pumpCts = new CancellationTokenSource();
            var pump = Task.Run(async () =>
            {
                while (pumpCts.IsCancellationRequested == false)
                {
                    foreach (var handler in sinkDb.ReplicationLoader.IncomingHandlers)
                    {
                        try
                        {
                            (handler as IncomingReplicationHandler)?.OnReplicationFromAnotherSource();
                        }
                        catch
                        {
                            // handler may be disposed mid-iteration; ignore
                        }
                    }

                    try
                    { await Task.Delay(100, pumpCts.Token); }
                    catch { /* cancelled */ }
                }
            });

            // 3. The hub goes silent on this connection (socket up, nothing arrives). The notify wake-ups keep driving
            // the "notify" branch -> GetHeartbeatStatusMessage -> LastHeartbeatTicks = now (a SEND-path update), so the
            // dead connection keeps looking "fresh" with zero receives -- which is what makes AssertValidConnection
            // reject the reconnect. The read-timeout fix must reap it despite that.
            networkGate.Close();

            using (var session = hubStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = "Users/2-A", Name = "Lev" });
                await session.SaveChangesAsync();
            }

            // Allow several timeout windows for the zombie to be reaped, the sink to reconnect, and the doc to arrive.
            var timeout = (int)sinkDb.Configuration.Replication.ActiveConnectionTimeout.AsTimeSpan.TotalMilliseconds * 4;

            // PRIMARY assertion (gates the read-timeout fix): even on a busy sink, the read-timeout must fire, reap the
            // zombie, and trigger a reconnect. A reconnect creates a new incoming stream, so connectionCounter increments.
            var reconnected = WaitForValue(() => connectionCounter > initialConnections, true, timeout: timeout, interval: 200);
            Assert.True(reconnected,
                $"The hung pull connection was never reaped while the sink was busy. connectionCounter={connectionCounter}, initial={initialConnections}. " +
                $"The notify wake-ups kept resetting the read-timeout, leaving an immortal zombie incoming handler that only a task restart would clear.");

            // SECONDARY assertion (end-to-end recovery): once reaped, the sink reconnects on a clean stream and the
            // second document arrives.
            Assert.True(WaitForDocument<User>(sinkStore, "Users/2-A", u => u.Name == "Lev", timeout), "Second replication failed (end-to-end recovery)");

            pumpCts.Cancel();
            try
            { await pump; }
            catch { /* ignored */ }
        }

        // The flip side of the read-timeout fix: it must NOT reap a HEALTHY idle connection.
        // A healthy idle source keeps sending heartbeats (every ReplicationMinimalHeartbeat), which the sink RECEIVES
        // (the msg.Document != null branch) and which restart sinceLastReceive. As long as ActiveConnectionTimeout >
        // ReplicationMinimalHeartbeat, the connection stays alive across many timeout windows. (The notify/"interrupted"
        // branch deliberately does NOT restart the timer; it doesn't need to -- received heartbeats do.)
        [RavenFact(RavenTestCategory.Replication)]
        public async Task HealthyIdleConnection_IsNotReaped()
        {
            DoNotReuseServer();

            // read-timeout (5s) > heartbeat interval (2s): a healthy idle peer's heartbeats keep the connection alive.
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Replication.ActiveConnectionTimeout)] = "5",
                [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "2"
            };

            using var hubServer = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings });
            using var sinkServer = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings });

            using var hubStore = GetDocumentStore(new Options { Server = hubServer });
            using var sinkStore = GetDocumentStore(new Options { Server = sinkServer });

            var pullReplicationName = $"{hubStore.Database}-pull";
            var connectionStringName = "ConnectionString-" + hubStore.Database;
            var sinkDb = await GetDatabase(sinkStore.Database, sinkServer);

            var connectionCounter = 0;
            var forTesting = sinkDb.ReplicationLoader.ForTestingPurposesOnly();
            forTesting.WrapIncomingReplicationStream = innerStream =>
            {
                // Passthrough: the connection stays healthy; we only count (re)connections.
                Interlocked.Increment(ref connectionCounter);
                return innerStream;
            };

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition(pullReplicationName)));
            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = connectionStringName,
                Database = hubStore.Database,
                TopologyDiscoveryUrls = hubStore.Urls
            }));
            await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                Name = pullReplicationName,
                HubName = pullReplicationName,
                ConnectionStringName = connectionStringName
            }));

            using (var session = hubStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = "Users/1-A", Name = "Lev" });
                await session.SaveChangesAsync();
            }
            Assert.True(WaitForDocument<User>(sinkStore, "Users/1-A", u => u.Name == "Lev"), "Initial replication failed");

            var initialConnections = connectionCounter;

            IncomingReplicationHandler pullHandler = null;
            Assert.True(WaitForValue(() =>
            {
                pullHandler = sinkDb.ReplicationLoader.IncomingHandlers.OfType<IncomingReplicationHandler>().FirstOrDefault();
                return pullHandler != null;
            }, true), "Expected an incoming pull replication handler on the sink");

            // A healthy idle source keeps sending heartbeats (every ReplicationMinimalHeartbeat = 2s), which the sink
            // RECEIVES (the msg.Document != null branch) and which restart sinceLastReceive. Because ActiveConnectionTimeout
            // (5s) is larger than the heartbeat interval, the connection must survive many read-timeout windows and must
            // never be reaped/reconnected. (The notify branch deliberately does NOT restart the timer -- and it doesn't
            // need to: received heartbeats keep the connection alive. The reproduction test above proves the converse,
            // that notify wake-ups alone do NOT keep a connection that receives nothing alive.)
            var readTimeoutMs = (int)sinkDb.Configuration.Replication.ActiveConnectionTimeout.AsTimeSpan.TotalMilliseconds;
            for (var i = 0; i < (readTimeoutMs * 3) / 1000; i++)
            {
                await Task.Delay(1000);
                Assert.Equal(initialConnections, connectionCounter); // a reconnect would increment this => reaped
                var current = sinkDb.ReplicationLoader.IncomingHandlers.OfType<IncomingReplicationHandler>().FirstOrDefault();
                Assert.True(ReferenceEquals(current, pullHandler),
                    $"The healthy incoming handler was reaped/replaced while idle (connectionCounter={connectionCounter}, initial={initialConnections}).");
            }

            // And it still works: a new document flows over the SAME connection (no reconnect).
            using (var session = hubStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = "Users/2-A", Name = "Lev" });
                await session.SaveChangesAsync();
            }
            Assert.True(WaitForDocument<User>(sinkStore, "Users/2-A", u => u.Name == "Lev", 10_000), "Healthy connection stopped replicating");
            Assert.Equal(initialConnections, connectionCounter);
        }

        // Realistic fleet-load check (external replication, same code path as a hub): many sources replicate into one
        // destination. One source stays idle (the "victim"); the others push continuously, so every batch fires
        // ReplicationLoader.OnIncomingReceiveSucceeded -> OnReplicationFromAnotherSource on the victim's handler (genuine
        // notify fan-out). Under that load an idle connection may be transiently reaped (the notify flood can delay the
        // idle peer's heartbeat round-trip past ActiveConnectionTimeout), but it must RECOVER and keep replicating -- no
        // permanent outage or deadlock. This guards against the read-timeout fix causing unrecoverable churn at scale.
        [RavenFact(RavenTestCategory.Replication)]
        public async Task BusyDestination_IdleSource_RecoversAndKeepsReplicating()
        {
            DoNotReuseServer();

            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Replication.ActiveConnectionTimeout)] = "10",
                [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "2"
            };
            using var server = GetNewServer(new ServerCreationOptions { CustomSettings = customSettings });

            using var dest = GetDocumentStore(new Options { Server = server });
            using var victim = GetDocumentStore(new Options { Server = server });
            var noisy = new List<DocumentStore>();
            for (var i = 0; i < 4; i++)
                noisy.Add((DocumentStore)GetDocumentStore(new Options { Server = server }));

            var destDb = await GetDatabase(dest.Database, server);

            var sources = new List<DocumentStore> { victim };
            sources.AddRange(noisy);

            // external replication: every source -> destination
            foreach (var src in sources)
                await SetupReplicationAsync(src, dest);

            // seed one doc per source so all connections establish and replicate
            for (var i = 0; i < sources.Count; i++)
            {
                using var s = sources[i].OpenAsyncSession();
                await s.StoreAsync(new User { Id = $"seed/{i}", Name = "x" });
                await s.SaveChangesAsync();
            }
            Assert.True(WaitForValue(() => destDb.ReplicationLoader.IncomingHandlers.Count() == sources.Count, true, timeout: 30_000),
                $"Expected {sources.Count} incoming handlers on the destination, got {destDb.ReplicationLoader.IncomingHandlers.Count()}");

            await Task.Delay(2000); // let things settle

            // The noisy sources push continuously -> genuine OnIncomingReceiveSucceeded fan-out onto every handler.
            using var noiseCts = new CancellationTokenSource();
            var noiseTasks = noisy.Select((src, idx) => Task.Run(async () =>
            {
                var k = 0;
                while (noiseCts.IsCancellationRequested == false)
                {
                    try
                    {
                        using var s = src.OpenAsyncSession();
                        await s.StoreAsync(new User { Id = $"noise/{idx}/{k++}", Name = "n" });
                        await s.SaveChangesAsync();
                    }
                    catch { /* shutting down */ }
                    try
                    { await Task.Delay(100, noiseCts.Token); }
                    catch { /* cancelled */ }
                }
            })).ToList();

            // Let the system run under heavy notify fan-out for several read-timeout windows. Under that load an idle
            // connection MAY be reaped transiently (the notify flood can delay the idle peer's heartbeat round-trip past
            // ActiveConnectionTimeout), but it must RECOVER -- reconnect and keep replicating, with no permanent outage.
            // Active connections are never affected (received data resets the timer). We tolerate transient reconnects
            // and assert recovery below.
            var readTimeoutMs = (int)destDb.Configuration.Replication.ActiveConnectionTimeout.AsTimeSpan.TotalMilliseconds;
            await Task.Delay(readTimeoutMs * 3);

            // The idle victim must be connected (recovered if it was transiently reaped) and must keep replicating.
            Assert.True(WaitForValue(() => destDb.ReplicationLoader.IncomingHandlers.Any(h => h.ConnectionInfo.SourceDatabaseName == victim.Database), true, timeout: 20_000),
                "The idle victim never re-established its connection under fleet load.");

            using (var s = victim.OpenAsyncSession())
            {
                await s.StoreAsync(new User { Id = "victim/final", Name = "v" });
                await s.SaveChangesAsync();
            }
            Assert.True(WaitForDocument<User>(dest, "victim/final", u => u.Name == "v", 20_000),
                "The idle victim source stopped replicating under fleet load.");

            noiseCts.Cancel();
            foreach (var t in noiseTasks)
            { try { await t; } catch { /* ignored */ } }
            foreach (var s in noisy)
                s.Dispose();
        }

        private class AsyncGate
        {
            private readonly object _lock = new object();
            private TaskCompletionSource<object> _openTcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            private TaskCompletionSource<object> _closeTcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            private volatile bool _isOpen = true;

            public AsyncGate()
            {
                _openTcs.SetResult(null);
            }

            public bool IsOpen => _isOpen;

            public void Close()
            {
                lock (_lock)
                {
                    if (!_isOpen)
                        return;
                    _isOpen = false;
                    _openTcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _closeTcs.TrySetResult(null);
                }
            }

            public void Open()
            {
                lock (_lock)
                {
                    if (_isOpen)
                        return;
                    _isOpen = true;
                    _closeTcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _openTcs.TrySetResult(null);
                }
            }

            public Task WaitToOpenAsync(CancellationToken token)
            {
                lock (_lock)
                {
                    if (_isOpen)
                        return Task.CompletedTask;
                    return _openTcs.Task.WithCancellation(token);
                }
            }

            public Task WaitToCloseAsync(CancellationToken token)
            {
                lock (_lock)
                {
                    if (!_isOpen)
                        return Task.CompletedTask;
                    return _closeTcs.Task.WithCancellation(token);
                }
            }
        }

        private class SmartHangingStreamWrapper : Stream
        {
            private readonly Stream _inner;
            private readonly AsyncGate _gate;
            private readonly CancellationTokenSource _disposeCts = new CancellationTokenSource();

            public SmartHangingStreamWrapper(Stream inner, AsyncGate gate)
            {
                _inner = inner;
                _gate = gate;
            }

            public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token))
                    {
                        try
                        {
                            await _gate.WaitToOpenAsync(linkedCts.Token);
                        }
                        catch (OperationCanceledException)
                        {
                            if (_disposeCts.IsCancellationRequested)
                                throw new IOException("Stream disposed");
                            throw;
                        }

                        var readTask = _inner.ReadAsync(buffer, offset, count, cancellationToken);
                        var closeGateTask = _gate.WaitToCloseAsync(linkedCts.Token);

                        var completedTask = await Task.WhenAny(readTask, closeGateTask);
                        if (completedTask == closeGateTask)
                        {
                            continue;
                        }

                        return await readTask;
                    }
                }
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _disposeCts.Cancel();
                    _inner.Dispose();
                    _disposeCts.Dispose();
                }
                base.Dispose(disposing);
            }

            public override void Flush() => _inner.Flush();
            public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
            public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
            public override void SetLength(long value) => _inner.SetLength(value);
            public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
            public override bool CanRead => _inner.CanRead;
            public override bool CanSeek => _inner.CanSeek;
            public override bool CanWrite => _inner.CanWrite;
            public override long Length => _inner.Length;
            public override long Position { get => _inner.Position; set => _inner.Position = value; }
        }
    }
}
