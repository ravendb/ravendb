using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.Replication;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL;

public class PinOnGoingTaskToMentorNode : ReplicationTestBase
{
    public PinOnGoingTaskToMentorNode(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Can_Set_Pin_To_Mentor_Node_Etl()
    {
        const string srcDb = "ETL-src";
        const string dstDb = "ETL-dst";
        
        var srcRaft = await CreateRaftCluster(3);
        var leader = srcRaft.Leader;

        var srcNodes = await CreateDatabaseInCluster(srcDb, 3, leader.WebUrl);
        var destNodes = await CreateDatabaseInCluster(dstDb, 2, leader.WebUrl);

        var mentorNode = srcNodes.Servers.First(s => s != leader);
        var mentorTag = mentorNode.ServerStore.NodeTag;
        using (var src = new DocumentStore
               {
                   Urls = srcNodes.Servers.Select(s => s.WebUrl).ToArray(),
                   Database = srcDb,
               }.Initialize())

        using (var dest = new DocumentStore
               {
                   Urls = new[]
                   {
                       destNodes.Servers.First(u => u != mentorNode).WebUrl
                   },
                   Database = dstDb,
               }.Initialize())
        {
            const string name = "PinToMentorNode";
            var urls =  destNodes.Servers.Select(u => u.WebUrl);
            var config = new RavenEtlConfiguration
            {
                Name = name,
                ConnectionStringName = name,
                MentorNode = mentorTag,
                PinToMentorNode = true,
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
                TopologyDiscoveryUrls = urls.ToArray(),
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


            var originalResult = await DisposeServerAndWaitForFinishOfDisposalAsync(originalTaskNodeServer);

            using (var session = src.OpenSession())
            {
                session.Store(new User()
                {
                    Name = "Joe Doe2"
                }, "users/2");

                session.SaveChanges();
            }
            
            Assert.False(WaitForDocument<User>(dest, "users/2", u => u.Name == "Joe Doe2", 10_000));

            var revivedServer =GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Core.ServerUrls), originalResult.Url}
                },
                RunInMemory = false,
                DeletePrevious = false,
                DataDirectory = originalResult.DataDirectory
            });

            var waitForNotPassive = revivedServer.ServerStore.Engine.WaitForLeaveState(RachisState.Passive,CancellationToken.None);
            Assert.True(waitForNotPassive.Wait(TimeSpan.FromSeconds(10_000)));
            using (var session = src.OpenSession())
            {
                session.Store(new User()
                    {
                        Name = "Joe Doe3"
                    },
                    "users/3");

                session.SaveChanges();
            }

            Assert.True(WaitForDocument<User>(dest, "users/3", u => u.Name == "Joe Doe3", 10_000));
            Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "Joe Doe2", 10_000));
        
        }
    }

    [Fact]
    public async Task Can_Fail_Over_When_Removing_Mentor_Node_Etl()
    {
        const string srcDb = "ETL-src";
        const string dstDb = "ETL-dst";
        
        var srcRaft = await CreateRaftCluster(5);
        var leader = srcRaft.Leader;

        var srcNodes = await CreateDatabaseInCluster(srcDb, 5, leader.WebUrl);
        var destNodes = await CreateDatabaseInCluster(dstDb, 3, leader.WebUrl);

        var mentorNode = srcNodes.Servers.First(s => s != leader);
        var mentorTag = mentorNode.ServerStore.NodeTag;

        using (var src = new DocumentStore
               {
                   Urls = new [] {leader.WebUrl},
                   Database = srcDb,
               }.Initialize())

        using (var dest = new DocumentStore
               {
                   Urls =  new [] {destNodes.Servers.First(u => u != mentorNode).WebUrl},
                   Database = dstDb,
               }.Initialize())
        {
            const string name = "PinToMentorNode";
            var urls =  destNodes.Servers.Select(u => u.WebUrl);
            var config = new RavenEtlConfiguration
            {
                Name = name,
                ConnectionStringName = name,
                MentorNode = mentorTag,
                PinToMentorNode = true,
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
                TopologyDiscoveryUrls = urls.ToArray(),
            };
            
            var result = src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
            Assert.NotNull(result.RaftCommandIndex);

            src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(config));
            
            var ongoingTask = src.Maintenance.Send(new GetOngoingTaskInfoOperation(name, OngoingTaskType.RavenEtl));
            var responsibleNodeNodeTag = ongoingTask.ResponsibleNode.NodeTag;
            
            using (var session = src.OpenSession())
            {
                session.Store(new User()
                {
                    Name = "Joe Doe"
                }, "users/1");

                session.SaveChanges();
            }
            
            Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Joe Doe", 30_000));
            
            await ActionWithLeader(l =>
            {
                l.ServerStore.RemoveFromClusterAsync(responsibleNodeNodeTag);
            });
            
            var waitForPassive = mentorNode.ServerStore.Engine.WaitForState(RachisState.Passive,CancellationToken.None);
            Assert.True(waitForPassive.Wait(TimeSpan.FromSeconds(10_000)));
            
            var val = await WaitForValueAsync(async () =>
                {
                    var dbRecord = await src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(srcDb));
                    return dbRecord.Topology.Members.Count;
                }, 4);
            Assert.Equal(4, val);

            using (var session = src.OpenSession())
            {
                session.Store(new User()
                {
                    Name = "Joe Doe2"
                }, "users/2");

                session.SaveChanges();
            }

            await WaitForValueAsync(async () =>
            {
                ongoingTask = await src.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(name, OngoingTaskType.RavenEtl));
                return ongoingTask.ResponsibleNode.NodeTag != responsibleNodeNodeTag;
            }, true);

            Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "Joe Doe2", 10_000));
        }
    }
    
    [Fact]
    public async Task Can_Set_Pin_To_Node_Property_Subscription()
    {
        var store = GetDocumentStore();

        var subscriptionName = await store.Subscriptions.CreateAsync<User>(options: new SubscriptionCreationOptions
        {
            MentorNode = "A",
            PinToMentorNode = true
        }).ConfigureAwait(false);
    
        var state = await store.Subscriptions.GetSubscriptionStateAsync(subscriptionName, store.Database);
        Assert.True(state.PinToMentorNode);
    }
    
    [Fact]
    public async Task Can_Set_Pin_To_Node_Backup()
    {
        var store = GetDocumentStore();
        var backupPath = NewDataPath();
        var updateBackupResult = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(new PeriodicBackupConfiguration
        {
            BackupType = BackupType.Backup,
            LocalSettings = new LocalSettings
            {
                FolderPath = backupPath
            },
            PinToMentorNode = true,
            FullBackupFrequency = "* * * * *",
        }));

        var res = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(updateBackupResult.TaskId, OngoingTaskType.Backup));
        Assert.True(res.PinToMentorNode);
    }
    
    [Fact]
    public async Task Can_Set_Pin_To_Node_ExternalReplication()
    {
        var dbName = GetDatabaseName();
        var watcher = new ExternalReplication(dbName, "Connection")
        {
            PinToMentorNode = true,
            Name = "MyExternalReplication"
        };

        using (var store = GetDocumentStore())
        {
            var replicationOperation = new UpdateExternalReplicationOperation(watcher);
            var replicationResult = await store.Maintenance.SendAsync(replicationOperation);
            var res = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(replicationResult.TaskId, OngoingTaskType.Replication));
            Assert.True(res.PinToMentorNode);
        }
    }

    [Fact]
    public async Task Can_Set_Pin_To_Node_Pull_Replication_As_Hub()
    {
        const int clusterSize = 3;

        var (hubNodes, hubLeader, hubCertificatesHolder) = await CreateRaftClusterWithSsl(clusterSize, watcherCluster: true, shouldRunInMemory: true);
        var adminHubClusterCert = hubCertificatesHolder.ServerCertificate.Value;
        
        var mentorNodes = hubNodes.Where(s => s.ServerStore.NodeTag != hubLeader.ServerStore.NodeTag).ToList();
       
        var hubMentorNode = mentorNodes[0];
        var sinkMentorNode = mentorNodes[1];

        using (var hubStore = GetDocumentStore(new Options
               {
                   Server = hubLeader,
                   ReplicationFactor = 3,
                   AdminCertificate = adminHubClusterCert,
                   ClientCertificate = adminHubClusterCert,
               }))
        {
            
            using (var sinkStore = GetDocumentStore(new Options
                   {
                       Server = sinkMentorNode,
                       ReplicationFactor = 3,
                       AdminCertificate = adminHubClusterCert,
                       ClientCertificate = adminHubClusterCert,
                   }))
            { 
                await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                MentorNode = hubMentorNode.ServerStore.NodeTag,
                PinToMentorNode = true,
                Name = hubStore.Database + "HUB",
            }));

            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hubStore.Database,
                Name = hubStore.Database + "ConStr",
                TopologyDiscoveryUrls = hubNodes.Select(u => u.WebUrl).ToArray()
            }));

            await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = hubStore.Database + "ConStr",
                HubName =  hubStore.Database + "HUB",
            }));
            WaitForUserToContinueTheTest(hubStore,true,hubStore.Database,adminHubClusterCert);

            using (var hubSession = hubStore.OpenSession())
            {
                hubSession.Store(new User
                {
                    Name = "Arava",
                }, "users/1");
                hubSession.SaveChanges();
            }

            Assert.True(WaitForDocument<User>(sinkStore, "users/1", u => u.Name == "Arava",30_000));
            var disposedServer = await DisposeServerAndWaitForFinishOfDisposalAsync(hubMentorNode);
            using (var hubSession = hubStore.OpenSession())
            {
                hubSession.Store(new User
                {
                    Name = "Arava2",
                }, "users/2");
                hubSession.SaveChanges();
            }
            Assert.False(WaitForDocument<User>(sinkStore, "users/2", u => u.Name == "Arava2",30_000));
            var revivedServer = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Core.ServerUrls), disposedServer.Url},
                    {RavenConfiguration.GetKey(x => x.Security.CertificatePath), hubCertificatesHolder.ServerCertificatePath}
                },
                RunInMemory = true,
                DataDirectory = disposedServer.DataDirectory
            });
            var waitForNotPassive = revivedServer.ServerStore.Engine.WaitForLeaveState(RachisState.Passive, CancellationToken.None);
            Assert.True(waitForNotPassive.Wait(TimeSpan.FromSeconds(20_000)));
            Assert.True(WaitForDocument<User>(sinkStore, "users/2", u => u.Name == "Arava2",30_000)); 
            Assert.Equal(3, await WaitForValueAsync(async () => await GetMembersCount(hubStore, hubStore.Database), 3));
            Assert.Equal(3, await WaitForValueAsync(async () => await GetMembersCount(sinkStore, sinkStore.Database), 3));
            }
        }
    }
   
    [Fact]
    public async Task Can_Set_Pin_To_Node_Pull_Replication_As_Sink()
    {
        const int clusterSize = 3;
    
        var (hubNodes, hubLeader, hubCertificatesHolder) = await CreateRaftClusterWithSsl(clusterSize, watcherCluster: true, shouldRunInMemory: true);
        var adminHubClusterCert = hubCertificatesHolder.ServerCertificate.Value;
        
        var mentorNodes = hubNodes.Where(s => s.ServerStore.NodeTag != hubLeader.ServerStore.NodeTag).ToList();
       
        var hubMentorNode = mentorNodes[0];
        var sinkMentorNode = mentorNodes[1];
        using (var hubStore = GetDocumentStore(new Options
        {
           Server = hubMentorNode,
           ReplicationFactor = 1,
           AdminCertificate = adminHubClusterCert,
           ClientCertificate = adminHubClusterCert,
           ModifyDatabaseRecord = r =>
           {
               r.Topology = new DatabaseTopology();
               r.Topology.Members.Add(hubMentorNode.ServerStore.NodeTag);
           }
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
           Server = hubLeader,
           ReplicationFactor = 3,
           AdminCertificate = adminHubClusterCert,
           ClientCertificate = adminHubClusterCert,
        }))
        {
            using (var hubSession = hubStore.OpenAsyncSession())
            {
                await hubSession.StoreAsync(new {Type = "Eggs"}, "menus/breakfast");
                await hubSession.StoreAsync(new {Name = "Bird Seed Milkshake"}, "recipes/bird-seed-milkshake");
                await hubSession.StoreAsync(new {Name = "3 USD"}, "prices/eastus/2");
                await hubSession.StoreAsync(new {Name = "3 EUR"}, "prices/eu/1");
                await hubSession.SaveChangesAsync();
            }

            using (var sinkSession = sinkStore.OpenAsyncSession())
            {
                await sinkSession.StoreAsync(new {Name = "Candy"}, "orders/bert/3");
                await sinkSession.SaveChangesAsync();
            }

            await hubStore.Maintenance.SendAsync(new PutPullReplicationAsHubOperation(new PullReplicationDefinition
            {
                Name = "Franchises",
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub,
                WithFiltering = true,
            }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation("Franchises",
                new ReplicationHubAccess
                {
                    Name = "Franchises",
                    CertificateBase64 = Convert.ToBase64String(hubCertificatesHolder.ClientCertificate1.Value.Export(X509ContentType.Cert)),
                    AllowedSinkToHubPaths = new[] {"orders/*","users/*"},
                    AllowedHubToSinkPaths = new[] {"menus/*", "prices/eastus/*", "recipes/*"}
                }));

            await sinkStore.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Database = hubStore.Database, Name = "HopperConStr", TopologyDiscoveryUrls = hubStore.Urls
            }));
            await sinkStore.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(new PullReplicationAsSink
            {
                ConnectionStringName = "HopperConStr",
                PinToMentorNode = true,
                MentorNode = sinkMentorNode.ServerStore.NodeTag,
                CertificateWithPrivateKey = Convert.ToBase64String(hubCertificatesHolder.ClientCertificate1.Value.Export(X509ContentType.Pfx)),
                HubName = "Franchises",
                Mode = PullReplicationMode.HubToSink | PullReplicationMode.SinkToHub
            }));

            var disposedServer = await DisposeServerAndWaitForFinishOfDisposalAsync(sinkMentorNode);
            using (var sinkSession = sinkStore.OpenAsyncSession())
            {
                await sinkSession.StoreAsync(new User {Name = "Arava",}, "users/1");
                await sinkSession.SaveChangesAsync();
            }
            var revivedServer = GetNewServer(new ServerCreationOptions
            {
                CustomSettings = new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Core.ServerUrls), disposedServer.Url},
                    {RavenConfiguration.GetKey(x => x.Security.CertificatePath), hubCertificatesHolder.ServerCertificatePath}
                },
                RunInMemory = true,
                DataDirectory = disposedServer.DataDirectory
            });
            var waitForNotPassive = revivedServer.ServerStore.Engine.WaitForLeaveState(RachisState.Passive, CancellationToken.None);
            Assert.True(waitForNotPassive.Wait(TimeSpan.FromSeconds(20_000)));
            Assert.True(WaitForDocument<User>(hubStore, "users/1", u => u.Name == "Arava", 30_000));
            Assert.Equal(1, await WaitForValueAsync(async () => await GetMembersCount(hubStore, hubStore.Database), 1));
            Assert.Equal(3, await WaitForValueAsync(async () => await GetMembersCount(sinkStore, sinkStore.Database), 3));
        }
    }
    
    private static AddEtlOperationResult AddEtl<T>(IDocumentStore src, EtlConfiguration<T> configuration, T connectionString) where T : ConnectionString
    {
        var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
        Assert.NotNull(putResult.RaftCommandIndex);

        var addResult = src.Maintenance.Send(new AddEtlOperation<T>(configuration));
        return addResult;
    }
}
