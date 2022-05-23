using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL;

public class PinOnGoingTaskToMentorNode : ClusterTestBase
{
    public PinOnGoingTaskToMentorNode(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task ETL_Task_Should_Be_Pinned_To_Mentor_Node()
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
    public async Task ETL_Should_Fail_Over_When_Removing_Mentor_Node_And_Pin_To_MentorNode()
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
    
    private static AddEtlOperationResult AddEtl<T>(IDocumentStore src, EtlConfiguration<T> configuration, T connectionString) where T : ConnectionString
    {
        var putResult = src.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
        Assert.NotNull(putResult.RaftCommandIndex);

        var addResult = src.Maintenance.Send(new AddEtlOperation<T>(configuration));
        return addResult;
    }

}
