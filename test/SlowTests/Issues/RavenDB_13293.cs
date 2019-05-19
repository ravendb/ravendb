using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13293 : ReplicationTestBase
    {
        [Fact]
        public async Task CanPassNodeTagToChangesApi()
        {
            var myNodesList = new List<string>();
            const int clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: false);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                myNodesList.AddRange(databaseResult.Topology.AllNodes);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "EGR"
                    }, "users/1-A");
                    await session.SaveChangesAsync();
                }

                IDatabaseChanges databaseChanges;
                foreach (var node in myNodesList)
                {
                    databaseChanges = store.Changes(store.Database, node);
                    var dbChanges = await databaseChanges.EnsureConnectedNow();
                    Assert.True(dbChanges.Connected);
                    var list = new BlockingCollection<DocumentChange>();
                    var observableWithTask = dbChanges.ForDocument("users/1-A");
                    observableWithTask.Subscribe(list.Add);
                    await observableWithTask.EnsureSubscribedNow();

                    using (var session = store.OpenAsyncSession())
                    {
                        var egr = await session.LoadAsync<User>("users/1-A");
                        egr.Name = $"EGR_{node}";
                        await session.SaveChangesAsync();
                    }
                    DocumentChange documentChange;
                    Assert.True(list.TryTake(out documentChange, TimeSpan.FromSeconds(15)));
                }
                databaseChanges = store.Changes(store.Database, "X");
                await Assert.ThrowsAsync<RequestedNodeUnavailableException>(async () => await databaseChanges.EnsureConnectedNow());

                databaseChanges = store.Changes(store.Database, "");
                await Assert.ThrowsAsync<RequestedNodeUnavailableException>(async () => await databaseChanges.EnsureConnectedNow());

                databaseChanges = store.Changes(store.Database, " ");
                await Assert.ThrowsAsync<RequestedNodeUnavailableException>(async () => await databaseChanges.EnsureConnectedNow());
            }
        }

        [Fact]
        public async Task CanPassNodeTagToRestoreBackupOperation()
        {
            var myBackupsList = new List<MyBackup>();
            var myNodesList = new List<string>();
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: false);

            using (var store = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName,
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                store.Maintenance.Send(new CreateSampleDataOperation());
                myNodesList.AddRange(databaseResult.Topology.AllNodes);

                foreach (var node in myNodesList)
                {
                    var myGuid = Guid.NewGuid();
                    var backupConfig = new PeriodicBackupConfiguration
                    {
                        LocalSettings = new LocalSettings
                        {
                            FolderPath = Path.Combine(backupPath, myGuid.ToString())
                        },
                        FullBackupFrequency = "0 */3 * * *",
                        BackupType = BackupType.Backup,
                        Name = $"Task_{node}_{myGuid}",
                        MentorNode = node
                    };
                    var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfig));
                    var res = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(result.TaskId, OngoingTaskType.Backup));

                    myBackupsList.Add(new MyBackup
                    {
                        BackupTaskId = result.TaskId,
                        Guid = myGuid,
                        NodeTag = res.ResponsibleNode.NodeTag
                    });
                }

                foreach (var myBackup in myBackupsList)
                {
                    await store.Maintenance.SendAsync(new StartBackupOperation(true, myBackup.BackupTaskId));
                    var backupOperation = new GetPeriodicBackupStatusOperation(myBackup.BackupTaskId);
                    var getPeriodicBackupResult = store.Maintenance.Send(backupOperation);

                    var value = WaitForValue(() =>
                    {
                        getPeriodicBackupResult = store.Maintenance.Send(backupOperation);
                        return getPeriodicBackupResult.Status?.LastEtag > 0;
                    }, true);
                    Assert.Equal(true, value);

                    Assert.NotNull(getPeriodicBackupResult);
                    Assert.Equal(myBackup.NodeTag, getPeriodicBackupResult.Status.NodeTag);
                    var prePath = Path.Combine(backupPath, myBackup.Guid.ToString());
                    myBackup.BackupPath = Path.Combine(prePath, getPeriodicBackupResult.Status.FolderName);
                }

                foreach (var myBackup in myBackupsList)
                {
                    var restoreConfig = new RestoreBackupConfiguration
                    {
                        DatabaseName = $"restored_DB1_{myBackup.NodeTag}",
                        BackupLocation = myBackup.BackupPath
                    };
                    var restoreBackupTask = store.Maintenance.Server.Send(new RestoreBackupOperation(restoreConfig, myBackup.NodeTag));
                    restoreBackupTask.WaitForCompletion(TimeSpan.FromSeconds(30));
                }
                var numOfDbs = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, int.MaxValue));
                Assert.Equal(clusterSize + 1, numOfDbs.Length);
            }
        }

        [Fact]
        public async Task CanPassNodeTagToRestorePatchOperation()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, useSsl: false);
            var myNodesList = new List<string>();

            using (var store = new DocumentStore
            {
                Urls = new[] {leader.WebUrl},
                Database = databaseName,
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                myNodesList.AddRange(databaseResult.Topology.AllNodes);

                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Items select new { doc.Name }" },
                    Name = "MyIndex"
                }}));

                using (var commands = store.Commands())
                {
                    await commands.PutAsync("items/1", null, new { Name = "testname" }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Items"}
                    });

                    WaitForIndexing(store);

                    var operation = store.Operations.Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = "FROM INDEX 'MyIndex' UPDATE { this.NewName = 'NewValue'; } "
                    }));

                    var opStatus = store.Maintenance.Send(new GetOperationStateOperation(operation.Id));
                    Assert.NotNull(opStatus);

                    foreach (var node in myNodesList)
                    {
                        var op = store.Maintenance.Send(new GetOperationStateOperation(operation.Id, node));
                        if(node == operation.NodeTag)
                            Assert.NotNull(op);
                        else
                            Assert.Null(op);
                    }

                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));
                    dynamic document = await commands.GetAsync("items/1");
                    Assert.Equal("NewValue", document.NewName.ToString());
                }
            }
        }

        public class MyBackup
        {
            public string BackupPath { get; set; }
            public long BackupTaskId { get; set; }
            public Guid Guid { get; set; }
            public string NodeTag { get; set; }
        }

        public class User
        {
            public string Name { get; set; }
        }
    }
}

