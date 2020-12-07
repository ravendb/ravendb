using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
using Raven.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13293 : ReplicationTestBase
    {
        public RavenDB_13293(ITestOutputHelper output) : base(output)
        {
        }

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
                databaseChanges = store.Changes(store.Database, "XYZ");
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
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            (List<RavenServer> Nodes, RavenServer Leader) cluster = await CreateRaftCluster(clusterSize, false, useSsl: false);
            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
                Database = databaseName,
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                await store.Maintenance.SendAsync(new CreateSampleDataOperation());
                var myNodesList = databaseResult.Topology.AllNodes.ToList();
                Assert.True(clusterSize == myNodesList.Count, $"clusterSize({clusterSize}) == myNodesList.Count({myNodesList.Count})");

                foreach (var node in myNodesList)
                {
                    var myGuid = Guid.NewGuid();
                    var backupConfig = new PeriodicBackupConfiguration
                    {
                        LocalSettings = new LocalSettings
                        {
                            FolderPath = Path.Combine(backupPath, myGuid.ToString())
                        },
                        FullBackupFrequency = "0 0 1 1 *", // once a year on 1st january at 00:00
                        BackupType = BackupType.Backup,
                        Name = $"Task_{node}_{myGuid}",
                        MentorNode = node
                    };
                    var result = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(backupConfig));
                    await WaitForRaftIndexToBeAppliedOnClusterNodes(result.RaftCommandIndex, cluster.Nodes);
                    var res = await store.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(result.TaskId, OngoingTaskType.Backup));
                    Assert.NotNull(res);
                    Assert.True(node == res.MentorNode, $"node({node}) == res.MentorNode({res.MentorNode})");
                    Assert.True(node == res.ResponsibleNode.NodeTag, $"node({node}) == res.ResponsibleNode.NodeTag({res.ResponsibleNode.NodeTag})");
                    myBackupsList.Add(new MyBackup
                    {
                        BackupTaskId = result.TaskId,
                        Guid = myGuid,
                        NodeTag = res.ResponsibleNode.NodeTag
                    });
                }

                foreach (var myBackup in myBackupsList)
                {
                    var res = await store.Maintenance.SendAsync(new StartBackupOperation(isFullBackup: true, myBackup.BackupTaskId));
                    Assert.True(myBackup.NodeTag == res.ResponsibleNode, $"myBackup.NodeTag({myBackup.NodeTag}) == res.ResponsibleNode({res.ResponsibleNode})");
                    var operation = new GetPeriodicBackupStatusOperation(myBackup.BackupTaskId);
                    PeriodicBackupStatus status = null;
                    var value = WaitForValue(() =>
                    {
                        status = store.Maintenance.Send(operation).Status;
                        if (status?.LastEtag == null)
                            return false;

                        return true;
                    }, true);

                    Assert.True(value, $"Got status: {status != null}, exception: {status?.Error?.Exception}, LocalBackup.Exception: {status?.LocalBackup?.Exception}");
                    Assert.True(status != null, $"status != null, exception: {status?.Error?.Exception}, LocalBackup.Exception: {status?.LocalBackup?.Exception}");
                    Assert.True(myBackup.NodeTag == status.NodeTag, $"myBackup.NodeTag({myBackup.NodeTag}) == status.NodeTag({status.NodeTag})");

                    var prePath = Path.Combine(backupPath, myBackup.Guid.ToString());
                    myBackup.BackupPath = Path.Combine(prePath, status.FolderName);
                }

                var dbs = new List<string>();
                foreach (var myBackup in myBackupsList)
                {
                    var name = $"restored_DB1_{myBackup.NodeTag}";
                    var restoreConfig = new RestoreBackupConfiguration
                    {
                        DatabaseName = name,
                        BackupLocation = myBackup.BackupPath
                    };

                    dbs.Add(name);
                    var restoreBackupTask = store.Maintenance.Server.Send(new RestoreBackupOperation(restoreConfig, myBackup.NodeTag));
                    restoreBackupTask.WaitForCompletion(TimeSpan.FromSeconds(30));
                }
                var dbNames = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, int.MaxValue));
                Assert.Equal(clusterSize + 1, dbNames.Length);
                dbs.ForEach(db => Assert.True(dbNames.Contains(db), $"numOfDbs.Contains(db), db = {db}"));
            }
        }

        [Fact]
        public async Task CanPassNodeTagToRestorePatchOperation()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            (List<RavenServer> Nodes, RavenServer Leader) cluster = await CreateRaftCluster(clusterSize, shouldRunInMemory: false);
            var myNodesList = new List<string>();

            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl},
                Database = databaseName,
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                myNodesList.AddRange(databaseResult.Topology.AllNodes);

                var result = store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from doc in docs.Items select new { doc.Name }" },
                    Name = "MyIndex"
                }}));

                var indexResult = result[0];
                await WaitForRaftIndexToBeAppliedOnClusterNodes(indexResult.RaftCommandIndex, cluster.Nodes);

                using (var commands = store.Commands())
                {
                    await commands.PutAsync("items/1", null, new { Name = "testname" }, new Dictionary<string, object>
                    {
                        {Constants.Documents.Metadata.Collection, "Items"}
                    });

                    WaitForIndexingInTheCluster(store, timeout: TimeSpan.FromSeconds(60));

                    Operation operation = await store.Operations.SendAsync(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = "FROM INDEX 'MyIndex' UPDATE { this.NewName = 'NewValue'; } "
                    }));

                    var opStatus = await store.Maintenance.SendAsync(new GetOperationStateOperation(operation.Id));
                    Assert.NotNull(opStatus);

                    foreach (var node in myNodesList)
                    {
                        var op = await store.Maintenance.SendAsync(new GetOperationStateOperation(operation.Id, node));
                        if (node == operation.NodeTag)
                            Assert.NotNull(op);
                        else
                            Assert.Null(op);
                    }

                    await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(60));
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

