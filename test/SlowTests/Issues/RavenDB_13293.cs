using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13293 : ReplicationTestBase
    {
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

                    SpinWait.SpinUntil(() =>
                    {
                        getPeriodicBackupResult = store.Maintenance.Send(backupOperation);
                        return getPeriodicBackupResult.Status?.LastEtag > 0;
                    }, TimeSpan.FromSeconds(30));

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
                        BackupLocation = myBackup.BackupPath,
                        NodeTag = myBackup.NodeTag
                    };
                    var restoreBackupTask = store.Maintenance.Server.Send(new RestoreBackupOperation(restoreConfig));
                    restoreBackupTask.WaitForCompletion(TimeSpan.FromSeconds(30));
                }
                var numOfDbs = await store.Maintenance.Server.SendAsync(new GetDatabaseNamesOperation(0, int.MaxValue));
                Assert.Equal(clusterSize + 1, numOfDbs.Length);
            }
        }

        public class MyBackup
        {
            public string BackupPath { get; set; }
            public long BackupTaskId { get; set; }

            public Guid Guid { get; set; }

            public string NodeTag { get; set; }
        }
    }
}

