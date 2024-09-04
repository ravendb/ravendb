using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22659 : ClusterTestBase
    {
        public RavenDB_22659(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CanDeleteDatabaseWhenRestoreCancelled()
        {
            var mre = new ManualResetEvent(false);
            var mre2 = new ManualResetEvent(false);
            var backupPath = NewDataPath();
            var db = "NewDatabase";
            //Cluster with 2 nodes
            var (nodes, leader) = await CreateRaftCluster(2);
            await CreateDatabaseInCluster(db, 2, leader.WebUrl);
            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.SaveChangesAsync();
                }

                //Backup
                var backupOperation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                }));
                var result = (BackupResult)await backupOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                //Restore
                var databaseName = $"{store.Database}_Restore";
                RestoreBackupOperation restoreOperation =
                    new RestoreBackupOperation(new RestoreBackupConfiguration
                        { BackupLocation = Path.Combine(backupPath, result.LocalBackup.BackupDirectory), DatabaseName = databaseName });

                foreach (var n in nodes)
                {
                    n.ServerStore.ForTestingPurposesOnly().RestoreDatabaseAfterSavingDatabaseRecord += () =>
                    {
                        mre.Set();
                        mre2.WaitOne(); // Wait to ensure the restore process doesn't finish until we intentionally cancel it.
                    };
                }

                var restoreTask= await store.Maintenance.Server.SendAsync(restoreOperation);

                var nodeToTakeDown = nodes.First(x => x.ServerStore.NodeTag != restoreTask.NodeTag);
                var remainingNode = nodes.First(x => x != nodeToTakeDown);

                var res = mre.WaitOne(TimeSpan.FromSeconds(30)); // Wait to ensure the database record is written before disposing of one node.
                Assert.True(res);

                var disposedNode = await DisposeServerAndWaitForFinishOfDisposalAsync(nodeToTakeDown);
                await restoreTask.KillAsync();
                mre2.Set();

                var database = await GetDatabase(remainingNode, store.Database);
                WaitForValue(() =>
                {
                    var operation = database.ServerStore.Operations.GetOperation(restoreTask.Id);
                    return operation.IsCompleted();
                }, true);

                var revivedNode = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    DataDirectory = disposedNode .DataDirectory,
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = disposedNode .Url
                    }
                });

                nodes.Add(revivedNode);
                Servers.Add(revivedNode);

                using (remainingNode.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var topology = remainingNode.ServerStore.Engine.GetTopology(ctx);
                    Assert.Equal(2, topology.AllNodes.Count);
                }

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CannotDeleteDatabaseWhenRestoreCancelledOnNonResponsibleNode()
        {
            var mre = new ManualResetEvent(false);
            var mre2 = new ManualResetEvent(false);
            var backupPath = NewDataPath();
            var db = "NewDatabase";
            //Cluster with 2 nodes
            var (nodes, leader) = await CreateRaftCluster(2);
            await CreateDatabaseInCluster(db, 2, leader.WebUrl);
            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.SaveChangesAsync();
                }

                //Backup
                var backupOperation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                }));
                var result = (BackupResult)await backupOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                //Restore
                var databaseName = $"{store.Database}_Restore";
                RestoreBackupOperation restoreOperation =
                    new RestoreBackupOperation(new RestoreBackupConfiguration
                    { BackupLocation = Path.Combine(backupPath, result.LocalBackup.BackupDirectory), DatabaseName = databaseName });

                foreach (var n in nodes)
                {
                    n.ServerStore.ForTestingPurposesOnly().RestoreDatabaseAfterSavingDatabaseRecord += () =>
                    {
                        mre.Set();
                        mre2.WaitOne(); // Wait to ensure the restore process doesn't finish until we intentionally cancel it.
                    };
                }

                var restoreTask = await store.Maintenance.Server.SendAsync(restoreOperation);

                var nodeToTakeDown = nodes.First(x => x.ServerStore.NodeTag != restoreTask.NodeTag);
                var remainingNode = nodes.First(x => x != nodeToTakeDown);

                var res = mre.WaitOne(TimeSpan.FromSeconds(30)); // Wait to ensure the database record is written before disposing of one node.
                Assert.True(res);

                var disposedNode = await DisposeServerAndWaitForFinishOfDisposalAsync(nodeToTakeDown);
                await restoreTask.KillAsync();
                mre2.Set();

                var database = await GetDatabase(remainingNode, store.Database);
                WaitForValue(() =>
                {
                    var operation = database.ServerStore.Operations.GetOperation(restoreTask.Id);
                    return operation.IsCompleted();
                }, true);

                var revivedNode = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    DataDirectory = disposedNode.DataDirectory,
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = disposedNode.Url
                    }
                });

                nodes.Add(revivedNode);
                Servers.Add(revivedNode);

                using (remainingNode.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var topology = remainingNode.ServerStore.Engine.GetTopology(ctx);
                    Assert.Equal(2, topology.AllNodes.Count);
                }

                var deleteException = await Assert.ThrowsAsync<RavenException>(async () =>
                {
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: revivedNode.ServerStore.NodeTag));
                });
                Assert.Contains("doesn't reside on node", deleteException.Message);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task CannotDeleteDatabaseInRestore()
        {
            var mre = new ManualResetEvent(false);
            var mre2 = new ManualResetEvent(false);
            var backupPath = NewDataPath();
            var db = "NewDatabase";
            //Cluster with 2 nodes
            var (nodes, leader) = await CreateRaftCluster(2);
            await CreateDatabaseInCluster(db, 2, leader.WebUrl);
            using (var store = new DocumentStore
            {
                Database = db,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Toli" }, "users/1");
                    await session.SaveChangesAsync();
                }

                //Backup
                var backupOperation = await store.Maintenance.SendAsync(new BackupOperation(new BackupConfiguration
                {
                    BackupType = BackupType.Backup,
                    LocalSettings = new LocalSettings
                    {
                        FolderPath = backupPath
                    }
                }));
                var result = (BackupResult)await backupOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                //Restore
                var databaseName = $"{store.Database}_Restore";
                RestoreBackupOperation restoreOperation =
                    new RestoreBackupOperation(new RestoreBackupConfiguration
                    { BackupLocation = Path.Combine(backupPath, result.LocalBackup.BackupDirectory), DatabaseName = databaseName });

                foreach (var n in nodes)
                {
                    n.ServerStore.ForTestingPurposesOnly().RestoreDatabaseAfterSavingDatabaseRecord += () =>
                    {
                        mre.Set();
                        mre2.WaitOne(); // Wait to ensure the restore process doesn't finish until we intentionally cancel it.
                    };
                }

                await store.Maintenance.Server.SendAsync(restoreOperation);

                var res = mre.WaitOne(TimeSpan.FromSeconds(30));
                Assert.True(res);

                var deleteException = await Assert.ThrowsAsync<RavenException>(async () =>
                {
                    await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(databaseName, hardDelete: true));
                });
                Assert.Contains("while the restore process is in progress", deleteException.Message);
                mre2.Set();
            }
        }
    }
}
