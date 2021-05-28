using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Replication
{
    public class ReplicationRevisionsTests : ReplicationTestBase
    {
        public ReplicationRevisionsTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReplicateRevision_WhenSourceDataFromExportAndDocDeleted_ShouldNotRecreateTheDoc()
        {
            var exportFile = GetTempFileName();
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.OperationTimeout)] = "120",
            };
            var (nodes, leader) = await CreateRaftCluster(2, customSettings: settings, watcherCluster: true);
            var nodeTags = nodes.Select(n => n.ServerStore.NodeTag).ToArray();

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 1 }))
            {
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration() }));
                var firstNode = await AssertWaitForNotNullAsync(async () =>
                    (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members?.FirstOrDefault());

                var entity = new User();
                using (var session = store.OpenAsyncSession())
                {
                    //Add first revision with first node tag
                    await session.StoreAsync(entity);
                    await session.SaveChangesAsync();
                }
                await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));

                await WaitAndAssertForValueAsync(
                        async () => (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members?.Count, 2);

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true, nodeTags.First(n => n == firstNode)));

                await WaitAndAssertForValueAsync(
                        async () => (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members?.Count, 1);
                await WaitAndAssertForValueAsync(async () =>
                {
                    var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return dbRecord?.DeletionInProgress == null || dbRecord.DeletionInProgress.Count == 0;
                }, true);

                await store.GetRequestExecutor().UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode { Url = store.Urls.First(), Database = store.Database }));

                using (var session = store.OpenAsyncSession())
                {
                    //Add update revision with second node tag
                    entity.Name = "Changed";
                    await session.StoreAsync(entity);
                    await session.SaveChangesAsync();

                    // Add delete revision with second node tag
                    session.Delete(entity.Id);
                    await session.SaveChangesAsync();
                }

                var operation = await store.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), exportFile);
                await operation.WaitForCompletionAsync();
            }

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 1 }))
            {
                var srcTag = await AssertWaitForNotNullAsync(async () =>
                    (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members.FirstOrDefault());

                var src = nodes.First(n => n.ServerStore.NodeTag == srcTag);
                var dest = nodes.First(n => n.ServerStore.NodeTag != srcTag);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), exportFile);
                await operation.WaitForCompletionAsync();

                using (var session = store.OpenAsyncSession())
                {
                    WaitForIndexing(store, store.Database, nodeTag: src.ServerStore.NodeTag);
                    var firstNodeDocs = await session.Query<User>().ToArrayAsync();
                    Assert.Equal(0, firstNodeDocs.Length);
                }

                var result = await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));
                await WaitAndAssertForValueAsync(async () => (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members?.Count, 2);

                await store.GetRequestExecutor().UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode { Url = store.Urls.First(), Database = store.Database }));

                using var re = RequestExecutor.CreateForSingleNodeWithConfigurationUpdates(dest.WebUrl, store.Database, null, store.Conventions);
                using (var secondSession = store.OpenAsyncSession(new SessionOptions { RequestExecutor = re }))
                {
                    WaitForIndexing(store, store.Database, nodeTag: dest.ServerStore.NodeTag);
                    var secondNodeDocs = await secondSession.Query<User>().ToArrayAsync();
                    Assert.Equal(0, secondNodeDocs.Length);
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ReplicateRevision_WhenSourceDataFromIncrementalBackupAndDocDeleted_ShouldNotRecreateTheDoc()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder", forceCreateDir: true);

            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2 }))
            {
                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration() }));
                var firstNodeTag = await AssertWaitForNotNullAsync(async () =>
                    (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members?.FirstOrDefault());
                var firstNode = nodes.First(n => n.ServerStore.NodeTag == firstNodeTag);
                var secondNode = nodes.First(n => n.ServerStore.NodeTag != firstNodeTag);

                var entity = new User();
                using (var re = RequestExecutor.CreateForSingleNodeWithConfigurationUpdates(firstNode.WebUrl, store.Database, null, store.Conventions))
                using (var session = store.OpenAsyncSession(new SessionOptions { RequestExecutor = re }))
                {
                    //Add first revision with first node tag
                    await session.StoreAsync(entity);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath, incrementalBackupFrequency: "0 * * * *", mentorNode: secondNode.ServerStore.NodeTag);
                var backupTaskId = await Backup.CreateAndRunBackupInClusterAsync(config, store, isFullBackup: true);

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true, firstNodeTag));
                await WaitAndAssertForValueAsync(async () =>
                {
                    var dbRecord = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return dbRecord?.DeletionInProgress == null || dbRecord.DeletionInProgress.Count == 0;
                }, true);

                using (var session = store.OpenAsyncSession())
                {
                    //Add update revision with second node tag
                    entity.Name = "Changed";
                    await session.StoreAsync(entity);
                    await session.SaveChangesAsync();

                    // Add delete revision with second node tag
                    session.Delete(entity.Id);
                    await session.SaveChangesAsync();
                }

                await Backup.RunBackupInClusterAsync(store, backupTaskId, isFullBackup: false);
            }

            using (var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 1 }))
            {
                var srcTag = await AssertWaitForNotNullAsync(async () =>
                    (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members.FirstOrDefault());

                var src = nodes.First(n => n.ServerStore.NodeTag == srcTag);
                var dest = nodes.First(n => n.ServerStore.NodeTag != srcTag);
                await store.Smuggler.ImportIncrementalAsync(new DatabaseSmugglerImportOptions(), Directory.GetDirectories(backupPath).First());

                using (var session = store.OpenAsyncSession())
                {
                    WaitForIndexing(store, store.Database, nodeTag: src.ServerStore.NodeTag);
                    var firstNodeDocs = await session.Query<User>().ToArrayAsync();
                    Assert.Equal(0, firstNodeDocs.Length);
                }

                var result = await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));
                await WaitAndAssertForValueAsync(async () => (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).Topology.Members?.Count, 2);

                await store.GetRequestExecutor().UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(new ServerNode { Url = store.Urls.First(), Database = store.Database }));

                using var re = RequestExecutor.CreateForSingleNodeWithConfigurationUpdates(dest.WebUrl, store.Database, null, store.Conventions);
                using (var secondSession = store.OpenAsyncSession(new SessionOptions { RequestExecutor = re }))
                {
                    WaitForIndexing(store, store.Database, nodeTag: dest.ServerStore.NodeTag);
                    var secondNodeDocs = await secondSession.Query<User>().ToArrayAsync();
                    Assert.Equal(0, secondNodeDocs.Length);
                }
            }
        }
    }
}
