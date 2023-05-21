using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using User = SlowTests.Core.Utils.Entities.User;
using Raven.Server.Documents;

namespace SlowTests.Issues
{
    public class RavenDB_19967 : ReplicationTestBase
    {
        public RavenDB_19967(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task DismissTombstoneNotification()
        {
            using (var store = GetDocumentStore())
            {
                var user = new User { Name = "Yonatan" };
                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var userIndex = new UserByName();
                string indexName = userIndex.IndexName;
                await userIndex.ExecuteAsync(store);
                await store.Maintenance.SendAsync(new DisableIndexOperation(indexName));

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(user.Id);
                    await session.SaveChangesAsync();
                }

                await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                var notificationId = AlertRaised.GetKey(AlertType.BlockingTombstones, nameof(AlertType.BlockingTombstones));
                Assert.True(documentDatabase.NotificationCenter.Exists(notificationId));

                await store.Maintenance.SendAsync(new EnableIndexOperation(indexName));

                await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                Assert.True(documentDatabase.NotificationCenter.Exists(notificationId) == false);
            }
        }

        [Fact]
        public async Task TombstoneCleaningAfterIndexDisabled()
        {
            using (var store = GetDocumentStore())
            {
                var user = new User { Name = "Yonatan" };
                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var userIndex = new UserByName();
                string indexName = userIndex.IndexName;
                await userIndex.ExecuteAsync(store);
                await store.Maintenance.SendAsync(new DisableIndexOperation(indexName));

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(user.Id);
                    await session.SaveChangesAsync();
                }

                await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                var notificationId = AlertRaised.GetKey(AlertType.BlockingTombstones, nameof(AlertType.BlockingTombstones));
                Assert.True(documentDatabase.NotificationCenter.Exists(notificationId));

                var blockingTombstonesDetails = documentDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(notificationId);
                var detail = blockingTombstonesDetails[0];

                Assert.Equal(1, detail.NumberOfTombstones);
                Assert.Equal(DocumentsStorage.TombstonesCount.TombstonesAccuracy.Exact, detail.Accuracy);
            }
        }

        [Fact]
        public async Task TombstoneCleaningMoreThan10K_AfterIndexDisabled()
        {
            using (var store = GetDocumentStore())
            {
                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10_001; i++)
                    {
                        var user = new User { Name = "Yonatan", Id = $"{i}"};
                        await session.StoreAsync(user);
                    }

                    await session.SaveChangesAsync();
                }

                var userIndex = new UserByName();
                string indexName = userIndex.IndexName;
                await userIndex.ExecuteAsync(store);
                await store.Maintenance.SendAsync(new DisableIndexOperation(indexName));

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10_001; i++)
                    {
                        session.Delete($"{i}");
                    }

                    await session.SaveChangesAsync();
                }

                await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                var notificationId = AlertRaised.GetKey(AlertType.BlockingTombstones, nameof(AlertType.BlockingTombstones));
                Assert.True(documentDatabase.NotificationCenter.Exists(notificationId));

                var blockingTombstonesDetails = documentDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(notificationId);
                var detail = blockingTombstonesDetails[0];

                Assert.Equal(10_000, detail.NumberOfTombstones);
                Assert.Equal(DocumentsStorage.TombstonesCount.TombstonesAccuracy.MoreThan, detail.Accuracy);
            }
        }

        [Fact]
        public async Task TombstoneCleaningAfterReplicationLoaderDisabled()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var user = new User { Name = "Yonatan" };
                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store1);
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var externalList = await SetupReplicationAsync(store1, store2);
                WaitForDocumentToReplicate<User>(store2, user.Id, 3000);
                await store1.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(externalList.First().TaskId, OngoingTaskType.Replication, disable: true));

                using (var session = store1.OpenAsyncSession())
                {
                    session.Delete(user.Id);
                    await session.SaveChangesAsync();
                }

                await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                var notificationId = AlertRaised.GetKey(AlertType.BlockingTombstones, nameof(AlertType.BlockingTombstones));
                Assert.True(documentDatabase.NotificationCenter.Exists(notificationId));

                var blockingTombstonesDetails = documentDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(notificationId);
                var detail = blockingTombstonesDetails[0];

                Assert.Equal(1, detail.NumberOfTombstones);
                Assert.Equal(DocumentsStorage.TombstonesCount.TombstonesAccuracy.Exact, detail.Accuracy);
            }
        }

        [Fact]
        public async Task TombstoneCleaningAfterEtlLoaderDisabled()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var user = new User { Name = "Yonatan"};
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var connectionString = new RavenConnectionString
                {
                    Name = store2.Identifier,
                    Database = store2.Database,
                    TopologyDiscoveryUrls = store2.Urls
                };

                await store1.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                var etlConfiguration = new RavenEtlConfiguration
                {
                    ConnectionStringName = connectionString.Name,
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "loadAll",
                            Collections = {"Users"},
                            Script = "loadToUsers(this)"
                        }
                    }
                };

                var result = await store1.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(etlConfiguration));
                Assert.True(WaitForDocument(store2, user.Id));

                await store1.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(result.TaskId, OngoingTaskType.RavenEtl, disable: true));
                using (var session = store1.OpenAsyncSession())
                {
                    session.Delete(user.Id);
                    await session.SaveChangesAsync();
                }

                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store1);
                await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                var notificationId = AlertRaised.GetKey(AlertType.BlockingTombstones, nameof(AlertType.BlockingTombstones));
                Assert.True(documentDatabase.NotificationCenter.Exists(notificationId));

                var blockingTombstonesDetails = documentDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(notificationId);
                var detail = blockingTombstonesDetails[0];

                Assert.Equal(1, detail.NumberOfTombstones);
                Assert.Equal(DocumentsStorage.TombstonesCount.TombstonesAccuracy.Exact, detail.Accuracy);
            }
        }

        [Fact]
        public async Task TombstoneCleaningAfterPeriodicBackupDisabled()
        {
            using (var store = GetDocumentStore())
            {
                var user = new User { Name = "Yonatan"};
                var user2 = new User { Name = "Yonatan2"};
                var backupPath = NewDataPath(suffix: "BackupFolder");
                using (var session = store.OpenAsyncSession())
                { 
                    await session.StoreAsync(user);
                    await session.StoreAsync(user2);
                    await session.SaveChangesAsync();
                }

                var config = Backup.CreateBackupConfiguration(backupPath: backupPath, backupType: BackupType.Backup, incrementalBackupFrequency: "0 0 1 1 *");
                var backupTaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store, isFullBackup: true);

                config.TaskId = backupTaskId;
                config.Disabled = true;
                var operation = new UpdatePeriodicBackupOperation(config);
                await store.Maintenance.SendAsync(operation);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(user.Id);
                    session.Delete(user2.Id);
                    await session.SaveChangesAsync();
                }

                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                var notificationId = AlertRaised.GetKey(AlertType.BlockingTombstones, nameof(AlertType.BlockingTombstones));
                Assert.True(documentDatabase.NotificationCenter.Exists(notificationId));

                var blockingTombstonesDetails = documentDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(notificationId);
                var detail = blockingTombstonesDetails[0];

                Assert.Equal(2, detail.NumberOfTombstones);
                Assert.Equal(DocumentsStorage.TombstonesCount.TombstonesAccuracy.Exact, detail.Accuracy);
            }
        }

        private class UserByName : AbstractIndexCreationTask<User>
        {
            public UserByName()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }
    }
}
