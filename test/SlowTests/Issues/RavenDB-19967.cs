using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using SlowTests.Server.Replication;
using Xunit;
using Xunit.Abstractions;
using User = SlowTests.Core.Utils.Entities.User;
using Tests.Infrastructure;

namespace SlowTests.Issues
{
    public class RavenDB_19967 : ReplicationTestBase
    {
        private readonly string _notificationId = AlertRaised.GetKey(AlertType.BlockingTombstones, nameof(AlertType.BlockingTombstones));
        private readonly string _customTaskName = $"Custom task name {Guid.NewGuid()}";

        private const int Timeout = 3000;
        private const int DocumentsCount = 13;
        private const int TombstonesCount = 7;

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
                Assert.True(documentDatabase.NotificationCenter.Exists(_notificationId));

                await store.Maintenance.SendAsync(new EnableIndexOperation(indexName));

                await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                Assert.True(documentDatabase.NotificationCenter.Exists(_notificationId) == false);
            }
        }

        [Fact]
        public async Task TombstoneCleaningAfterIndexDisabled()
        {
            using (var store = GetDocumentStore())
            {
                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                // Documents creation
                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= DocumentsCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Store(new User { Name = $"Yonatan{i}" }, docId);
                    }
                    session.SaveChanges();
                }

                var userIndex = new UserByName();
                string indexName = userIndex.IndexName;
                await userIndex.ExecuteAsync(store);
                await store.Maintenance.SendAsync(new DisableIndexOperation(indexName));

                // When deleting documents, we expect the creation of Tombstones, since Index is disabled.
                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= TombstonesCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Delete(docId);
                    }
                    session.SaveChanges();
                }

                await documentDatabase.TombstoneCleaner.ExecuteCleanup();
                Assert.True(documentDatabase.NotificationCenter.Exists(_notificationId));

                var notificationDetails = documentDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(_notificationId);
                Assert.Equal(1, notificationDetails.Count);
                Assert.Equal(nameof(UserByName), notificationDetails.First().Source);
                Assert.Equal("Users", notificationDetails.First().Collection);
                Assert.Equal(TombstonesCount, notificationDetails.First().NumberOfTombstones);
                Assert.Equal(0, notificationDetails.First().BlockerTaskId);
                Assert.Equal(ITombstoneAware.TombstoneDeletionBlockerType.Index, notificationDetails.First().BlockerType);
                Assert.True(notificationDetails.First().SizeOfTombstonesInBytes > 0,
                    $"Expected the size of tombstones to be greater than 0, but it was {notificationDetails.First().SizeOfTombstonesInBytes}");
            }
        }

        [Fact]
        public async Task TombstoneCleaningAfterReplicationLoaderDisabled()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                var srcDocumentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store1);
                var destDocumentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store2);

                // Documents creation
                var documentCreationTasks = new Task[DocumentsCount];
                using (var session = store1.OpenSession())
                {
                    for (int i = 1; i <= DocumentsCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Store(new User { Name = $"Yonatan{i}" }, docId);
                        documentCreationTasks[i - 1] = Task.Run(() => WaitForDocument(store2, docId, Timeout));
                    }
                    session.SaveChanges();
                }

                // Assert documents replication
                await Task.WhenAll(documentCreationTasks).ContinueWith(_ =>
                {
                    for (int i = 1; i <= DocumentsCount; i++)
                    {
                        var docId = $"users/{i}";
                        Assert.True(documentCreationTasks[i - 1].IsCompletedSuccessfully, $"The document '{docId}' was not received within the timeout in the `Sink` database.");
                    }
                });

                // Setup and subsequent disabling of the replication task
                var externalList = await SetupReplicationAsync(store1, store2);
                await store1.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(externalList.First().TaskId, OngoingTaskType.Replication, disable: true));

                // When deleting documents, we expect the creation of Tombstones, since replication task is disabled.
                using (var session = store1.OpenSession())
                {
                    for (int i = 1; i <= TombstonesCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Delete(docId);
                    }
                    session.SaveChanges();
                }

                await srcDocumentDatabase.TombstoneCleaner.ExecuteCleanup();
                Assert.True(srcDocumentDatabase.NotificationCenter.Exists(_notificationId));

                var notificationDetails = srcDocumentDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(_notificationId);
                Assert.Equal(1, notificationDetails.Count);
                Assert.Equal($"External Replication to ConnectionString-{Server.WebUrl} (DB: {destDocumentDatabase.Name})", notificationDetails.First().Source);
                Assert.Equal("users", notificationDetails.First().Collection);
                Assert.Equal(TombstonesCount, notificationDetails.First().NumberOfTombstones);
                Assert.Equal(externalList.First().TaskId, notificationDetails.First().BlockerTaskId);
                Assert.Equal(ITombstoneAware.TombstoneDeletionBlockerType.ExternalReplication, notificationDetails.First().BlockerType);
                Assert.True(notificationDetails.First().SizeOfTombstonesInBytes > 0,
                    $"Expected the size of tombstones to be greater than 0, but it was {notificationDetails.First().SizeOfTombstonesInBytes}");
            }
        }

        [Fact]
        public async Task TombstoneCleaningAfterPullExternalReplicationDisabled()
        {
            const int sinkTombstonesCount = 7;
            const int hubTombstonesCount = 11;
            var taskName = $"pull replication {Guid.NewGuid()}";

            using (var hub = GetDocumentStore())
            using (var sink = GetDocumentStore())
            {
                var hubCreationTaskId = (await hub.Maintenance.ForDatabase(hub.Database).SendAsync(new PutPullReplicationAsHubOperation(taskName)));
                var sinkCreationTaskId = (await PullReplicationTests.SetupPullReplicationAsync(taskName, sink, hub));

                // Documents creation
                var documentCreationTasks = new Task[DocumentsCount];
                using (var session = hub.OpenSession())
                {
                    for (int i = 1; i <= DocumentsCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Store(new User { Name = $"Lev{i}" }, docId);
                        documentCreationTasks[i - 1] = Task.Run(() => WaitForDocument(sink, docId, Timeout));
                    }
                    session.SaveChanges();
                }

                // Assert documents replication
                await Task.WhenAll(documentCreationTasks).ContinueWith(_ =>
                {
                    for (int i = 1; i <= DocumentsCount; i++)
                    {
                        var docId = $"users/{i}";
                        Assert.True(documentCreationTasks[i - 1].IsCompletedSuccessfully, $"The document '{docId}' was not received within the timeout in the `Sink` database.");
                    }
                });

                // Disable replication tasks
                await sink.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(sinkCreationTaskId.First().TaskId, OngoingTaskType.PullReplicationAsSink, disable: true));
                await hub.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(hubCreationTaskId.TaskId, OngoingTaskType.PullReplicationAsHub, disable: true));

                // When deleting documents, we expect the creation of Tombstones, since replication tasks are disabled.
                using (var session = sink.OpenSession())
                {
                    for (int i = 1; i <= sinkTombstonesCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Delete(docId);
                    }
                    session.SaveChanges();
                }

                using (var session = hub.OpenSession())
                {
                    for (int i = 1; i <= hubTombstonesCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Delete(docId);
                    }
                    session.SaveChanges();
                }

                // Assert 'Sink' BlockingTombstones notification and its details.
                var sinkDatabase = await Databases.GetDocumentDatabaseInstanceFor(sink);
                await sinkDatabase.TombstoneCleaner.ExecuteCleanup();
                Assert.True(sinkDatabase.NotificationCenter.Exists(_notificationId));

                var sinkNotificationDetails = sinkDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(_notificationId);
                Assert.Equal(1, sinkNotificationDetails.Count);
                Assert.Equal("users", sinkNotificationDetails.First().Collection);
                Assert.Contains(taskName, sinkNotificationDetails.First().Source);
                Assert.Equal(sinkTombstonesCount, sinkNotificationDetails.First().NumberOfTombstones);
                Assert.Equal(sinkCreationTaskId.First().TaskId, sinkNotificationDetails.First().BlockerTaskId);
                Assert.Equal(ITombstoneAware.TombstoneDeletionBlockerType.PullReplicationAsSink, sinkNotificationDetails.First().BlockerType);
                Assert.True(sinkNotificationDetails.First().SizeOfTombstonesInBytes > 0,
                    $"Expected the size of tombstones to be greater than 0, but it was {sinkNotificationDetails.First().SizeOfTombstonesInBytes}");

                // Assert 'Hub' BlockingTombstones notification and its details.
                var hubDatabase = await Databases.GetDocumentDatabaseInstanceFor(hub);
                await hubDatabase.TombstoneCleaner.ExecuteCleanup();
                Assert.True(hubDatabase.NotificationCenter.Exists(_notificationId));

                var hubNotificationDetails = hubDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(_notificationId);
                Assert.Equal(1, hubNotificationDetails.Count);
                Assert.Equal("users", hubNotificationDetails.First().Collection);
                Assert.Equal($"{taskName}", hubNotificationDetails.First().Source);
                Assert.Equal(hubTombstonesCount, hubNotificationDetails.First().NumberOfTombstones);
                Assert.Equal(hubCreationTaskId.TaskId, hubNotificationDetails.First().BlockerTaskId);
                Assert.Equal(ITombstoneAware.TombstoneDeletionBlockerType.PullReplicationAsHub, hubNotificationDetails.First().BlockerType);
                Assert.True(hubNotificationDetails.First().SizeOfTombstonesInBytes > 0,
                    $"Expected the size of tombstones to be greater than 0, but it was {hubNotificationDetails.First().SizeOfTombstonesInBytes}");
            }
        }

        [Fact]
        public void CheckForNewEtlTypes()
        {
            var knownEtlTypes = new[]
            {
                EtlType.Raven,
                EtlType.Sql,
                EtlType.Olap,
                EtlType.ElasticSearch,
                EtlType.Queue
                };

            var currentEtlTypes = Enum.GetValues(typeof(EtlType)).Cast<EtlType>();
            var newEtlTypes = currentEtlTypes.Except(knownEtlTypes).ToArray();

            if (newEtlTypes.Any())
                throw new Exception($"New EtlType values detected: {string.Join(", ", newEtlTypes)}. Update {nameof(TombstoneCleaningAfterEtlLoaderDisabled)} test to cover it.");
        }

        [Theory]
        [InlineData(EtlType.Raven)]
        [InlineData(EtlType.Sql)]
        [InlineData(EtlType.Olap)]
        [InlineData(EtlType.ElasticSearch)]
        [InlineData(EtlType.Queue)]
        public async Task TombstoneCleaningAfterEtlLoaderDisabled(EtlType etlType)
        {
            using (var store = GetDocumentStore())
            {
                // Documents creation
                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= DocumentsCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Store(new User { Name = $"Lev{i}" }, docId);
                    }
                    session.SaveChanges();
                }

                var transforms = new Transformation
                {
                    Name = "loadAll",
                    Collections = { "Users" },
                    Script = "loadToUsers(this)"
                };

                long taskId;
                ITombstoneAware.TombstoneDeletionBlockerType blockerType;
                switch (etlType)
                {
                    case EtlType.Raven:
                        var ravenConnectionString = new RavenConnectionString { Name = store.Identifier, Database = store.Database + "-non-existing", TopologyDiscoveryUrls = store.Urls };
                        var ravenConfiguration = new RavenEtlConfiguration { Name = _customTaskName, ConnectionStringName = ravenConnectionString.Name, Transforms = { transforms } };
                        taskId = await AddEtlAndDisableIt(store, ravenConnectionString, ravenConfiguration, OngoingTaskType.RavenEtl);
                        blockerType = ITombstoneAware.TombstoneDeletionBlockerType.RavenEtl;
                        break;
                    case EtlType.Sql:
                        var sqlConnectionString = new SqlConnectionString { Name = store.Identifier, FactoryName = "Microsoft.Data.SqlClient", ConnectionString = "Server=127.0.0.1;Port=2345;Database=myDataBase;User Id=foo;Password=bar;" };
                        var sqlConfiguration = new SqlEtlConfiguration { Name = _customTaskName, ConnectionStringName = sqlConnectionString.Name, Transforms = { transforms }, SqlTables = { new SqlEtlTable { TableName = "Orders", DocumentIdColumn = "Id" } } };
                        taskId = await AddEtlAndDisableIt(store, sqlConnectionString, sqlConfiguration, OngoingTaskType.SqlEtl);
                        blockerType = ITombstoneAware.TombstoneDeletionBlockerType.SqlEtl;
                        break;
                    case EtlType.Olap:
                        var olapConnectionString = new OlapConnectionString { Name = store.Identifier };
                        var olapConfiguration = new OlapEtlConfiguration { Name = _customTaskName, ConnectionStringName = olapConnectionString.Name, Transforms = { transforms } };
                        taskId = await AddEtlAndDisableIt(store, olapConnectionString, olapConfiguration, OngoingTaskType.OlapEtl);
                        blockerType = ITombstoneAware.TombstoneDeletionBlockerType.OlapEtl;
                        break;
                    case EtlType.ElasticSearch:
                        var elasticConnectionString = new ElasticSearchConnectionString { Name = store.Identifier, Nodes = new[] { "http://127.0.0.1:8080" } };
                        var elasticConfiguration = new ElasticSearchEtlConfiguration { Name = _customTaskName, ConnectionStringName = elasticConnectionString.Name, Transforms = { transforms } };
                        taskId = await AddEtlAndDisableIt(store, elasticConnectionString, elasticConfiguration, OngoingTaskType.ElasticSearchEtl);
                        blockerType = ITombstoneAware.TombstoneDeletionBlockerType.ElasticSearchEtl;
                        break;
                    case EtlType.Queue:
                        var queueConnectionString = new QueueConnectionString { Name = store.Identifier, BrokerType = QueueBrokerType.RabbitMq, RabbitMqConnectionSettings = new RabbitMqConnectionSettings { ConnectionString = "test" } };
                        var queueConfiguration = new QueueEtlConfiguration { Name = _customTaskName, ConnectionStringName = queueConnectionString.Name, Transforms = { transforms }, BrokerType = QueueBrokerType.RabbitMq };
                        taskId = await AddEtlAndDisableIt(store, queueConnectionString, queueConfiguration, OngoingTaskType.QueueEtl);
                        blockerType = ITombstoneAware.TombstoneDeletionBlockerType.QueueEtl;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(etlType), etlType, "New EtlType values detected");
                }

                // When deleting documents, we expect the creation of Tombstones, since ETL task is disabled.
                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= TombstonesCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Delete(docId);
                    }
                    session.SaveChanges();
                }

                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                Assert.True(documentDatabase.NotificationCenter.Exists(_notificationId));

                var notificationDetails = documentDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(_notificationId);
                Assert.Equal(1, notificationDetails.Count);
                Assert.Equal("users", notificationDetails.First().Collection);
                Assert.Equal(_customTaskName, notificationDetails.First().Source);
                Assert.Equal(taskId, notificationDetails.First().BlockerTaskId);
                Assert.Equal(blockerType, notificationDetails.First().BlockerType);
                Assert.True(notificationDetails.First().SizeOfTombstonesInBytes > 0,
                    $"Expected the size of tombstones to be greater than 0, but it was {notificationDetails.First().SizeOfTombstonesInBytes}");

                Assert.Equal(TombstonesCount, notificationDetails.First().NumberOfTombstones);
            }

            async Task<long> AddEtlAndDisableIt<T>(IDocumentStore store, T connectionString, EtlConfiguration<T> configuration, OngoingTaskType type) where T : ConnectionString
            {
                var putResult = store.Maintenance.Send(new PutConnectionStringOperation<T>(connectionString));
                Assert.NotNull(putResult.RaftCommandIndex);

                var addResult = store.Maintenance.Send(new AddEtlOperation<T>(configuration));
                await store.Maintenance.SendAsync(new ToggleOngoingTaskStateOperation(addResult.TaskId, type, disable: true));

                return addResult.TaskId;
            }
        }

        [Fact]
        public async Task TombstoneCleaningAfterPeriodicBackupDisabled()
        {
            var backupPath = NewDataPath(suffix: "BackupFolder");

            using (var store = GetDocumentStore())
            {
                // Documents creation
                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= DocumentsCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Store(new User { Name = $"Yonatan{i}" }, docId);
                    }
                    session.SaveChanges();
                }

                var config = Backup.CreateBackupConfiguration(
                    backupPath: backupPath,
                    backupType: BackupType.Backup,
                    incrementalBackupFrequency: "0 0 1 1 *",
                    name: _customTaskName);

                config.TaskId = await Backup.UpdateConfigAndRunBackupAsync(Server, config, store, isFullBackup: true);
                config.Disabled = true;

                var operation = new UpdatePeriodicBackupOperation(config);
                await store.Maintenance.SendAsync(operation);

                // When deleting documents, we expect the creation of Tombstones, since Backup task is disabled.
                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= TombstonesCount; i++)
                    {
                        var docId = $"users/{i}";
                        session.Delete(docId);
                    }
                    session.SaveChanges();
                }

                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                await documentDatabase.TombstoneCleaner.ExecuteCleanup();

                Assert.True(documentDatabase.NotificationCenter.Exists(_notificationId));

                var notificationDetails = documentDatabase.NotificationCenter.TombstoneNotifications.GetNotificationDetails(_notificationId);
                Assert.Equal(1, notificationDetails.Count);
                Assert.Equal("users", notificationDetails.First().Collection);
                Assert.Equal(_customTaskName, notificationDetails.First().Source);
                Assert.Equal(TombstonesCount, notificationDetails.First().NumberOfTombstones);
                Assert.Equal(config.TaskId, notificationDetails.First().BlockerTaskId);
                Assert.Equal(ITombstoneAware.TombstoneDeletionBlockerType.Backup, notificationDetails.First().BlockerType);
                Assert.True(notificationDetails.First().SizeOfTombstonesInBytes > 0,
                    $"Expected the size of tombstones to be greater than 0, but it was {notificationDetails.First().SizeOfTombstonesInBytes}");
            }
        }

        [Fact]
        public async Task TombstoneCleaningAfterErroredIndex()
        {
            using (var store = GetDocumentStore())
            {
                var user = new User
                {
                    Name = "Yonatan",
                    Count = 0
                };
                var documentDatabase = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                var index = new ErroredIndex();
                await index.ExecuteAsync(store);

                var state = await WaitForValueAsync(async () =>
                {
                    var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));
                    return indexStats.State;
                }, IndexState.Error);
                Assert.Equal(IndexState.Error, state);

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
            }
        }

        [Fact]
        public async Task TombstoneCleaningAfterPausedIndex()
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
                await userIndex.ExecuteAsync(store);
                await store.Maintenance.SendAsync(new StopIndexOperation(userIndex.IndexName));

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

        private class ErroredIndex : AbstractIndexCreationTask<User>
        {
            public ErroredIndex()
            {
                Map = users => from user in users
                               select new
                               {
                                   Count = 3 / user.Count
                               };
            }
        }
    }
}
