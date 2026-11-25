using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Commands.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_14128 : RavenTestBase
    {
        private const string IndexName = "Users/ByName";

        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        public RavenDB_14128(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task ShouldNotifyOnlyFeaturesAffectedByDatabaseRecordChange()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var handled = new ConcurrentQueue<(string Feature, long Index)>();
                database.ForTestingPurposesOnly().OnDatabaseRecordChangeHandled = (feature, index) => handled.Enqueue((feature, index));

                handled.Clear();
                var putIndexResults = await store.Maintenance.SendAsync(new PutIndexesOperation(CreateUsersByNameIndex()));
                var raftIndex = putIndexResults[0].RaftCommandIndex;
                await database.RachisLogIndexNotifications.WaitForIndexNotification(raftIndex, Timeout);

                AssertHandledOnly(handled, raftIndex, nameof(DocumentDatabase.IndexStore));
                Assert.NotNull(database.IndexStore.GetIndex(IndexName));
                Assert.Equal(raftIndex, (await GetClusterState(store)).LastIndexesIndex);

                handled.Clear();
                var expirationResult = await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 100 }));
                raftIndex = expirationResult.RaftCommandIndex.Value;
                await database.RachisLogIndexNotifications.WaitForIndexNotification(raftIndex, Timeout);

                AssertHandledOnly(handled, raftIndex);
                Assert.NotNull(database.ExpiredDocumentsCleaner);

                handled.Clear();
                var backupResult = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(Backup.CreateBackupConfiguration(NewDataPath(forceCreateDir: true))));
                raftIndex = backupResult.RaftCommandIndex;
                await database.RachisLogIndexNotifications.WaitForIndexNotification(raftIndex, Timeout);

                AssertHandledOnly(handled, raftIndex, nameof(DocumentDatabase.PeriodicBackupRunner));
                Assert.Equal(1, database.PeriodicBackupRunner.PeriodicBackups.Count);
                Assert.Equal(raftIndex, (await GetClusterState(store)).LastPeriodicBackupsIndex);

                handled.Clear();
                var connectionStringResult = await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = "ExternalDatabase",
                    Database = "ExternalDatabase",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:1" }
                }));
                raftIndex = connectionStringResult.RaftCommandIndex;
                await database.RachisLogIndexNotifications.WaitForIndexNotification(raftIndex, Timeout);

                AssertHandledOnly(handled, raftIndex,
                    nameof(DocumentDatabase.ReplicationLoader),
                    nameof(DocumentDatabase.EtlLoader),
                    nameof(DocumentDatabase.EmbeddingsGeneratorQueries),
                    nameof(DocumentDatabase.EmbeddingsGeneratorEtl),
                    nameof(DocumentDatabase.QueueSinkLoader),
                    nameof(DocumentDatabase.CdcSinkLoader));

                handled.Clear();
                var replicationResult = await store.Maintenance.SendAsync(new UpdateExternalReplicationOperation(new ExternalReplication("ExternalDatabase", "ExternalDatabase")));
                raftIndex = replicationResult.RaftCommandIndex;
                await database.RachisLogIndexNotifications.WaitForIndexNotification(raftIndex, Timeout);

                AssertHandledOnly(handled, raftIndex, nameof(DocumentDatabase.ReplicationLoader));
                Assert.Equal(raftIndex, (await GetClusterState(store)).LastReplicationIndex);
            }
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task ShouldReadOnlySectionsHandledOnEveryChangeWhenNoFeatureIsAffected()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var putIndexResults = await store.Maintenance.SendAsync(new PutIndexesOperation(CreateUsersByNameIndex()));
                await database.RachisLogIndexNotifications.WaitForIndexNotification(putIndexResults[0].RaftCommandIndex, Timeout);

                var backupResult = await store.Maintenance.SendAsync(new UpdatePeriodicBackupOperation(Backup.CreateBackupConfiguration(NewDataPath(forceCreateDir: true))));
                await database.RachisLogIndexNotifications.WaitForIndexNotification(backupResult.RaftCommandIndex, Timeout);

                var reads = new ConcurrentQueue<bool>();
                database.ForTestingPurposesOnly().OnDatabaseRecordReadForStateChange = withFeatureSections => reads.Enqueue(withFeatureSections);

                var expirationResult = await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 100 }));
                await database.RachisLogIndexNotifications.WaitForIndexNotification(expirationResult.RaftCommandIndex.Value, Timeout);

                Assert.Contains(false, reads);
                Assert.Equal(100, database.ExpiredDocumentsCleaner.ExpirationConfiguration.DeleteFrequencyInSec);

                await store.Maintenance.SendAsync(new PutClientConfigurationOperation(new ClientConfiguration { MaxNumberOfRequestsPerSession = 50 }));
                Assert.Equal(50, await WaitForValueAsync(() => database.ClientConfiguration?.MaxNumberOfRequestsPerSession, 50));

                // the features kept what they handled before
                Assert.NotNull(database.IndexStore.GetIndex(IndexName));
                Assert.Equal(1, database.PeriodicBackupRunner.PeriodicBackups.Count);

                reads.Clear();
                var secondIndex = CreateUsersByNameIndex();
                secondIndex.Name = "Users/ByName2";
                putIndexResults = await store.Maintenance.SendAsync(new PutIndexesOperation(secondIndex));
                await database.RachisLogIndexNotifications.WaitForIndexNotification(putIndexResults[0].RaftCommandIndex, Timeout);

                Assert.Contains(true, reads);
                Assert.NotNull(database.IndexStore.GetIndex(IndexName));
                Assert.NotNull(database.IndexStore.GetIndex(secondIndex.Name));
                Assert.Equal(1, database.PeriodicBackupRunner.PeriodicBackups.Count);
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public async Task ShouldApplyIndexChangesThatFailedOnNextDatabaseRecordChange()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var failIndexStart = 1;
                database.IndexStore.ForTestingPurposesOnly().BeforeIndexStart = index =>
                {
                    if (index.Name == IndexName && Interlocked.Exchange(ref failIndexStart, 0) == 1)
                        throw new InvalidOperationException("Simulated failure of the index start");
                };

                var e = await Assert.ThrowsAnyAsync<Exception>(() => store.Maintenance.SendAsync(new PutIndexesOperation(CreateUsersByNameIndex())));
                Assert.Contains("Simulated failure of the index start", e.ToString());
                Assert.Null(database.IndexStore.GetIndex(IndexName));

                var handled = new ConcurrentQueue<(string Feature, long Index)>();
                database.ForTestingPurposesOnly().OnDatabaseRecordChangeHandled = (feature, index) => handled.Enqueue((feature, index));

                // the index store didn't apply all the changes it was notified about, so it handles the next change of the record, even an unrelated one
                var expirationResult = await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 100 }));
                var raftIndex = expirationResult.RaftCommandIndex.Value;
                await database.RachisLogIndexNotifications.WaitForIndexNotification(raftIndex, Timeout);

                Assert.Contains((nameof(DocumentDatabase.IndexStore), raftIndex), handled);
                Assert.NotNull(database.IndexStore.GetIndex(IndexName));

                // once it applied them, the unrelated changes are skipped again
                handled.Clear();
                expirationResult = await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 200 }));
                raftIndex = expirationResult.RaftCommandIndex.Value;
                await database.RachisLogIndexNotifications.WaitForIndexNotification(raftIndex, Timeout);

                AssertHandledOnly(handled, raftIndex);
            }
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task ShouldHandleChangeOfNotificationThatWasSkipped()
        {
            using (var store = GetDocumentStore())
            using (var putIndexNotificationArrived = new ManualResetEventSlim())
            using (var releasePutIndexNotification = new ManualResetEventSlim())
            {
                var database = await GetDatabase(store.Database);

                var testingStuff = Server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly();
                testingStuff.InsideHandleClusterDatabaseChanged = type =>
                {
                    if (type != nameof(PutIndexCommand) && type != nameof(PutIndexesCommand))
                        return;

                    putIndexNotificationArrived.Set();
                    releasePutIndexNotification.Wait(Timeout);
                };

                Task<PutIndexResult[]> putIndexTask;

                try
                {
                    putIndexTask = store.Maintenance.SendAsync(new PutIndexesOperation(CreateUsersByNameIndex()));
                    Assert.True(putIndexNotificationArrived.Wait(Timeout));

                    // the notification about a newer and unrelated change is handled first,
                    // so the notification about the index will be skipped but the index must be created anyway
                    var expirationResult = await store.Maintenance.SendAsync(new ConfigureExpirationOperation(new ExpirationConfiguration { Disabled = false, DeleteFrequencyInSec = 100 }));
                    await database.RachisLogIndexNotifications.WaitForIndexNotification(expirationResult.RaftCommandIndex.Value, Timeout);

                    Assert.NotNull(database.IndexStore.GetIndex(IndexName));
                }
                finally
                {
                    testingStuff.InsideHandleClusterDatabaseChanged = null;
                    releasePutIndexNotification.Set();
                }

                await putIndexTask;

                Assert.NotNull(database.IndexStore.GetIndex(IndexName));
            }
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task ShouldIgnoreClusterStateThatCameWithTheDatabaseRecord()
        {
            using (var store = GetDocumentStore())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                // e.g. a record of a database from another cluster that has much higher raft indexes
                const long bogusIndex = long.MaxValue / 2;
                record.ClusterState = new DatabaseRecordClusterState
                {
                    LastTopologyIndex = bogusIndex,
                    LastIndexesIndex = bogusIndex,
                    LastSortersIndex = bogusIndex,
                    LastAnalyzersIndex = bogusIndex,
                    LastReplicationIndex = bogusIndex,
                    LastConnectionStringsIndex = bogusIndex,
                    LastEtlsIndex = bogusIndex,
                    LastEmbeddingsGenerationsIndex = bogusIndex,
                    LastPeriodicBackupsIndex = bogusIndex,
                    LastQueueSinksIndex = bogusIndex,
                    LastCdcSinksIndex = bogusIndex
                };

                var updateResult = await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, record.Etag));
                await Server.ServerStore.Cluster.WaitForIndexNotification(updateResult.RaftCommandIndex);

                var clusterState = await GetClusterState(store);
                Assert.Equal(updateResult.RaftCommandIndex, clusterState.LastIndexesIndex);
                Assert.Equal(updateResult.RaftCommandIndex, clusterState.LastTopologyIndex);

                await store.Maintenance.SendAsync(new PutIndexesOperation(CreateUsersByNameIndex()));

                var database = await GetDatabase(store.Database);
                Assert.NotNull(database.IndexStore.GetIndex(IndexName));
            }
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task ShouldNotifyOnlyFeaturesAffectedByValueChange()
        {
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore())
            {
                var database = await GetDatabase(src.Database);

                var handled = new ConcurrentQueue<(string Feature, long Index)>();
                database.ForTestingPurposesOnly().OnValueChangeHandled = (feature, index) => handled.Enqueue((feature, index));

                // cluster identities aren't relevant for any feature
                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe" }, "users|");
                    await session.SaveChangesAsync();
                }

                var subscriptionName = await src.Subscriptions.CreateAsync<User>();
                Assert.True(await WaitForValueAsync(() => handled.Any(x => x.Feature == nameof(DocumentDatabase.SubscriptionStorage)), true));

                await using (var worker = src.Subscriptions.GetSubscriptionWorker<User>(subscriptionName))
                {
                    var batchProcessed = new AsyncManualResetEvent();
                    _ = worker.Run(_ => batchProcessed.Set());

                    Assert.True(await batchProcessed.WaitAsync(Timeout));
                }

                Assert.All(handled, x => Assert.Equal(nameof(DocumentDatabase.SubscriptionStorage), x.Feature));

                Etl.AddEtl(src, dst, "Users", script: null);
                Assert.True(WaitForDocument<User>(dst, "users/1", u => u.Name == "Joe", (int)Timeout.TotalMilliseconds));
                Assert.True(await WaitForValueAsync(() => handled.Any(x => x.Feature == nameof(DocumentDatabase.EtlLoader)), true));

                var etlIndexes = handled.Where(x => x.Feature == nameof(DocumentDatabase.EtlLoader)).Select(x => x.Index).ToHashSet();
                Assert.DoesNotContain(handled, x => x.Feature == nameof(DocumentDatabase.SubscriptionStorage) && etlIndexes.Contains(x.Index));
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task OrchestratorShouldHandleOnlyRelevantChanges()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var orchestrator = Sharding.GetOrchestrator(store.Database);

                var recordChangeHandled = new ConcurrentQueue<(string Feature, long Index)>();
                var valueChangeHandled = new ConcurrentQueue<(string Feature, long Index)>();

                var testingStuff = orchestrator.ForTestingPurposesOnly();
                testingStuff.OnDatabaseRecordChangeHandled = (feature, index) => recordChangeHandled.Enqueue((feature, index));
                testingStuff.OnValueChangeHandled = (feature, index) => valueChangeHandled.Enqueue((feature, index));

                var subscriptionName = await store.Subscriptions.CreateAsync<User>();
                Assert.True(await WaitForValueAsync(() => valueChangeHandled.Any(x => x.Feature == nameof(ShardedDatabaseContext.SubscriptionsStorage)), true));
                Assert.DoesNotContain(recordChangeHandled, x => x.Feature == nameof(ShardedDatabaseContext.Indexes));

                recordChangeHandled.Clear();
                var putIndexResults = await store.Maintenance.SendAsync(new PutIndexesOperation(CreateUsersByNameIndex()));
                var raftIndex = putIndexResults[0].RaftCommandIndex;

                Assert.True(await WaitForValueAsync(() => recordChangeHandled.Any(x => x.Feature == nameof(ShardedDatabaseContext.Indexes)), true));
                Assert.DoesNotContain(recordChangeHandled, x => x.Feature == nameof(ShardedDatabaseContext.SubscriptionsStorage) && x.Index == raftIndex);
                Assert.NotNull(orchestrator.Indexes.GetIndex(IndexName));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe" }, "users/1");
                    await session.SaveChangesAsync();
                }

                await using (var worker = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionName))
                {
                    var batchProcessed = new AsyncManualResetEvent();
                    _ = worker.Run(_ => batchProcessed.Set());

                    Assert.True(await batchProcessed.WaitAsync(Timeout));
                }

                Assert.DoesNotContain(recordChangeHandled, x => x.Feature == nameof(ShardedDatabaseContext.SubscriptionsStorage) && x.Index == raftIndex);
            }
        }

        private static IndexDefinition CreateUsersByNameIndex()
        {
            return new IndexDefinition
            {
                Name = IndexName,
                Maps = { "from u in docs.Users select new { u.Name }" }
            };
        }

        private static async Task<DatabaseRecordClusterState> GetClusterState(IDocumentStore store)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            return record.ClusterState;
        }

        private static void AssertHandledOnly(ConcurrentQueue<(string Feature, long Index)> handled, long raftIndex, params string[] expectedFeatures)
        {
            // a feature might have handled the change already, while handling a forced notification (e.g. database initialization)
            var handledFeatures = handled.Select(x => x.Feature).ToHashSet();
            foreach (var feature in expectedFeatures)
                Assert.Contains(feature, handledFeatures);

            var handledForIndex = handled.Where(x => x.Index == raftIndex).Select(x => x.Feature).ToHashSet();
            Assert.Empty(handledForIndex.Except(expectedFeatures));
        }
    }
}
