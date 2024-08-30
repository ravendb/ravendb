using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly ShardingTestBase Sharding;

    public class ShardingTestBase
    {
        public readonly ShardedBackupTestBase Backup;
        public readonly ShardedSubscriptionTestBase Subscriptions;
        public readonly ShardedEtlTestBase Etl;
        public readonly ReshardingTestBase Resharding;
        public readonly ShardedReplicationTestBase Replication;

        private readonly RavenTestBase _parent;

        public ShardingTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            Backup = new ShardedBackupTestBase(_parent);
            Resharding = new ReshardingTestBase(_parent);
            Subscriptions = new ShardedSubscriptionTestBase(_parent);
            Replication = new ShardedReplicationTestBase(_parent);
            Etl = new ShardedEtlTestBase(_parent);
        }

        public DocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null, Dictionary<int, DatabaseTopology> shards = null)
        {
            var shardedOptions = options ?? new Options();
            shardedOptions.ModifyDatabaseRecord += r =>
            {
                r.Sharding ??= new ShardingConfiguration();

                if (shards == null && r.Sharding.Shards == null)
                {
                    r.Sharding.Shards = new Dictionary<int, DatabaseTopology>()
                    {
                        {0, new DatabaseTopology()},
                        {1, new DatabaseTopology()},
                        {2, new DatabaseTopology()},
                    };
                } else if (shards != null)
                {
                    r.Sharding.Shards = shards;
                }

            };
            return _parent.GetDocumentStore(shardedOptions, caller);
        }

        public Options GetOptionsForCluster(RavenServer leader, int shards, int shardReplicationFactor, int orchestratorReplicationFactor, bool dynamicNodeDistribution = false)
        {
            var options = new Options
            {
                DatabaseMode = RavenDatabaseMode.Sharded,
                ModifyDatabaseRecord = r =>
                {
                    r.Sharding = new ShardingConfiguration
                    {
                        Shards = new Dictionary<int, DatabaseTopology>(shards),
                        Orchestrator = new OrchestratorConfiguration
                        {
                            Topology = new OrchestratorTopology
                            {
                                DynamicNodesDistribution = dynamicNodeDistribution,
                                ReplicationFactor = orchestratorReplicationFactor
                            }
                        }
                    };

                    for (int shardNumber = 0; shardNumber < shards; shardNumber++)
                    {
                        r.Sharding.Shards[shardNumber] = new DatabaseTopology
                        {
                            ReplicationFactor = shardReplicationFactor,
                            DynamicNodesDistribution = dynamicNodeDistribution
                        };
                    }
                },
                ReplicationFactor = shardReplicationFactor, // this ensures not to use the same path for the replicas
                Server = leader
            };
            options.AddToDescription($"{nameof(RavenDataAttribute.DatabaseMode)} = {nameof(RavenDatabaseMode.Sharded)}");
            return options;
        }

        public static int GetNextSortedShardNumber(PrefixedShardingSetting prefixedShardingSetting, int shardNumber)
        {
            var shardsSorted = prefixedShardingSetting.Shards.OrderBy(x => x).ToArray();
            return GetNextSortedShardNumber(shardNumber, shardsSorted);
        }

        public static int GetNextSortedShardNumber(Dictionary<int, DatabaseTopology> shards, int shardNumber)
        {
            var shardsSorted = shards.Keys.OrderBy(x => x).ToArray();
            return GetNextSortedShardNumber(shardNumber, shardsSorted);
        }

        private static int GetNextSortedShardNumber(int shardNumber, int[] shardsSorted)
        {
            var toShard = -1;
            for (int i = 0; i < shardsSorted.Length; i++)
            {
                if (shardsSorted[i] == shardNumber)
                {
                    if (i + 1 < shardsSorted.Length)
                    {
                        toShard = shardsSorted[i + 1];
                    }
                    else
                    {
                        toShard = shardsSorted[0];
                    }

                    break;
                }
            }

            if (shardNumber == -1)
                throw new ArgumentException($"Shard number {shardNumber} doesn't exist in the database record.");

            return toShard;
        }

        public async ValueTask<string> GetShardDatabaseNameForDocAsync(IDocumentStore store, string docId, string databaseName = null)
        {
            var shard = await GetShardNumberForAsync(store, docId);
            return ShardHelper.ToShardName(databaseName ?? store.Database, shard);
        }

        public ShardingConfiguration GetShardingConfiguration(IDocumentStore store, string database = null)
        {
            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(database ?? store.Database));
            return record.Sharding;
        }

        public async Task<ShardingConfiguration> GetShardingConfigurationAsync(IDocumentStore store, string database = null)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database ?? store.Database));
            return record.Sharding;
        }

        public string GetRandomIdForShard(ShardingConfiguration config, int shardNumber)
        {
            var tries = 100;
            using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            while (tries > 0)
            {
                var id = $"foo/{Random.Shared.Next()}";
                if (ShardHelper.GetShardNumberFor(config, allocator, id) == shardNumber)
                    return id;
                tries--;
            }

            throw new InvalidOperationException($"Have no luck! couldn't randomize an id for shard {shardNumber}");
        }

        public int GetBucket(ShardingConfiguration config, string id)
        {
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
                return ShardHelper.GetBucketFor(config, allocator, id);
        }

        public async Task<int> GetBucketAsync(IDocumentStore store, string id)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            return GetBucket(record.Sharding, id);
        }

        public async Task<int> GetShardNumberForAsync(IDocumentStore store, string id)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
                return ShardHelper.GetShardNumberFor(record.Sharding, allocator, id);
        }

        public async Task<ShardedDocumentDatabase> GetShardedDocumentDatabaseForBucketAsync(string database, int bucket)
        {
            using (_parent.Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var config = _parent.Server.ServerStore.Cluster.ReadShardingConfiguration(context, database);
                var shardNumber = ShardHelper.GetShardNumberFor(config, bucket);
                var shardedName = ShardHelper.ToShardName(database, shardNumber);
                var shardedDatabase = (await _parent.Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(shardedName)) as ShardedDocumentDatabase;
                Assert.NotNull(shardedDatabase);
                return shardedDatabase;
            }
        }

        public ShardedDatabaseContext GetOrchestrator(string database, RavenServer server = null)
        {
            if ((server ?? _parent.Server).ServerStore.DatabasesLandlord.ShardedDatabasesCache.TryGetValue(database, out var task) == false)
                throw new InvalidOperationException($"The orchestrator for '{database}' wasn't found on this node");

            return task.Result;
        }

        public ShardedDatabaseContext GetOrchestratorInCluster(string database, List<RavenServer> servers)
        {
            ShardedDatabaseContext orchestrator = null;
            foreach (var server in servers)
            {
                try
                {
                    orchestrator = GetOrchestrator(database, server);
                }
                catch (InvalidOperationException)
                {
                    // expected
                }
            }

            Assert.NotNull(orchestrator);
            return orchestrator;
        }

        public List<ShardedDatabaseContext> GetOrchestratorsInCluster(string database, List<RavenServer> servers)
        {
            var orchestrators = new List<ShardedDatabaseContext>();
            ShardedDatabaseContext orchestrator = null;
            foreach (var server in servers)
            {
                try
                {
                    orchestrator = GetOrchestrator(database, server);
                }
                catch (InvalidOperationException)
                {
                    // expected
                }
            }

            Assert.NotNull(orchestrator);
            orchestrators.Add(orchestrator);
            return orchestrators;
        }

        public async Task WaitForOrchestratorsToUpdate(string database, long index)
        {
            var servers = _parent.GetServers();
            foreach (var server in servers)
            {
                if (server.ServerStore.DatabasesLandlord.ShardedDatabasesCache.TryGetValue(database, out var task))
                {
                    var orchestrator = await task;
                    await orchestrator.RachisLogIndexNotifications.WaitForIndexNotification(index, TimeSpan.FromSeconds(10));
                }
            }
        }

        public async ValueTask<IAsyncEnumerable<ShardedDocumentDatabase>> GetShardsDocumentDatabaseInstancesForDocId(IDocumentStore store, string docId, List<RavenServer> servers = null)
        {
            var shardDatabaseName = await GetShardDatabaseNameForDocAsync(store, docId);
            return GetShardsDocumentDatabaseInstancesFor(shardDatabaseName, servers);
        }

        public IAsyncEnumerable<ShardedDocumentDatabase> GetShardsDocumentDatabaseInstancesFor(IDocumentStore store, List<RavenServer> servers = null)
        {
            return GetShardsDocumentDatabaseInstancesFor(store.Database, servers);
        }

        public async IAsyncEnumerable<ShardedDocumentDatabase> GetShardsDocumentDatabaseInstancesFor(string database, List<RavenServer> servers = null)
        {
            servers ??= _parent.GetServers();
            foreach (var server in servers.Where(s => s.Disposed == false))
            {
                foreach (var task in server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database))
                {
                    var databaseInstance = await task;
                    Debug.Assert(databaseInstance != null, $"The requested database '{database}' is null, probably you try to loaded sharded database without the $");
                    yield return databaseInstance;
                }
            }
        }

        public async Task<string> GetNotificationInfoAsync(string databaseName, List<RavenServer> nodes)
        {
            var sb = new StringBuilder();
            sb.AppendLine("Shards RachisLogIndexNotifications:");
            var shards = GetShardsDocumentDatabaseInstancesFor(databaseName, nodes);
            await foreach (var shard in shards)
            {
                sb.AppendLine($"Node {shard.ServerStore.NodeTag} Shard {shard.ShardNumber}");
                sb.AppendLine(shard.RachisLogIndexNotifications.PrintLastNotifications());
            }
            return sb.ToString();
        }

        public async ValueTask<ShardedDocumentDatabase> GetAnyShardDocumentDatabaseInstanceFor(string shardDatabase, List<RavenServer> servers = null)
        {
            if (ShardHelper.IsShardName(shardDatabase) == false)
            {
                throw new ArgumentException($"database name {shardDatabase} has to contain $ in order to get its instance.");
            }

            servers ??= _parent.GetServers();
            foreach (var server in servers)
            {
                foreach (var task in server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(shardDatabase))
                {
                    if(await task == null)
                        continue;

                    return task.Result;
                }
            }

            return null;
        }

        public async ValueTask<DatabaseStatistics> GetDatabaseStatisticsAsync(IDocumentStore store, string database = null, DatabaseRecord record = null, List<RavenServer> servers = null)
        {
            var shardingConfiguration = record != null ? record.Sharding : await GetShardingConfigurationAsync(store, database);
            DatabaseStatistics combined = new DatabaseStatistics();
            var essential = await store.Maintenance.SendAsync(new GetEssentialStatisticsOperation());

            combined.CountOfConflicts = essential.CountOfConflicts;
            combined.CountOfCounterEntries = essential.CountOfCounterEntries;
            combined.CountOfDocuments = essential.CountOfDocuments;
            combined.CountOfDocumentsConflicts = essential.CountOfDocumentsConflicts;
            combined.CountOfRevisionDocuments = essential.CountOfRevisionDocuments;
            combined.CountOfTimeSeriesSegments = essential.CountOfTimeSeriesSegments;
            combined.CountOfTombstones = essential.CountOfTombstones;
            combined.CountOfAttachments = essential.CountOfAttachments;

            var uniqueAttachments = new HashSet<string>();
            foreach (var shardNumber in shardingConfiguration.Shards.Keys)
            {
                var db = await GetAnyShardDocumentDatabaseInstanceFor(ShardHelper.ToShardName(database ?? store.Database, shardNumber), servers);
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var hash in db.DocumentsStorage.AttachmentsStorage.GetAllAttachmentsStreamHashes(context))
                    {
                        uniqueAttachments.Add(hash);
                    }
                }
            }

            combined.CountOfUniqueAttachments = uniqueAttachments.Count;
            return combined;
        }

        public bool AllShardHaveDocs(IDictionary<string, List<DocumentDatabase>> servers, long count = 1L)
        {
            foreach (var kvp in servers)
            {
                foreach (var documentDatabase in kvp.Value)
                {
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var ids = documentDatabase.DocumentsStorage.GetNumberOfDocuments(context);
                        if (ids < count)
                            return false;
                    }
                }
            }

            return true;
        }

        public long GetDocsCountForCollectionInAllShards(IDictionary<string, List<DocumentDatabase>> servers, string collection)
        {
            var sum = 0L;
            foreach (var kvp in servers)
            {
                foreach (var documentDatabase in kvp.Value)
                {
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var ids = documentDatabase.DocumentsStorage.GetCollectionDetails(context, collection).CountOfDocuments;
                        sum += ids;
                    }
                }
            }

            return sum;
        }

        internal async Task<ShardedOngoingTasksHandlerProcessorForGetOngoingTasks> InstantiateShardedOutgoingTaskProcessor(string name, RavenServer server)
        {
            Assert.True(server.ServerStore.DatabasesLandlord.ShardedDatabasesCache.TryGetValue(name, out var db));
            var database = await db;
            var handler = new ShardedOngoingTasksHandler();
            var ctx = new RequestHandlerContext { RavenServer = server, DatabaseContext = database, HttpContext = new DefaultHttpContext() };
            handler.InitForOfflineOperation(ctx);
            return new ShardedOngoingTasksHandlerProcessorForGetOngoingTasks(handler);
        }

        public async Task<bool> AllShardHaveDocsAsync(RavenServer server, string databaseName, long count = 1L)
        {
            var databases = server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(databaseName);
            foreach (var task in databases)
            {
                var documentDatabase = await task;
                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var ids = documentDatabase.DocumentsStorage.GetNumberOfDocuments(context);
                    if (ids < count)
                        return false;
                }
            }

            return true;
        }

        public async Task<Dictionary<int, string>> GetOneDocIdForEachShardAsync(RavenServer server, string databaseName)
        {
            var docIdPerShard = new Dictionary<int, string>();
            var databases = server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(databaseName);
            foreach (var task in databases)
            {
                var documentDatabase = await task;
                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var ids = documentDatabase.DocumentsStorage.GetNumberOfDocuments(context);
                    if (ids < 1)
                        return null;

                    var randomId = documentDatabase.DocumentsStorage.GetAllIds(context).FirstOrDefault();
                    if (randomId == null)
                        return null;

                    docIdPerShard.Add(documentDatabase.ShardNumber, randomId);
                }
            }

            return docIdPerShard;
        }

        
        public async Task EnsureNoReplicationLoopForShardingAsync(RavenServer server, string database)
        {
            // wait for the replication ping-pong to settle down
            await Task.Delay(TimeSpan.FromSeconds(3));

            var stores = server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database);

            foreach (var store in stores)
            {
                var storage = await store;

                await _parent.Replication.EnsureNoReplicationLoopAsync(storage);
            }
        }

        public async Task EnsureNoDatabaseChangeVectorLeakAsync(string database)
        {
            var ids = new Dictionary<string, int>(StringComparer.Ordinal);

            await foreach (var shard in GetShardsDocumentDatabaseInstancesFor(database))
            {
                if (ids.TryAdd(shard.DbBase64Id, shard.ShardNumber) == false)
                {
                    throw new InvalidOperationException("this shouldn't happened and means that different databases has same ID");
                }
            }

            await foreach (var shard in GetShardsDocumentDatabaseInstancesFor(database))
            {
                if (shard.IsDisposed)
                    continue;

                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var databaseChangeVector = DocumentsStorage.GetFullDatabaseChangeVector(ctx);
                    if (string.IsNullOrEmpty(databaseChangeVector))
                        continue;

                    foreach (var entry in databaseChangeVector.ToChangeVectorList())
                    {
                        if (entry.NodeTag == ChangeVectorParser.RaftInt)
                            continue;

                        if (entry.NodeTag == ChangeVectorParser.TrxnInt)
                            continue;

                        if (entry.NodeTag == ChangeVectorParser.MoveInt)
                            continue;

                        if (ids.TryGetValue(entry.DbId, out var shardNumber))
                        {
                            if (shardNumber == shard.ShardNumber)
                                continue; // this database has a replication factor > 1

                            var sb = new StringBuilder();
                            sb.AppendLine("There was database change vector leaked detected");
                            sb.AppendLine($"The entry '{entry.DbId}' exists in shards {ids[entry.DbId]} and {shard.ShardNumber}");

                            throw new InvalidOperationException(sb.ToString());
                        }
                    }
                }
            }
        }
    }
}
