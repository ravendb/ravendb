using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly ShardingTestBase Sharding;

    public class ShardingTestBase
    {
        public ShardedBackupTestsBase Backup;
        public ShardedSubscriptionTestBase Subscriptions;

        private readonly RavenTestBase _parent;
        public readonly ReshardingTestBase Resharding;

        public ShardingTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            Backup = new ShardedBackupTestsBase(_parent);
            Resharding = new ReshardingTestBase(_parent);
            Subscriptions = new ShardedSubscriptionTestBase(_parent);
        }

        public DocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null, Dictionary<int, DatabaseTopology> shards = null)
        {
            var shardedOptions = options ?? new Options();
            shardedOptions.ModifyDatabaseRecord += r =>
            {
                r.Sharding ??= new ShardingConfiguration();

                if (shards == null)
                {
                    r.Sharding.Shards = new Dictionary<int, DatabaseTopology>()
                    {
                        {0, new DatabaseTopology()},
                        {1, new DatabaseTopology()},
                        {2, new DatabaseTopology()},
                    };
                }
                else
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

                    for (int shardNumber = 0; shardNumber < r.Sharding.Shards.Count; shardNumber++)
                    {
                        r.Sharding.Shards[shardNumber] = new DatabaseTopology
                        {
                            ReplicationFactor = shardReplicationFactor,
                            DynamicNodesDistribution = dynamicNodeDistribution
                        };
                    }
                },
                Server = leader
            };

            return options;
        }

        public static int GetNextSortedShardNumber(Dictionary<int, DatabaseTopology> shards, int shardNumber)
        {
            var shardsSorted = shards.Keys.OrderBy(x => x).ToArray();
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

        public async Task<ShardingConfiguration> GetShardingConfigurationAsync(IDocumentStore store)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            return record.Sharding;
        }

        public async Task<int> GetShardNumber(IDocumentStore store, string id, string database = null)
        {
            database ??= store.Database;
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database));
            var bucket = ShardHelper.GetBucket(id);
            return ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket);
        }

        public async Task<ShardedDocumentDatabase> GetShardedDocumentDatabaseForBucketAsync(string database, int bucket)
        {
            using (_parent.Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var config = _parent.Server.ServerStore.Cluster.ReadShardingConfiguration(context, database);
                var shardNumber = ShardHelper.GetShardNumber(config.BucketRanges, bucket);
                var shardedName = ShardHelper.ToShardName(database, shardNumber);
                var shardedDatabase = (await _parent.Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(shardedName)) as ShardedDocumentDatabase;
                Assert.NotNull(shardedDatabase);
                return shardedDatabase;

            }
        }

        public IAsyncEnumerable<ShardedDocumentDatabase> GetShardsDocumentDatabaseInstancesFor(IDocumentStore store, List<RavenServer> servers = null)
        {
            return GetShardsDocumentDatabaseInstancesFor(store.Database, servers);
        }

        public async IAsyncEnumerable<ShardedDocumentDatabase> GetShardsDocumentDatabaseInstancesFor(string database, List<RavenServer> servers = null)
        {
            servers ??= _parent.GetServers();
            foreach (var server in servers)
            {
                foreach (var task in server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database))
                {
                    var databaseInstance = await task;
                    Debug.Assert(databaseInstance != null, $"The requested database '{database}' is null, probably you try to loaded sharded database without the $");
                    yield return databaseInstance;
                }
            }
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
    }
}
