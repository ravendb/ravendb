using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Sharding;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    public sealed class ShardedAdminShardingHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/sharding/resharding/cleanup", "POST")]
        public async Task ExecuteMoveDocuments()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support documents migration operation. " +
                                                                             "This operation is available only from a specific shard"))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/sharding/prefixes/add", "POST")]
        public async Task AddPrefixConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), GetType().Name);
                var setting = JsonDeserializationCluster.PrefixedShardingSetting(json);

                var shardingConfiguration = ServerStore.Cluster.ReadShardingConfiguration(DatabaseName);
                ShardingStore.AssertValidPrefix(setting, shardingConfiguration);

                var exists = shardingConfiguration.Prefixed.BinarySearch(setting, PrefixedSettingComparer.Instance) >= 0;
                if (exists)
                    throw new InvalidOperationException(
                        $"Prefix '{setting.Prefix}' already exists in {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. please use '{nameof(UpdatePrefixedShardingSettingOperation)} operation'");

                var clusterTopology = ServerStore.GetClusterTopology(context);
                var urls = shardingConfiguration.Orchestrator.Topology.Members.Select(clusterTopology.GetUrlFromTag).ToArray();

                if (await AssertNoDocumentsStartingWith(context, setting.Prefix, urls) == false)
                    throw new InvalidOperationException(
                        $"Cannot add prefix '{setting.Prefix}' to {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. " +
                        $"There are existing documents in database '{DatabaseName}' that start with '{setting.Prefix}'. " +
                        "In order to define sharding by prefix, you cannot have any documents in the database that starts with this prefix.");

                var cmd = new AddPrefixedSettingCommand(setting, DatabaseName, GetRaftRequestIdFromQuery());
                var (raftIndex, _) = await ServerStore.SendToLeaderAsync(cmd);

                await DatabaseContext.ServerStore.WaitForExecutionOnRelevantNodesAsync(context, shardingConfiguration.Orchestrator.Topology.Members, raftIndex);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            }
        }

        [RavenShardedAction("/databases/*/admin/sharding/prefixes/delete", "DELETE")]
        public async Task DeletePrefixConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var prefix = GetStringQueryString("prefix");

                var shardingConfiguration = ServerStore.Cluster.ReadShardingConfiguration(DatabaseName);
                bool found = shardingConfiguration.Prefixed.Any(value => string.Equals(value.Prefix, prefix, StringComparison.OrdinalIgnoreCase));
                if (found == false)
                    throw new InvalidDataException($"Prefix '{prefix}' wasn't found in sharding configuration");

                var clusterTopology = ServerStore.GetClusterTopology(context);
                var urls = shardingConfiguration.Orchestrator.Topology.Members.Select(clusterTopology.GetUrlFromTag).ToArray();

                if (await AssertNoDocumentsStartingWith(context, prefix, urls) == false)
                    throw new InvalidOperationException(
                        $"Cannot remove prefix '{prefix}' from {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. " +
                        $"There are existing documents in database '{DatabaseName}' that start with '{prefix}'. " +
                        "In order to remove a sharding by prefix setting, you cannot have any documents in the database that starts with this prefix.");

                var cmd = new DeletePrefixedSettingCommand(prefix, DatabaseName, GetRaftRequestIdFromQuery());
                var (raftIndex, _) = await ServerStore.SendToLeaderAsync(cmd);

                await DatabaseContext.ServerStore.WaitForExecutionOnRelevantNodesAsync(context, shardingConfiguration.Orchestrator.Topology.Members, raftIndex);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            }
        }

        [RavenShardedAction("/databases/*/admin/sharding/prefixes/update", "POST")]
        public async Task UpdatePrefixConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), GetType().Name);
                var setting = JsonDeserializationCluster.PrefixedShardingSetting(json);

                var shardingConfiguration = ServerStore.Cluster.ReadShardingConfiguration(DatabaseName);

                var location = shardingConfiguration.Prefixed.BinarySearch(setting, PrefixedSettingComparer.Instance);
                if (location < 0)
                    throw new InvalidDataException($"Prefix '{setting.Prefix}' wasn't found in sharding configuration");

                var oldSetting = shardingConfiguration.Prefixed[location];
                var removedShards = oldSetting.Shards;

                foreach (var shard in setting.Shards)
                {
                    if (oldSetting.Shards.Contains(shard))
                        removedShards.Remove(shard);
                    else if (shardingConfiguration.Shards.ContainsKey(shard) == false)
                        throw new InvalidDataException($"Cannot assign shard number {shard} to prefix {setting.Prefix}, " +
                                                       $"there's no shard '{shard}' in sharding topology!");
                }

                if (removedShards.Count > 0)
                {                
                    // check that there are no bucket ranges mapped to these shards

                    int index;
                    bool found = false;
                    var prefixBucketRangeStart = oldSetting.BucketRangeStart;

                    for (index = 0; index < shardingConfiguration.BucketRanges.Count; index++)
                    {
                        var range = shardingConfiguration.BucketRanges[index];
                        if (range.BucketRangeStart < prefixBucketRangeStart)
                            continue;

                        if (range.BucketRangeStart == prefixBucketRangeStart)
                            found = true;

                        break;
                    }

                    if (found)
                    {
                        var shards = new List<int>
                        {
                            shardingConfiguration.BucketRanges[index++].ShardNumber
                        };
                        var nextPrefixedRangeStart = prefixBucketRangeStart + ShardHelper.NumberOfBuckets;
                        for (; index < shardingConfiguration.BucketRanges.Count; index++)
                        {
                            var range = shardingConfiguration.BucketRanges[index];
                            if (range.BucketRangeStart < nextPrefixedRangeStart)
                            {
                                shards.Add(range.ShardNumber);
                                continue;
                            }

                            break;
                        }

                        foreach (var shard in removedShards)
                        {
                            if (shards.Contains(shard))
                                throw new InvalidOperationException(
                                    $"Cannot remove shard {shard} from '{setting.Prefix}' settings in {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. " +
                                    $"There are bucket ranges mapped to this shard. " +
                                    "In order to remove a shard from a Prefixed setting, first you need to migrate all its buckets to another shard.");

                        }
                    }
                }

                var cmd = new UpdatePrefixedSettingCommand(setting, DatabaseName, GetRaftRequestIdFromQuery());
                var (raftIndex, _) = await ServerStore.SendToLeaderAsync(cmd);

                await DatabaseContext.ServerStore.WaitForExecutionOnRelevantNodesAsync(context, shardingConfiguration.Orchestrator.Topology.Members, raftIndex);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            }
        }

        private async Task<bool> AssertNoDocumentsStartingWith(TransactionOperationContext context, string prefix, string[] urls, string database = null)
        {
            using (var requestExecutor = RequestExecutor.CreateForServer(urls, database ?? DatabaseName, ServerStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
            {
                var command = new GetDocumentsCommand(requestExecutor.Conventions, startWith: prefix,
                    startAfter: null, matches: null, exclude: null, start: 0, pageSize: 1, metadataOnly: false);

                await requestExecutor.ExecuteAsync(command, context, sessionInfo: null);
                return command.Result.Results.Length == 0;
            }
        }
    }
}
