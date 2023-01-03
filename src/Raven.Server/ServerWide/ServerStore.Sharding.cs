using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.ServerWide
{
    public partial class ServerStore
    {
        public static bool BlockPrefixedSharding = true;

        public static void FillShardingConfiguration(ServerStore serverStore, AddDatabaseCommand addDatabase, ClusterTopology clusterTopology)
        {
            var shardingConfiguration = addDatabase.Record.Sharding;
            if (shardingConfiguration.BucketRanges == null ||
                shardingConfiguration.BucketRanges.Count == 0)
            {
                shardingConfiguration.BucketRanges = new List<ShardBucketRange>();
                var start = 0;
                var step = ShardHelper.NumberOfBuckets / shardingConfiguration.Shards.Count;
                for (int i = 0; i < shardingConfiguration.Shards.Count; i++)
                {
                    shardingConfiguration.BucketRanges.Add(new ShardBucketRange
                    {
                        ShardNumber = i,
                        BucketRangeStart = start
                    });
                    start += step;
                }
            }

            if (addDatabase.RaftCommandIndex == null)
            {
                FillPrefixedSharding(shardingConfiguration);
            }

            var orchestratorTopology = shardingConfiguration.Orchestrator.Topology;
            if (orchestratorTopology.Count == 0)
            {
                serverStore.AssignNodesToDatabase(clusterTopology, addDatabase.Record.DatabaseName, addDatabase.Encrypted, orchestratorTopology);
            }

            Debug.Assert(orchestratorTopology.Count != 0, "Empty orchestrator topology after AssignNodesToDatabase");

            var pool = GetNodesDistribution(clusterTopology, shardingConfiguration.Shards);
            var index = 0;
            var keys = pool.Keys.ToList();
            foreach (var (_, shardTopology) in shardingConfiguration.Shards)
            {
                while (shardTopology.ReplicationFactor > shardTopology.Count)
                {
                    var tag = keys[index++ % keys.Count];

                    if (pool[tag] > 0 && shardTopology.AllNodes.Contains(tag) == false)
                    {
                        pool[tag]--;
                        shardTopology.Members.Add(tag);
                    }

                    if (pool[tag] == 0)
                        keys.Remove(tag);
                }

                Debug.Assert(shardTopology.Count != 0, "Empty shard topology after AssignNodesToDatabase");
            }
        }

        private static void FillPrefixedSharding(ShardingConfiguration shardingConfiguration)
        {
            if (shardingConfiguration.Prefixed is not { Count: > 0 })
                return;

            var start = ShardHelper.NumberOfBuckets;
            foreach (var setting in shardingConfiguration.Prefixed)
            {
                AddPrefixedBucketRange(setting, start, shardingConfiguration);
                start += ShardHelper.NumberOfBuckets;
            }
        }

        private static void AddPrefixedBucketRange(PrefixedShardingSetting setting, int rangeStart, ShardingConfiguration shardingConfiguration)
        {
            if (setting.Prefix.EndsWith('/') == false && setting.Prefix.EndsWith('-') == false)
                throw new InvalidOperationException(
                    $"Cannot add prefix '{setting.Prefix}' to {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. " +
                    "In order to define sharding by prefix, the prefix string must end with '/' or '-' characters.");

            setting.BucketRangeStart = rangeStart;

            var shards = setting.Shards;
            var step = ShardHelper.NumberOfBuckets / shards.Count;

            foreach (var shardNumber in shards)
            {
                if (shardingConfiguration.Shards.ContainsKey(shardNumber) == false)
                {
                    throw new InvalidDataException($"Cannot assign shard number {shardNumber} to prefix {setting.Prefix}, " +
                                                   $"there's no shard '{shardNumber}' in sharding topology!");
                }

                shardingConfiguration.BucketRanges.Add(new ShardBucketRange
                {
                    ShardNumber = shardNumber,
                    BucketRangeStart = rangeStart
                });
                rangeStart += step;
            }
        }

        public static async Task UpdatePrefixedShardingIfNeeded(ServerStore serverStore, TransactionOperationContext context, DatabaseRecord databaseRecord, ClusterTopology clusterTopology)
        {
            var existingConfiguration = serverStore.Cluster.ReadShardingConfiguration(context, databaseRecord.DatabaseName);
            if (databaseRecord.Sharding.Prefixed.SequenceEqual(existingConfiguration.Prefixed))
                return;

            var urls = databaseRecord.Sharding.Orchestrator.Topology.Members.Select(clusterTopology.GetUrlFromTag).ToArray();
            using (var requestExecutor = RequestExecutor.CreateForServer(urls, databaseRecord.DatabaseName, serverStore.Server.Certificate.Certificate, DocumentConventions.DefaultForServer))
            {
                await HandlePrefixSettingsUpdate(context, databaseRecord, existingConfiguration.Prefixed, requestExecutor);
            }
        }

        private static async Task HandlePrefixSettingsUpdate(JsonOperationContext context, DatabaseRecord databaseRecord, List<PrefixedShardingSetting> existingSettings, RequestExecutor requestExecutor)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Minor,
                "optimize this and reuse deleted bucket ranges");

            var shardingConfiguration = databaseRecord.Sharding;
            var safeToRemove = new List<PrefixedShardingSetting>();
            var maxBucketRangeStart = 0;

            foreach (var existingSetting in existingSettings)
            {
                bool found = false;
                foreach (var setting in shardingConfiguration.Prefixed)
                {
                    if (setting.Prefix != existingSetting.Prefix)
                        continue;

                    found = true;

                    if (setting.Shards.SequenceEqual(existingSetting.Shards) == false)
                    {
                        // todo

                        // assigned shards were changed for this prefix settings
                        // check if we can change it in Sharding.BucketRanges (no existing docs)
                    }

                    setting.BucketRangeStart = existingSetting.BucketRangeStart;
                    if (maxBucketRangeStart < setting.BucketRangeStart)
                        maxBucketRangeStart = setting.BucketRangeStart;

                    break;
                }

                if (found)
                    continue;

                // existingSetting.Prefix was removed
                if (await AssertNoDocsStartingWith(existingSetting.Prefix, context, requestExecutor) == false)
                    throw new InvalidOperationException(
                        $"Cannot remove prefix '{existingSetting.Prefix}' from {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. " +
                        $"There are existing documents in database '{databaseRecord.DatabaseName}' that start with '{existingSetting.Prefix}'. " +
                        "In order to remove a sharding by prefix setting, you cannot have any documents in the database that starts with this prefix.");

                safeToRemove.Add(existingSetting);
            }

            // remove deleted prefixes from Sharding.BucketRanges
            foreach (var setting in safeToRemove)
            {
                for (int index = 0; index < shardingConfiguration.BucketRanges.Count; index++)
                {
                    var range = shardingConfiguration.BucketRanges[index];
                    if (range.BucketRangeStart != setting.BucketRangeStart)
                        continue;

                    shardingConfiguration.BucketRanges.RemoveRange(index, setting.Shards.Count);
                    break;
                }
            }

            var start = maxBucketRangeStart + ShardHelper.NumberOfBuckets;

            // add new prefixed settings to Sharding.BucketRanges
            foreach (var setting in shardingConfiguration.Prefixed)
            {
                if (setting.BucketRangeStart != 0)
                    continue; // already added to BucketRanges

                if (await AssertNoDocsStartingWith(setting.Prefix, context, requestExecutor) == false)
                    throw new InvalidOperationException(
                        $"Cannot add prefix '{setting.Prefix}' to {nameof(ShardingConfiguration)}.{nameof(ShardingConfiguration.Prefixed)}. " +
                        $"There are existing documents in database '{databaseRecord.DatabaseName}' that start with '{setting.Prefix}'. " +
                        "In order to define sharding by prefix, you cannot have any documents in the database that starts with this prefix.");

                AddPrefixedBucketRange(setting, start, shardingConfiguration);
                start += ShardHelper.NumberOfBuckets;
            }

        }

        private static async Task<bool> AssertNoDocsStartingWith(string prefix, JsonOperationContext context, RequestExecutor requestExecutor)
        {
            var command = new GetDocumentsCommand(startWith: prefix,
                startAfter: null, matches: null, exclude: null, start: 0, pageSize: int.MaxValue, metadataOnly: false);

            await requestExecutor.ExecuteAsync(command, context, sessionInfo: null);
            return command.Result.Results.Length == 0;
        }

        private static Dictionary<string, int> GetNodesDistribution(ClusterTopology clusterTopology, Dictionary<int, DatabaseTopology> shards)
        {
            var total = 0;
            var pool = new Dictionary<string, int>(); // tag, number of occurrences

            foreach (var node in clusterTopology.AllNodes)
            {
                pool[node.Key] = 0;
            }

            foreach (var (shardNumber, shardTopology) in shards)
            {
                total += shardTopology.ReplicationFactor;
            }

            var perNode = total / pool.Count;
            foreach (var node in pool.Keys)
            {
                pool[node] = perNode;
                total -= perNode;
            }

            foreach (var node in pool.Keys)
            {
                if (total == 0)
                    break;

                pool[node]++;
                total--;
            }

            return pool;
        }
    }
} 
