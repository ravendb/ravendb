using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Sharding;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public sealed class ShardedAdminDatabaseHandler : ServerRequestHandler
    {
        [RavenAction("/admin/databases/orchestrator", "PUT", AuthorizationStatus.Operator)]
        public async Task AddNodeToOrchestratorTopology()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name").Trim();
            var node = GetStringQueryString("node", required: false);
            var raftRequestId = GetRaftRequestIdFromQuery();

            AssertNotAShardDatabaseName(name);

            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name, out var index);
                if (databaseRecord == null)
                {
                    throw new DatabaseDoesNotExistException("Database Record not found when attempting to add a node to the database topology");
                }

                if (databaseRecord.IsSharded == false)
                    throw new NotSupportedException($"Modifying the orchestrator topology is only valid for sharded databases. Instead got a non sharded database {name}");

                var clusterTopology = ServerStore.GetClusterTopology(context);

                var topology = databaseRecord.Sharding.Orchestrator.Topology;
                
                // the case where an explicit node was requested
                if (string.IsNullOrEmpty(node) == false)
                {
                    if (topology.RelevantFor(node))
                        throw new InvalidOperationException($"Can't add node {node} to {name} topology because it is already part of it");

                    var databaseIsBeingDeleted = databaseRecord.DeletionInProgress != null &&
                                                 databaseRecord.EntireDatabasePendingDeletion();
                    if (databaseIsBeingDeleted)
                        throw new InvalidOperationException($"Can't add node {node} to database '{name}' topology because it is currently being deleted from node '{node}'");

                    var url = clusterTopology.GetUrlFromTag(node);
                    if (url == null)
                        throw new InvalidOperationException($"Can't add node {node} to database '{name}' topology because node {node} is not part of the cluster");

                    if (Server.AllowEncryptedDatabasesOverHttp == false && databaseRecord.IsEncrypted && AdminDatabasesHandler.NotUsingHttps(url))
                        throw new InvalidOperationException($"Can't add node {node} to database '{name}' topology because database {name} is encrypted but node {node} doesn't have an SSL certificate.");
                }

                //The case were we don't care where the database will be added to
                else
                {
                    node = FindFitNodeForDatabase(name, topology, databaseRecord.IsEncrypted, clusterTopology);
                }

                topology.Promotables.Add(node);
                topology.DemotionReasons[node] = "Joined the orchestrator topology as a new promotable node";
                topology.PromotablesStatus[node] = DatabasePromotionStatus.WaitingForFirstPromotion;

                topology.ReplicationFactor++;

                var update = new UpdateTopologyCommand(name, SystemTime.UtcNow, raftRequestId)
                {
                    Topology = topology
                };

                var (newIndex, _) = await ServerStore.SendToLeaderAsync(update);
                await ServerStore.WaitForExecutionOnSpecificNodeAsync(context, node, newIndex);
                
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOrchestratorTopologyResult.Name)] = name,
                        [nameof(ModifyOrchestratorTopologyResult.OrchestratorTopology)] = topology.ToJson(),
                        [nameof(ModifyOrchestratorTopologyResult.RaftCommandIndex)] = newIndex
                    });
                }
            }
        }

        private static string FindFitNodeForDatabase(string databaseName, DatabaseTopology topology, bool isEncrypted, ClusterTopology clusterTopology, string node = null)
        {
            var candidateNodes = clusterTopology.Members.Keys
                .Concat(clusterTopology.Promotables.Keys)
                .Concat(clusterTopology.Watchers.Keys)
                .ToHashSet(StringComparer.InvariantCultureIgnoreCase);

            candidateNodes.RemoveWhere(n => topology.AllNodes.Contains(n));

            if (candidateNodes.Count == 0)
                throw new InvalidOperationException($"Looking for a fit node for database {databaseName} but all nodes in cluster are already taken");

            candidateNodes.RemoveWhere(n => isEncrypted && AdminDatabasesHandler.NotUsingHttps(clusterTopology.GetUrlFromTag(n)));
            
            if (isEncrypted && candidateNodes.Count == 0)
                throw new InvalidOperationException($"Database {databaseName} is encrypted and requires a node which supports SSL. There is no such node available in the cluster.");

            if (candidateNodes.Count == 0)
                throw new InvalidOperationException($"Database {databaseName} already exists on all the nodes of the cluster");

            if (string.IsNullOrEmpty(node) == false && candidateNodes.Contains(node) == false)
                throw new InvalidOperationException($"Cannot put the requested node '{node}' to topology");

            return node ?? candidateNodes.ElementAt(Random.Shared.Next(candidateNodes.Count - 1));
        }

        [RavenAction("/admin/databases/orchestrator", "DELETE", AuthorizationStatus.Operator)]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name").Trim();
            var node = GetStringQueryString("node", required: true);
            var raftRequestId = GetRaftRequestIdFromQuery();

            AssertNotAShardDatabaseName(name);

            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name, out var index);
                if (databaseRecord == null)
                {
                    throw new DatabaseDoesNotExistException("Database Record not found when attempting to remove a node from the database topology");
                }

                if (databaseRecord.IsSharded == false)
                    throw new NotSupportedException($"Modifying the orchestrator topology is only valid for sharded databases. Instead got a non sharded database {name}");

                var clusterTopology = ServerStore.GetClusterTopology(context);
                
                var topology = databaseRecord.Sharding.Orchestrator.Topology;
                
                // the case where an explicit node was requested
                if (string.IsNullOrEmpty(node) == false)
                {
                    if (topology.RelevantFor(node) == false)
                        throw new InvalidOperationException($"Can't remove node {node} from {name} topology because it is already not a part of it");

                    if (topology.Members.Count == 1)
                        throw new InvalidOperationException($"Can't remove node {node} from {name} orchestrator topology because it is the only one in the topology.");
                }

                topology.RemoveFromTopology(node);
                topology.ReplicationFactor--;

                var update = new UpdateTopologyCommand(name, SystemTime.UtcNow, raftRequestId)
                {
                    Topology = topology
                };

                var (newIndex, _) = await ServerStore.SendToLeaderAsync(update);
                await ServerStore.WaitForExecutionOnSpecificNodeAsync(context, node, newIndex);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOrchestratorTopologyResult.Name)] = name,
                        [nameof(ModifyOrchestratorTopologyResult.OrchestratorTopology)] = topology.ToJson(),
                        [nameof(ModifyOrchestratorTopologyResult.RaftCommandIndex)] = newIndex
                    });
                }
            }
        }

        public static void AssertNotAShardDatabaseName(string name)
        {
            if (ShardHelper.IsShardName(name))
                throw new NotSupportedException($"Adding node to orchestrator is only valid for sharded databases. Instead got a shard {name}");
        }

        [RavenAction("/admin/databases/shard", "PUT", AuthorizationStatus.Operator)]
        public async Task CreateNewShard()
        {
            var database = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name").Trim();
            var shardNumber = GetIntValueQueryString("shardNumber", required: false);
            var nodes = GetStringValuesQueryString("node", required: false);
            var replicationFactor = GetIntValueQueryString("replicationFactor", required: false);
            var dynamicNodeDistribution = GetBoolValueQueryString("dynamicNodeDistribution", required: false);
            var raftRequestId = GetRaftRequestIdFromQuery();
            
            if (ShardHelper.IsShardName(database))
            {
                throw new NotSupportedException(
                    $"Cannot add a new shard to an existing shard instance. To increase a shard's replication factor use the {nameof(AddDatabaseNodeOperation)}.");
            }
            
            if (replicationFactor.HasValue && replicationFactor < 1)
                throw new InvalidOperationException($"Cannot add a new shard to database {database} with a replication factor {replicationFactor}");

            var nodesList = new List<string>();
            foreach (var node in nodes)
            {
                if (nodesList.Contains(node))
                    throw new InvalidOperationException($"Cannot add a new shard to database {database}. The provided list of nodes contains duplicates {string.Join(",", nodes.ToArray())}");

                nodesList.Add(node.Trim());
            }

            await ServerStore.EnsureNotPassiveAsync();

            using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, database, out var index);
                if (databaseRecord == null)
                {
                    throw new DatabaseDoesNotExistException("Database Record not found when attempting to create a new shard.");
                }

                if (databaseRecord.IsSharded == false)
                {
                    throw new NotSupportedException($"Attempting to add a shard to database {database}, but it is not sharded.");
                }

                if (shardNumber.HasValue)
                {
                    if (shardNumber.Value < 0)
                        throw new ArgumentException($"Cannot add a shard with a negative number {shardNumber.Value}.");

                    if(databaseRecord.Sharding.Shards.ContainsKey(shardNumber.Value))
                        throw new InvalidOperationException($"Cannot add shard {shardNumber.Value} to database {database} because it already exists.");

                    if (databaseRecord.IsShardBeingDeletedOnAnyNode(shardNumber.Value))
                        throw new InvalidOperationException(
                            $"Cannot add shard {shardNumber.Value} to database {database} because there is a shard with this number in the process of being deleted.");
                }

                var clusterTopology = Server.ServerStore.GetClusterTopology(context);
                foreach (var node in nodesList)
                {
                    var url = clusterTopology.GetUrlFromTag(node);
                    if (url == null)
                        throw new InvalidOperationException($"Can't create new shard on node {node} for database '{database}' because node {node} is not part of the cluster");

                    if (Server.AllowEncryptedDatabasesOverHttp == false && databaseRecord.IsEncrypted && AdminDatabasesHandler.NotUsingHttps(url))
                        throw new InvalidOperationException($"Can't create node {node} for database '{database}' because database {database} is encrypted but node {node} doesn't have an SSL certificate.");
                }

                if (databaseRecord.EntireDatabasePendingDeletion())
                {
                    throw new InvalidOperationException($"Can't add a new shard to database '{database}' because it is currently being deleted.");
                }


                int newChosenShardNumber;
                
                if (shardNumber.HasValue == false)
                {
                    newChosenShardNumber = GetMaxShardNumber(databaseRecord.Sharding) + 1;

                    // A shard might still be in the progress of deletion and removed from topology so we don't want to add it right now
                    while (databaseRecord.IsShardBeingDeletedOnAnyNode(newChosenShardNumber))
                    {
                        newChosenShardNumber++;
                    }
                }
                else
                {
                    newChosenShardNumber = shardNumber.Value;
                }

                if (replicationFactor.HasValue && replicationFactor.Value > clusterTopology.AllNodes.Count)
                    throw new InvalidOperationException($"Replication factor {replicationFactor.Value} cannot exceed the number of nodes in the cluster {clusterTopology.AllNodes.Count}.");

                replicationFactor  ??= (nodesList.Count > 0 ? nodesList.Count : databaseRecord.Sharding.Shards.ElementAt(0).Value.ReplicationFactor);
                dynamicNodeDistribution ??= databaseRecord.Sharding.Shards.ElementAt(0).Value.DynamicNodesDistribution;

                if (nodesList.Count == 0)
                {
                    var nodeToInstanceCount = new Dictionary<string, int>();
                    foreach (var node in clusterTopology.AllNodes.Keys)
                    {
                        nodeToInstanceCount[node] = 0;
                    }

                    foreach (var (_, topology) in databaseRecord.Sharding.Shards)
                    {
                        foreach (var node in topology.AllNodes)
                        {
                            nodeToInstanceCount[node] += 1;
                        }
                    }

                    nodesList = nodeToInstanceCount.OrderBy(n => n.Value).Select(kvp => kvp.Key).ToList();
                }
                
                var newShardTopology = new DatabaseTopology
                {
                    DynamicNodesDistribution = dynamicNodeDistribution.Value
                };

                for (int i = 0; i < replicationFactor; i++)
                {
                    var node = nodesList.ElementAtOrDefault(i);
                    node = FindFitNodeForDatabase(database, newShardTopology, databaseRecord.IsEncrypted, clusterTopology, node);
                    newShardTopology.Members.Add(node);
                }

                DatabaseHelper.InitializeDatabaseTopology(ServerStore, newShardTopology, clusterTopology, replicationFactor.Value,
                    databaseRecord.Sharding.Orchestrator.Topology.ClusterTransactionIdBase64);

                var update = new CreateNewShardCommand(database, newChosenShardNumber, newShardTopology, SystemTime.UtcNow, raftRequestId);

                var (newIndex, _) = await ServerStore.SendToLeaderAsync(update);
                await ServerStore.WaitForExecutionOnRelevantNodesAsync(context, newShardTopology.Members, newIndex);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(AddDatabaseShardResult.Name)] = database,
                        [nameof(AddDatabaseShardResult.ShardNumber)] = newChosenShardNumber,
                        [nameof(AddDatabaseShardResult.ShardTopology)] = newShardTopology.ToJson(),
                        [nameof(AddDatabaseShardResult.RaftCommandIndex)] = newIndex
                    });
                }
            }
        }

        private int GetMaxShardNumber(RawShardingConfiguration config)
        {
            var max = 0;
            foreach (var shardNumber in config.Shards.Keys)
            {
                if (shardNumber > max)
                {
                    max = shardNumber;
                }
            }
            return max;
        }
    }
}
