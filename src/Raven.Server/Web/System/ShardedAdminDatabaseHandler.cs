using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Client.Util;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class ShardedAdminDatabaseHandler : ServerRequestHandler
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

                    if (databaseRecord.IsEncrypted && AdminDatabasesHandler.NotUsingHttps(url))
                        throw new InvalidOperationException($"Can't add node {node} to database '{name}' topology because database {name} is encrypted but node {node} doesn't have an SSL certificate.");
                }

                //The case were we don't care where the database will be added to
                else
                {
                    var allNodes = clusterTopology.Members.Keys
                        .Concat(clusterTopology.Promotables.Keys)
                        .Concat(clusterTopology.Watchers.Keys)
                        .ToList();

                    allNodes.RemoveAll(n => topology.AllNodes.Contains(n) || (databaseRecord.IsEncrypted && AdminDatabasesHandler.NotUsingHttps(clusterTopology.GetUrlFromTag(n))));

                    if (databaseRecord.IsEncrypted && allNodes.Count == 0)
                        throw new InvalidOperationException($"Database {name} is encrypted and requires a node which supports SSL. There is no such node available in the cluster.");

                    if (allNodes.Count == 0)
                        throw new InvalidOperationException($"Database {name} already exists on all the nodes of the cluster");

                    var rand = new Random().Next();
                    node = allNodes[rand % allNodes.Count];
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
                await WaitForExecutionOnSpecificNode(context, clusterTopology, node, newIndex);
                
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOrchestratorTopologyResult.DatabaseName)] = name,
                        [nameof(ModifyOrchestratorTopologyResult.OrchestratorTopology)] = topology.ToJson(),
                        [nameof(ModifyOrchestratorTopologyResult.RaftCommandIndex)] = newIndex
                    });
                }
            }
        }

        [RavenAction("/admin/databases/orchestrator", "DELETE", AuthorizationStatus.Operator)]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name").Trim();
            var node = GetStringQueryString("node", required: true);
            var force = GetBoolValueQueryString("force", required: false) ?? false;
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

                    if (topology.Members.Count == 1 && force == false)
                        throw new InvalidOperationException($"Can't remove node {node} from {name} orchestrator topology because it is the only one in the topology. To remove it anyway, use force=true");
                }

                topology.RemoveFromTopology(node);
                topology.ReplicationFactor--;

                var update = new UpdateTopologyCommand(name, SystemTime.UtcNow, raftRequestId)
                {
                    Topology = topology
                };

                var (newIndex, _) = await ServerStore.SendToLeaderAsync(update);
                await WaitForExecutionOnSpecificNode(context, clusterTopology, node, newIndex);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, new DynamicJsonValue
                    {
                        [nameof(ModifyOrchestratorTopologyResult.DatabaseName)] = name,
                        [nameof(ModifyOrchestratorTopologyResult.OrchestratorTopology)] = topology.ToJson(),
                        [nameof(ModifyOrchestratorTopologyResult.RaftCommandIndex)] = newIndex
                    });
                }
            }
        }

        public static void AssertNotAShardDatabaseName(string name)
        {
            if (ShardHelper.IsShardedName(name))
                throw new NotSupportedException($"Adding node to orchestrator is only valid for sharded databases. Instead got a shard {name}");
        }
    }
}
