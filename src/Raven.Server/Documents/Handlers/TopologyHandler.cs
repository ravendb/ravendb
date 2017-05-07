using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Http;
using Raven.Server.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class TopologyHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/debug/live-topology", "GET")]
        public async Task GetLiveTopology()
        {
            var sp = Stopwatch.StartNew();
           
            TransactionOperationContext context;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
            using(context.OpenReadTransaction())
            {
                var databaseRecord = Server.ServerStore.Cluster.ReadDatabase(context, Database.Name);

                var replicationDiscovertTimeout = Database.Configuration
                    .Replication
                    .ReplicationTopologyDiscoveryTimeout
                    .AsTimeSpan;
                using (var clusterTopologyExplorer = new ClusterTopologyExplorer(
                    Database,
                    new List<string>
                    {
                        Database.DbId.ToString()
                    },
                    replicationDiscovertTimeout,
                    databaseRecord?.Topology.GetDestinations(Server.ServerStore.NodeTag, Database.Name).ToList()))
                {
                    var topology = await clusterTopologyExplorer.DiscoverTopologyAsync();
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        var json = topology.ToJson();
                        json["Duration"] = sp.Elapsed.ToString();
                        context.Write(writer, json);
                        writer.Flush();
                    }
                }
            }
        }

        private void WriteEmptyTopology(JsonOperationContext context)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new LiveTopologyInfo
                {
                    DatabaseId = Database.DbId.ToString()
                }.ToJson());
                writer.Flush();
            }
        }

        private void GenerateTopology(DocumentsOperationContext context, BlittableJsonTextWriter writer, IEnumerable<DynamicJsonValue> nodes = null, long etag = -1)
        {
            context.Write(writer, new DynamicJsonValue
            {                
                [nameof(Topology.Nodes)] = (nodes == null)? new DynamicJsonArray(): new DynamicJsonArray(nodes),
                [nameof(Topology.ReadBehavior)] =
				ReadBehavior.CurrentNodeWithFailoverWhenRequestTimeSlaThresholdIsReached.ToString(),                [nameof(Topology.WriteBehavior)] = WriteBehavior.LeaderOnly.ToString(),
                [nameof(Topology.SLA)] = new DynamicJsonValue
                {
                    [nameof(TopologySla.RequestTimeThresholdInMilliseconds)] = 100,
                },
                [nameof(Topology.Etag)] = etag,
            });
        }

        private IEnumerable<DynamicJsonValue> GenerateNodesFromReplicationDocument(List<ReplicationNode> nodes)
        {
            var destinations = new DynamicJsonValue[nodes.Count];
            var etags = new long[nodes.Count];
            for (int index = 0; index < nodes.Count; index++)
            {
                var des = nodes[index];
                if (des.CanBeFailover() == false || des.Disabled || des.IgnoredClient ||
                    des.SpecifiedCollections?.Count > 0)
                    continue;
                etags[index] = Database.ReplicationLoader.GetLastReplicatedEtagForDestination(des) ??
                               -1;
                destinations[index] = new DynamicJsonValue
                {
                    [nameof(ServerNode.Url)] = des.Url,
					[nameof(ServerNode.Database)] = des.Database                };
            }

            // We want to have the client failover to the most up to date destination if it needs to, so we sort
            // them by the last replicated etag

            Array.Sort(etags,destinations);
            for (int i = destinations.Length - 1; i >= 0; i--)
            {
                if (destinations[i] != null)
                    yield return destinations[i];
            }
        }
    }
}