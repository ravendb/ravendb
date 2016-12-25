using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.NewClient.Client.Http;
using Raven.Server.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using ReadBehavior = Raven.Client.Http.ReadBehavior;
using ServerNode = Raven.Client.Http.ServerNode;
using Topology = Raven.Client.Http.Topology;
using TopologySla = Raven.Client.Http.TopologySla;
using WriteBehavior = Raven.Client.Http.WriteBehavior;

namespace Raven.Server.Documents.Handlers
{
    public class TopologyHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/topology", "GET")]
        public async Task GetTopology()
        {
            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, await GenerateTopology(context));
            }
        }
        
        private async Task<DynamicJsonValue> GenerateTopology(DocumentsOperationContext context)
        {
            Document replicationConfigDocument;
            using (context.OpenReadTransaction())
            {
                replicationConfigDocument = Database.DocumentsStorage.Get(context, Constants.Replication.DocumentReplicationConfiguration);
            }

            //This is the case where we don't have real replication topology.
            if (replicationConfigDocument == null)
            {
                return GetEmptyTopology();
            }

            var replicationDocument = JsonDeserializationServer.ReplicationDocument(replicationConfigDocument.Data);
            if (replicationDocument.Destinations.Count == 0)
                return GetEmptyTopology();

            using (var discoverer = new ReplicationTopologyDiscoverer(
                Database.DocumentReplicationLoader.Outgoing,
                Database.DatabaseShutdown,
                new List<Guid>()))
            {
                var nodes = await discoverer.WaitForTopologyDiscovery();
                return GetTopology(nodes);
            }
        }

        private DynamicJsonValue NodeGraphToJson(TopologyNode node)
        {
            var json = new DynamicJsonValue
            {
                [nameof(TopologyNode.Node)] = new DynamicJsonValue
                {
                    [nameof(ServerNode.Url)] = node.Node.Url,
                    [nameof(ServerNode.ApiKey)] = node.Node.ApiKey,
                    [nameof(ServerNode.Database)] = node.Node.Database
                },
                [nameof(TopologyNode.Outgoing)] = new DynamicJsonArray(node.Outgoing.Select(NodeGraphToJson))
            };

            return json;
        }

        private DynamicJsonValue GetTopology(TopologyNode[] nodes)
        {
            return new DynamicJsonValue
            {
                [nameof(Topology.LeaderNode)] = new DynamicJsonValue
                {
                    [nameof(ServerNode.Url)] = GetStringQueryString("url", required: false) ?? 
                                                    Server.Configuration.Core.ServerUrl,
                    [nameof(ServerNode.Database)] = Database.Name,
                },
                [nameof(Topology.Nodes)] = new DynamicJsonArray(nodes.Select(NodeGraphToJson)),
                [nameof(Topology.ReadBehavior)] = ReadBehavior.LeaderWithFailoverWhenRequestTimeSlaThresholdIsReached.ToString(),
                [nameof(Topology.WriteBehavior)] = WriteBehavior.LeaderOnly.ToString(),
                [nameof(Topology.SLA)] = new DynamicJsonValue
                {
                    [nameof(TopologySla.RequestTimeThresholdInMilliseconds)] = 100,
                },
                [nameof(Topology.Etag)] = -1,
            };
        }

        private DynamicJsonValue GetEmptyTopology()
        {
            return new DynamicJsonValue
            {
                [nameof(Topology.LeaderNode)] = new DynamicJsonValue
                {
                    [nameof(ServerNode.Url)] = GetStringQueryString("url", required: false) ?? Server.Configuration.Core.ServerUrl,
                    [nameof(ServerNode.Database)] = Database.Name,
                },
                [nameof(Topology.Nodes)] = null,
                [nameof(Topology.ReadBehavior)] = ReadBehavior.LeaderWithFailoverWhenRequestTimeSlaThresholdIsReached.ToString(),
                [nameof(Topology.WriteBehavior)] = WriteBehavior.LeaderOnly.ToString(),
                [nameof(Topology.SLA)] = new DynamicJsonValue
                {
                    [nameof(TopologySla.RequestTimeThresholdInMilliseconds)] = 100,
                },
                [nameof(Topology.Etag)] = -1,
            };
        }

        private IEnumerable<DynamicJsonValue> GenerateNodesFromReplicationDocument(ReplicationDocument replicationDocument)
        {
            var destinations = new DynamicJsonValue[replicationDocument.Destinations.Count];
            var etags = new long[replicationDocument.Destinations.Count];
            for (int index = 0; index < replicationDocument.Destinations.Count; index++)
            {
                var des = replicationDocument.Destinations[index];
                if (des.CanBeFailover() == false || des.Disabled || des.IgnoredClient ||
                    des.SpecifiedCollections?.Count > 0)
                    continue;
                etags[index] = Database.DocumentReplicationLoader.GetLastReplicatedEtagForDestination(des) ??
                               -1;
                destinations[index] = new DynamicJsonValue
                {
                    [nameof(ServerNode.Url)] = des.Url,
                    [nameof(ServerNode.ApiKey)] = des.ApiKey,
                    [nameof(ServerNode.Database)] = des.Database
                };
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