using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.NewClient.Client.Http;
using Raven.Server.Documents.Replication;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using TopologyNode = Raven.NewClient.Client.Http.TopologyNode;

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
                writer.Flush();
            }
        }
        
        private async Task<DynamicJsonValue> GenerateTopology(DocumentsOperationContext context)
        {
            Document replicationConfigDocument;
            using (context.OpenReadTransaction())
                replicationConfigDocument = 
                    Database.DocumentsStorage.Get(context,Constants.Replication.DocumentReplicationConfiguration);

            //This is the case where we don't have real replication topology.
            if (replicationConfigDocument == null)
                return GetTopologyFrom(Enumerable.Empty<TopologyNode>()).ToJson();

            var replicationDocument = JsonDeserializationServer.ReplicationDocument(replicationConfigDocument.Data);
            if (replicationDocument.Destinations.Count == 0)
                return GetTopologyFrom(Enumerable.Empty<TopologyNode>()).ToJson();

            using (var discoverer = new ReplicationTopologyDiscoverer(
                Database.DocumentReplicationLoader.Outgoing,
                new List<string> { Database.DbId.ToString() }, //already know myself, prevent loops in A <-> B topologies
                Database.DatabaseShutdown))
            {
                var nodes = await discoverer.DiscoverTopologyAsync();
                if (nodes.Length > 0)
                {
                    var etags = new long[nodes.Length];
                    for (int i = 0; i < nodes.Length; i++)
                        etags[i] = Database.DocumentReplicationLoader.GetLastReplicatedEtagForDestination(nodes[i]) ?? -1;

                    //sort by last replicated etag, so if a failover happens, clients will go to the most updated node
                    Array.Sort(etags, nodes);
                }

                return GetTopologyFrom(nodes).ToJson();
            }
        }

        private Topology GetTopologyFrom(IEnumerable<TopologyNode> nodes)
        {
            return new Topology
            {
                LeaderNode = new ServerNode
                {
                    Url = GetStringQueryString("url", required: false) ?? Server.Configuration.Core.ServerUrl,
                    Database = Database.Name,
                    DbId = Database.DbId.ToString()
                },
                Etag = -1, //TODO: check what should be here
                SLA = new TopologySla { RequestTimeThresholdInMilliseconds = 1000 }, //TODO : make this configurable?
                ReadBehavior = ReadBehavior.LeaderWithFailoverWhenRequestTimeSlaThresholdIsReached,
                WriteBehavior = WriteBehavior.LeaderOnly,
                Outgoing = nodes != null ? nodes.ToList() : new List<TopologyNode>()
            };
        }
    }
}