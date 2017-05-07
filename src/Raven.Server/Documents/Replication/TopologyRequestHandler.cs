using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class TopologyRequestHandler
    {
        public void AcceptIncomingConnectionAndRespond(TcpConnectionOptions tcp, string debugTag)
        {
            DocumentsOperationContext context;
            using (tcp)
            using(tcp.ConnectionProcessingInProgress(debugTag))
            using (tcp.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                TopologyDiscoveryRequest header;
                var localDbId = tcp.DocumentDatabase.DbId.ToString();
                using (var headerJson = context.ParseToMemory(
                    tcp.Stream,
                    "ReplicationTopologDiscovery/read-discovery-header",
                    BlittableJsonDocumentBuilder.UsageMode.None,
                    tcp.PinnedBuffer))
                {
                    headerJson.BlittableValidation();
                    header = JsonDeserializationServer.TopologyDiscoveryRequest(headerJson);
                }

                if (header.AlreadyVisited.Contains(localDbId))
                {
                    WriteDiscoveryResponse(tcp, context, new LiveTopologyInfo
                    {
                        DatabaseId = localDbId
                    }, null, TopologyDiscoveryResponseHeader.Status.AlreadyKnown);
                    return;
                }

                header.AlreadyVisited.Add(localDbId);
                var destinations = tcp.DocumentDatabase.ReplicationLoader?.Destinations?.ToList();

                //This is the case where we don't have real replication topology.
                if (destinations == null || destinations.Count == 0)
                {
                    //return record of node without any incoming or outgoing connections
                    var localNodeTopologyInfo = new NodeTopologyInfo();
                    var topology = new LiveTopologyInfo
                    {
                        DatabaseId = localDbId,
                        NodesById =
                        {
                            { tcp.DocumentDatabase.DbId.ToString(), localNodeTopologyInfo }
                        }
                    };
                    ReplicationUtils.GetLocalIncomingTopology(tcp.DocumentDatabase.ReplicationLoader, localNodeTopologyInfo);
                    WriteDiscoveryResponse(tcp, context, topology, null, TopologyDiscoveryResponseHeader.Status.Ok);

                    return;
                }

                var localTopology = ReplicationUtils.GetLocalTopology(tcp.DocumentDatabase, tcp.DocumentDatabase.ReplicationLoader.Destinations);

                using (var topologyDiscoverer = new ClusterTopologyExplorer(
                    tcp.DocumentDatabase,
                    header.AlreadyVisited,
                    TimeSpan.FromMilliseconds(header.Timeout),
                    destinations))
                {
                    try
                    {
                        var discoveryTask = topologyDiscoverer.DiscoverTopologyAsync();
                        discoveryTask.Wait(tcp.DocumentDatabase.DatabaseShutdown);
                        var topology = GetLiveTopologyWithLocalNodes(localDbId, localTopology);
                        foreach (var nodeInfo in discoveryTask.Result.NodesById)
                            topology.NodesById[nodeInfo.Key] = nodeInfo.Value;

                        WriteDiscoveryResponse(tcp, context, topology, null, TopologyDiscoveryResponseHeader.Status.Ok);
                    }
                    catch (Exception e)
                    {
                        var topology = GetLiveTopologyWithLocalNodes(localDbId, localTopology);
                        WriteDiscoveryResponse(
                            tcp,
                            context,
                            topology,
                            e.ToString(),
                            TopologyDiscoveryResponseHeader.Status.Error);
                    }
                }
            }
        }

        private static LiveTopologyInfo GetLiveTopologyWithLocalNodes(string localDbId, NodeTopologyInfo localTopology)
        {
            var topology = new LiveTopologyInfo
            {
                DatabaseId = localDbId
            };
            topology.NodesById.Add(localDbId, localTopology);
            return topology;
        }

        
        private static void WriteDiscoveryResponse(
            TcpConnectionOptions tcp, 
            JsonOperationContext context, 
            LiveTopologyInfo liveTopology, 
            string exception, 
            TopologyDiscoveryResponseHeader.Status responseStatus)
        {
            using (var writer = new BlittableJsonTextWriter(context, tcp.Stream))
            {
                context.Write(writer,new DynamicJsonValue
                {
                    [nameof(TopologyDiscoveryResponseHeader.Type)] = responseStatus.ToString(),
                    [nameof(TopologyDiscoveryResponseHeader.Exception)] = exception
                });

                context.Write(writer, liveTopology.ToJson());
                writer.Flush();
            }
        }

        
    }
}
