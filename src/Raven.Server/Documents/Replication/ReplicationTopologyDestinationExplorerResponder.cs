using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationTopologyDestinationExplorerResponder
    {
        public void AcceptIncomingConnectionAndRespond(TcpConnectionOptions tcp)
        {
            DocumentsOperationContext context;
            using (tcp)
            using (var multiDocumentParser = tcp.MultiDocumentParser)
            using (tcp.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                TopologyDiscoveryRequest header;
                var localDbId = tcp.DocumentDatabase.DbId.ToString();
                using (var headerJson = multiDocumentParser.ParseToMemory("ReplicationTopologDiscovery/read-discovery-header"))
                {
                    headerJson.BlittableValidation();
                    header = JsonDeserializationServer.TopologyDiscoveryRequest(headerJson);
                }

                List<string> alreadyVisitedByOrigin;
                
                if (header.AlreadyVisited.TryGetValue(header.OriginDbId, out alreadyVisitedByOrigin) &&
                    alreadyVisitedByOrigin.Contains(localDbId))
                {
                    WriteDiscoveryResponse(tcp, context, new FullTopologyInfo(localDbId), null , TopologyDiscoveryResponse.Status.AlreadyKnown);
                    return;
                }

                //prevent loops 
                UpdateAlreadyVisitedWithLocalDestinations(header.OriginDbId, tcp, header.AlreadyVisited);

                ReplicationDocument replicationDocument;
                using (context.OpenReadTransaction())
                {
                    var configurationDocument = tcp.DocumentDatabase.DocumentsStorage.Get(context, Constants.Replication.DocumentReplicationConfiguration);
                    //This is the case where we don't have real replication topology.
                    if (configurationDocument == null)
                    {
                        //return record of node without any incoming or outgoing connections
                        WriteDiscoveryResponse(tcp, context, new FullTopologyInfo(localDbId)
                        {
                            NodesByDbId =
                            {
                                { tcp.DocumentDatabase.DbId.ToString(), new NodeTopologyInfo() }
                            }
                        }, null , TopologyDiscoveryResponse.Status.Leaf);
                        return;
                    }

                    replicationDocument = JsonDeserializationServer.ReplicationDocument(configurationDocument.Data);
                }

                NodeTopologyInfo localTopology;

                using(context.OpenReadTransaction())
                    localTopology = ReplicationUtils.GetLocalTopology(
                        tcp.DocumentDatabase, replicationDocument, context);

                using (var topologyDiscoverer = new ReplicationClusterTopologyExplorer(
                    tcp.DocumentDatabase,
                    header.AlreadyVisited,
                    TimeSpan.FromMilliseconds(header.Timeout),
                    replicationDocument.Destinations))
                {
                    //if true, all adjacent nodes are already visited, so no need to continue
                    //(filtering is done in a ctor of ReplicationTopologyDiscoverer, using header.AlreadyVisited)
                    if (topologyDiscoverer.DiscovererCount == 0)
                    {
                        var topology = GetFullTopologyWithLocalNodes(localDbId, localTopology);
                        WriteDiscoveryResponse(tcp, context, topology, null , TopologyDiscoveryResponse.Status.Ok);
                        return;
                    }

                    try
                    {
                        var discoveryTask = topologyDiscoverer.DiscoverTopologyAsync();
                        discoveryTask.Wait(tcp.DocumentDatabase.DatabaseShutdown);

                        var topology = GetFullTopologyWithLocalNodes(localDbId, localTopology);
                        foreach (var nodeInfo in discoveryTask.Result.NodesByDbId)
                            topology.NodesByDbId[nodeInfo.Key] = nodeInfo.Value;

                        WriteDiscoveryResponse(tcp, context, topology, null , TopologyDiscoveryResponse.Status.Ok);
                    }
                    catch (Exception e)
                    {
                        var topology = GetFullTopologyWithLocalNodes(localDbId, localTopology);
                        WriteDiscoveryResponse(
                            tcp, 
                            context,
                            topology,
                            e.ToString(),
                            TopologyDiscoveryResponse.Status.Error);
                    }
                }
            }
        }

        private static FullTopologyInfo GetFullTopologyWithLocalNodes(string localDbId, NodeTopologyInfo localTopology)
        {
            var topology = new FullTopologyInfo(localDbId);
            topology.NodesByDbId.Add(localDbId, localTopology);
            return topology;
        }

        private void UpdateAlreadyVisitedWithLocalDestinations(
            string originDbId,
            TcpConnectionOptions tcp,
            Dictionary<string, List<string>> alreadyVisited)
        {
            List<string> visitedDbIds;
            if (!alreadyVisited.TryGetValue(originDbId, out visitedDbIds))
            {
                visitedDbIds = new List<string>();
                alreadyVisited.Add(originDbId,visitedDbIds);
            }

            var localDbId = tcp.DocumentDatabase.DbId.ToString();
            if (!visitedDbIds.Contains(localDbId))
                visitedDbIds.Add(localDbId);
        }

        private static void WriteDiscoveryResponse(
            TcpConnectionOptions tcp, 
            JsonOperationContext context, 
            FullTopologyInfo fullTopology, 
            string exception, 
            TopologyDiscoveryResponse.Status responseStatus)
        {
            using (var writer = new BlittableJsonTextWriter(context, tcp.Stream))
            {
                context.Write(writer,new DynamicJsonValue
                {
                    [nameof(TopologyDiscoveryResponse.DiscoveryStatus)] = (int)responseStatus,
                    [nameof(TopologyDiscoveryResponse.Exception)] = exception
                });

                context.Write(writer, fullTopology.ToJson());
                writer.Flush();
            }
        }

        
    }
}
