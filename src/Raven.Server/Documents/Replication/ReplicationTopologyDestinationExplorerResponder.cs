using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Replication.Messages;
using Raven.NewClient.Abstractions.Extensions;
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
                using (var headerJson = multiDocumentParser.ParseToMemory("ReplicationTopologDiscovery/read-discovery-header"))
                {
                    headerJson.BlittableValidation();
                    header = JsonDeserializationServer.TopologyDiscoveryRequest(headerJson);
                }

                List<string> alreadyVisitedByOrigin;
                if (header.AlreadyVisited.TryGetValue(header.OriginDbId, out alreadyVisitedByOrigin) &&
                    alreadyVisitedByOrigin.Contains(tcp.DocumentDatabase.DbId.ToString()))
                {
                    WriteDiscoveryResponse(tcp, context, new List<NodeTopologyInfo>(), TopologyDiscoveryResponse.Status.AlreadyKnown);
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
                        WriteDiscoveryResponse(tcp, context, new List<NodeTopologyInfo>(), TopologyDiscoveryResponse.Status.Leaf);
                        return;
                    }

                    //here we need to construct the topology from the replication document 
                    replicationDocument = JsonDeserializationServer.ReplicationDocument(configurationDocument.Data);
                }
                // outgoing, last heartbeat time, etag, global change vector
                // destinations, may be inactive, need to test them as well, if not already in outgoing
                // incoming, last heartbeat time, etag 

                // if error, what is the error in connection
                // if timeout, what is the timeout value                
                List<ReplicationDestination> activeDestinations;
                var localTopology = ReplicationUtils.GetLocalTopology(
                    tcp.DocumentDatabase, replicationDocument, context, out activeDestinations);

                using (var topologyDiscoverer = new ReplicationClusterTopologyExplorer(
                    tcp.DocumentDatabase,
                    header.AlreadyVisited,
                    header.Timeout, 
                    activeDestinations))
                {
                    //if true, all adjacent nodes are already visited, so no need to continue
                    //(filtering is done in a ctor of ReplicationTopologyDiscoverer, using header.AlreadyVisited)
                    if (topologyDiscoverer.DiscovererCount == 0)
                    {
                        WriteDiscoveryResponse(tcp, context, new List<NodeTopologyInfo> { localTopology },TopologyDiscoveryResponse.Status.Ok);
                        return;
                    }

                    //TODO: add proper exception handling here 
                    var discoveryTask = topologyDiscoverer.DiscoverTopologyAsync();
                    discoveryTask.Wait(tcp.DocumentDatabase.DatabaseShutdown);

                    var discoveredTopology = discoveryTask.Result;
                    discoveredTopology.Add(localTopology);
                    WriteDiscoveryResponse(tcp, context, discoveredTopology, TopologyDiscoveryResponse.Status.Ok);
                }
            }
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

            foreach (var outgoing in tcp.DocumentDatabase.DocumentReplicationLoader.OutgoingHandlers)
                if(!visitedDbIds.Contains(outgoing.DestinationDbId))
                    visitedDbIds.Add(outgoing.DestinationDbId);
        }

        

        private static void WriteDiscoveryResponse(
            TcpConnectionOptions tcp,
            JsonOperationContext context,
            IReadOnlyList<NodeTopologyInfo> nodesTopologyInfo,
            TopologyDiscoveryResponse.Status responseStatus)
        {
            using (var writer = new BlittableJsonTextWriter(context, tcp.Stream))
            {
                context.Write(writer,new DynamicJsonValue
                {
                    [nameof(TopologyDiscoveryResponse.DiscoveryStatus)] = (int)responseStatus
                });

                var mergedTopology = ReplicationUtils.Merge(nodesTopologyInfo);
                context.Write(writer, mergedTopology.ToJson());
                writer.Flush();
            }
        }

        
    }
}
