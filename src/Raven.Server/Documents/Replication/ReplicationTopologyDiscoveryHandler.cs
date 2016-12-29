using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Replication.Messages;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class ReplicationTopologyDiscoveryHandler
    {
        private readonly Guid _dbId;

        public ReplicationTopologyDiscoveryHandler(Guid dbId)
        {
            _dbId = dbId;
        }

        public void AcceptIncomingConnectionAndRespond(TcpConnectionOptions tcp)
        {
            JsonOperationContext context;
            using (tcp)
            using (var multiDocumentParser = tcp.MultiDocumentParser)
            using (tcp.DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                TopologyDiscoveryHeader header;
                using (var headerJson = multiDocumentParser.ParseToMemory("ReplicationTopologDiscovery/read-discovery-header"))
                    header = JsonDeserializationServer.DiscoveryHeader(headerJson);

                var outgoingHandlers = tcp.DocumentDatabase.DocumentReplicationLoader.Outgoing;

                //if this node is a sink, no need to continue
                if(outgoingHandlers.Count == 0)
                {
                    WriteDiscoveryResponse(tcp, context, new List<TopologyNode>(), TopologyDiscoveryResponse.ResponseType.Ok);
                    return;
                }

                var token = tcp.DocumentDatabase.DatabaseShutdown;
                using (var topologyDiscoverer = new ReplicationTopologyDiscoverer(
                    outgoingHandlers,
                    header.AlreadyVisitedDbIds,
                    token))
                {
                    //if true, all adjacent nodes are already visited, so no need to continue
                    //(filtering is done in a ctor of ReplicationTopologyDiscoverer, using header.AlreadyVisitedDbIds)
                    if (topologyDiscoverer.DiscovererCount == 0)
                    {
                        WriteDiscoveryResponse(tcp, context, new List<TopologyNode>(), TopologyDiscoveryResponse.ResponseType.Ok);
                        return;
                    }

                    var discoveryTask = topologyDiscoverer.DiscoverTopologyAsync();

                    List<TopologyNode> nodes;
                    TopologyDiscoveryResponse.ResponseType responseType;

                    //if true, then the discovery is taking too long..
                    //TODO : make topology discovery timeout configurable
                    if (Task.WaitAny(discoveryTask, Task.Delay(TimeSpan.FromSeconds(60), token)) != discoveryTask.Id)
                    {
                        responseType = TopologyDiscoveryResponse.ResponseType.Timeout;
                        nodes = Enumerable.Empty<TopologyNode>().ToList();
                    }
                    else
                    {
                        responseType = TopologyDiscoveryResponse.ResponseType.Ok;
                        nodes = discoveryTask.Result.ToList();

                        var replicationHandlers = tcp.DocumentDatabase.DocumentReplicationLoader.Outgoing;
                        foreach (var node in nodes)
                        {
                            var relevantReplicationHandler = replicationHandlers.FirstOrDefault(h =>
                                                                        h.DestinationDbId == node.Node.DbId);

                            //precaution, if this will be true I will be surprised..
                            if (relevantReplicationHandler == null)
                            {
                                throw new InvalidDataException($@"Couldn't find outgoing replication handler for DbId = {node.Node.DbId}. 
                                                                  This is not supposed to happen and is likely a bug.");
                            }

                            node.SpecifiedCollections = relevantReplicationHandler.Destination.SpecifiedCollections;
                            node.Disabled = relevantReplicationHandler.Destination.Disabled;
                            node.IgnoredClient = relevantReplicationHandler.Destination.IgnoredClient;
                            node.Node.ApiKey = relevantReplicationHandler.Destination.ApiKey;
                            node.Node.Url = relevantReplicationHandler.Destination.Url;
                        }
                    }

                    WriteDiscoveryResponse(tcp, context, nodes, responseType);
                }
            }
        }

        private static void WriteDiscoveryResponse(TcpConnectionOptions tcp, JsonOperationContext context, List<TopologyNode> nodes, TopologyDiscoveryResponse.ResponseType responseType)
        {
            using (var writer = new BlittableJsonTextWriter(context, tcp.Stream))
            {
                context.Write(writer,
                    new DynamicJsonValue
                    {
                        [nameof(TopologyDiscoveryResponse.DiscoveredTopology)] = new TopologyNode
                        {
                            Outgoing = nodes.ToList(),
                            Node = new ServerNode
                            {
                                DbId = tcp.DocumentDatabase.DbId.ToString(),
                                Database = tcp.DocumentDatabase.Name
                            },
                            SpecifiedCollections = new Dictionary<string, string>()
                        }.ToJson(),
                        [nameof(TopologyDiscoveryResponse.FromDbId)] = tcp.DocumentDatabase.DbId.ToString(),
                        [nameof(TopologyDiscoveryResponse.DiscoveryResponseType)] = responseType.ToString()
                    });
                writer.Flush();
            }
        }
    }
}

