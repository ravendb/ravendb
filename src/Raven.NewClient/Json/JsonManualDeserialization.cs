using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Replication.Messages;
using Sparrow.Json;

namespace Raven.NewClient.Json
{
    public class JsonManualDeserialization : JsonDeserializationBase
    {
        public static Topology ConvertToTopology(BlittableJsonReaderObject json)
        {
            var topologyInfoConverter =
                (Func<BlittableJsonReaderObject, TopologyInfo>)GetConverterFromCache(typeof(TopologyInfo));

            var topology = topologyInfoConverter(json).ToTopology();

            BlittableJsonReaderArray outgoingCollection;
            if (!json.TryGet(nameof(Topology.Outgoing), out outgoingCollection))
            {
                throw new InvalidDataException($"Expected to find outgoing information in a property '{nameof(Topology.Outgoing)}', DbId of the node = {topology.LeaderNode.DbId}");
            }

            foreach(var nodeJson in outgoingCollection.Cast<BlittableJsonReaderObject>())
                topology.Outgoing.Add(ConvertToTopologyNode(nodeJson));

            return topology;
        }

        private static readonly Func<BlittableJsonReaderObject, TopologyNodeInfo> nodeInfoConverter =
            (Func<BlittableJsonReaderObject, TopologyNodeInfo>)GetConverterFromCache(typeof(TopologyNodeInfo));

        public static TopologyDiscoveryResponse ConvertToDiscoveryResponse(BlittableJsonReaderObject json)
        {
            var node = ToObject(json, nameof(TopologyDiscoveryResponse.DiscoveredTopology),ConvertToTopologyNode);
            string fromDbId;
            if(!json.TryGet(nameof(TopologyDiscoveryResponse.FromDbId),out fromDbId))
                throw new MissingFieldException($"Missing field named '{nameof(TopologyDiscoveryResponse.FromDbId)}'");

            string responseTypeAsString;
            if(!json.TryGet(nameof(TopologyDiscoveryResponse.DiscoveryResponseType),out responseTypeAsString))
                throw new MissingFieldException($"Missing field named '{nameof(TopologyDiscoveryResponse.DiscoveryResponseType)}'");

            TopologyDiscoveryResponse.ResponseType responseType;
            if (!Enum.TryParse(responseTypeAsString, out responseType))
            {
                throw new InvalidDataException($"Invalid enum value for field named '");
            }

            return new TopologyDiscoveryResponse
            {
                FromDbId = fromDbId,
                DiscoveredTopology = node,
                DiscoveryResponseType = responseType
            };
        }

        public static TopologyNode ConvertToTopologyNode(BlittableJsonReaderObject json)
        {
            var nodeList = ToDictionary(json, "NodeList", nodeInfoConverter);
            var adjacencyList = ToDictionaryOfStringArray(json, "NodeAdjacency");

            var initialNode = nodeList.FirstOrDefault(n => n.Value.IsInitialNode);
            var topologyNode = initialNode.Value.ToTopologyNode();
            if(topologyNode == null)
                throw new InvalidDataException("Invalid topology, no nodes marked as 'initial'");

            BuildTopologyGraph(topologyNode,nodeList,adjacencyList, new HashSet<string>());

            return topologyNode;
        }

        private static void BuildTopologyGraph(
            TopologyNode current, 
            Dictionary<string, TopologyNodeInfo> nodeList,
            Dictionary<string, string[]> adjacencyList,
            HashSet<string> visitedNodes)
        {
            if (visitedNodes.Contains(current.Node.DbId))
                return;

            string[] currentAdjacencyList;
            if (!adjacencyList.TryGetValue(current.Node.DbId, out currentAdjacencyList))
            {
                throw new InvalidDataException($@"Couldn't find an adjacency list for node with DbId = {current.Node.DbId}.
                                                    This is not supposed to happen and is likely a bug.");
            }
            visitedNodes.Add(current.Node.DbId);

            foreach (var dbId in currentAdjacencyList)
            {
                TopologyNodeInfo topologyNodeInfo;
                if (!nodeList.TryGetValue(dbId, out topologyNodeInfo))
                {
                    throw new InvalidDataException($@"Couldn't find a topology node info with DbId = {current.Node.DbId}.
                                                         This is not supposed to happen and is likely a bug.");
                }

                var topologyNode = topologyNodeInfo.ToTopologyNode();
                current.Outgoing.Add(topologyNode);

                BuildTopologyGraph(topologyNode,nodeList,adjacencyList, visitedNodes);
            }
        }

        private class TopologyInfo
        {
            public long Etag;
            public ServerNode LeaderNode;
            public ReadBehavior ReadBehavior;
            public WriteBehavior WriteBehavior;
            public TopologySla SLA;

            public Topology ToTopology()
            {
                return new Topology
                {
                    Outgoing = new List<TopologyNode>(),
                    Etag = Etag,
                    LeaderNode = LeaderNode,
                    ReadBehavior = ReadBehavior,
                    SLA = SLA,
                    WriteBehavior = WriteBehavior
                };
            }
        }

        // ReSharper disable MemberCanBePrivate.Local
        private class TopologyNodeInfo
        {
            public bool IsInitialNode;

            public ServerNode Node;

            public Dictionary<string, string> SpecifiedCollections;

            public bool IgnoredClient;

            public bool Disabled;

            public TopologyNode ToTopologyNode()
            {
                return new TopologyNode
                {
                    Node = Node,
                    Outgoing = new List<TopologyNode>(),
                    SpecifiedCollections = SpecifiedCollections,
                    IgnoredClient = IgnoredClient,
                    Disabled = Disabled
                };
            }
        }
        // ReSharper restore MemberCanBePrivate.Local        
    }
}
