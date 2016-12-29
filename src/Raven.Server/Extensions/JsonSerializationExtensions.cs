using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.NewClient.Client.Http;
using Sparrow.Json.Parsing;

namespace Raven.Server.Extensions
{
    public static class JsonSerializationExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DynamicJsonValue ToJson(this Topology topology)
        {
            return new DynamicJsonValue
            {
                [nameof(Topology.LeaderNode)] = topology.LeaderNode.ToJson(),
                [nameof(Topology.Etag)] = topology.Etag,
                [nameof(Topology.WriteBehavior)] = topology.WriteBehavior.ToString(),
                [nameof(Topology.ReadBehavior)] = topology.ReadBehavior.ToString(),
                [nameof(Topology.SLA)] = topology.SLA.ToJson(),
                [nameof(Topology.Outgoing)] = new DynamicJsonArray(topology.Outgoing?.Select(ToJson))
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static DynamicJsonValue ToJson(this TopologyNode node)
        {
            var ajacencyList = new Dictionary<string, List<string>>();
            BuildTopologyGraph(node, ajacencyList);
            var nodeList = new Dictionary<string, DynamicJsonValue>();
            BuildTopologyNodeList(node, nodeList, true);

            var nodeListAsJson = new DynamicJsonValue();
            foreach (var kvp in nodeList)
                nodeListAsJson[kvp.Key] = kvp.Value;

            return new DynamicJsonValue
            {
                ["NodeList"] = nodeListAsJson,
                ["NodeAdjacency"] = ajacencyList.ToJson()
            };
        }

        private static void BuildTopologyNodeList(TopologyNode topologyNode, Dictionary<string,DynamicJsonValue> nodeList, bool isFirstNode)
        {
            DynamicJsonValue currentNodeEntry;
            if(string.IsNullOrWhiteSpace(topologyNode.Node?.DbId)) //precaution..
                throw new InvalidDataException("Cannot build topology node list since the TopologyNode records do not contain node information.");

            if (!nodeList.TryGetValue(topologyNode.Node.DbId, out currentNodeEntry))
            {
                currentNodeEntry = TopologyNodeToJson(topologyNode, isFirstNode);
                nodeList.Add(topologyNode.Node.DbId,currentNodeEntry);
            }

            if (topologyNode.Outgoing != null)
            {
                foreach (var outgoingNode in topologyNode.Outgoing)
                    if (!nodeList.ContainsKey(outgoingNode.Node.DbId))
                        BuildTopologyNodeList(outgoingNode, nodeList, false);
            }
        }

        private static void BuildTopologyGraph(TopologyNode node, Dictionary<string,List<string>> topologyAdjacencyList)
        {
            List<string> relevantAdjacency;
            if (!topologyAdjacencyList.TryGetValue(node.Node.DbId, out relevantAdjacency))
            {
                relevantAdjacency = new List<string>();
                topologyAdjacencyList.Add(node.Node.DbId, relevantAdjacency);
            }

            if (node.Outgoing != null)
            {
                foreach (var outgoingNode in node.Outgoing)
                {
                    if (!relevantAdjacency.Contains(outgoingNode.Node.DbId))
                    {
                        relevantAdjacency.Add(outgoingNode.Node.DbId);
                        BuildTopologyGraph(outgoingNode, topologyAdjacencyList);
                    }
                }
            }
        }       

        public static DynamicJsonValue ToJson(this Dictionary<string, List<string>> dict)
        {
            var json = new DynamicJsonValue();

            foreach (var kvp in dict)
                json[kvp.Key] = new DynamicJsonArray(kvp.Value);

            return json;
        }

        private static DynamicJsonValue TopologyNodeToJson(TopologyNode topologyNode, bool isFirstNode)
        {
            var specifiedCollectionsAsJson = new DynamicJsonValue();

            if (topologyNode.SpecifiedCollections != null)
            {
                foreach (var kvp in topologyNode.SpecifiedCollections)
                    specifiedCollectionsAsJson[kvp.Key] = kvp.Value;
            }

            var jsonValue = new DynamicJsonValue
            {
                [nameof(TopologyNode.Node)] = topologyNode.Node.ToJson(),
                ["IsInitialNode"] = isFirstNode,
                [nameof(TopologyNode.Disabled)] = topologyNode.Disabled,
                [nameof(TopologyNode.IgnoredClient)] = topologyNode.IgnoredClient,
                [nameof(TopologyNode.SpecifiedCollections)] = specifiedCollectionsAsJson,                
            };

            return jsonValue;
        }

        public static DynamicJsonValue ToJson(this ServerNode node)
        {
            return new DynamicJsonValue
            {
                [nameof(ServerNode.Url)] = node.Url,
                [nameof(ServerNode.ApiKey)] = node.ApiKey,
                [nameof(ServerNode.Database)] = node.Database,
                [nameof(ServerNode.DbId)] = node.DbId,
            };
        }

        public static DynamicJsonValue ToJson(this IndexDefinition definition)
        {
            var result = new DynamicJsonValue
            {
                [nameof(IndexDefinition.IndexId)] = definition.IndexId,
                [nameof(IndexDefinition.IsSideBySideIndex)] = definition.IsSideBySideIndex,
                [nameof(IndexDefinition.IsTestIndex)] = definition.IsTestIndex,
                [nameof(IndexDefinition.LockMode)] = definition.LockMode.ToString(),
                [nameof(IndexDefinition.Name)] = definition.Name,
                [nameof(IndexDefinition.Reduce)] = definition.Reduce,
                [nameof(IndexDefinition.Type)] = definition.Type.ToString(),
                [nameof(IndexDefinition.Maps)] = new DynamicJsonArray(definition.Maps)
            };

            var fields = new DynamicJsonValue();
            foreach (var kvp in definition.Fields)
            {
                DynamicJsonValue spatial = null;
                if (kvp.Value.Spatial != null)
                {
                    spatial = new DynamicJsonValue
                    {
                        [nameof(SpatialOptions.MaxTreeLevel)] = kvp.Value.Spatial.MaxTreeLevel,
                        [nameof(SpatialOptions.MaxX)] = kvp.Value.Spatial.MaxX,
                        [nameof(SpatialOptions.MaxY)] = kvp.Value.Spatial.MaxY,
                        [nameof(SpatialOptions.MinX)] = kvp.Value.Spatial.MinX,
                        [nameof(SpatialOptions.MinY)] = kvp.Value.Spatial.MinY,
                        [nameof(SpatialOptions.Strategy)] = kvp.Value.Spatial.Strategy.ToString(),
                        [nameof(SpatialOptions.Type)] = kvp.Value.Spatial.Type.ToString(),
                        [nameof(SpatialOptions.Units)] = kvp.Value.Spatial.Units.ToString()
                    };
                }

                var field = new DynamicJsonValue
                {
                    [nameof(IndexFieldOptions.Analyzer)] = kvp.Value.Analyzer,
                    [nameof(IndexFieldOptions.Indexing)] = kvp.Value.Indexing?.ToString(),
                    [nameof(IndexFieldOptions.Sort)] = kvp.Value.Sort?.ToString(),
                    [nameof(IndexFieldOptions.Spatial)] = spatial,
                    [nameof(IndexFieldOptions.Storage)] = kvp.Value.Storage?.ToString(),
                    [nameof(IndexFieldOptions.Suggestions)] = kvp.Value.Suggestions,
                    [nameof(IndexFieldOptions.TermVector)] = kvp.Value.TermVector?.ToString()
                };

                fields[kvp.Key] = field;
            }

            result[nameof(IndexDefinition.Fields)] = fields;

            var settings = new DynamicJsonValue();
            foreach (var kvp in definition.Configuration)
                settings[kvp.Key] = kvp.Value;

            result[nameof(IndexDefinition.Configuration)] = settings;

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static DynamicJsonValue ToJson(this TopologySla sla)
        {
            return new DynamicJsonValue
            {
                [nameof(TopologySla.RequestTimeThresholdInMilliseconds)] = sla?.RequestTimeThresholdInMilliseconds
            };
        }
    }
}