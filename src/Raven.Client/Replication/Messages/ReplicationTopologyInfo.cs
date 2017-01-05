using System;
using System.Collections.Generic;
using Raven.Abstractions.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Replication.Messages
{
    public class FullTopologyInfo
    {
        public string DatabaseId;

        public Dictionary<string, NodeTopologyInfo> NodesById = new Dictionary<string, NodeTopologyInfo>();

        public List<InactiveNodeStatus> FailedToReach = new List<InactiveNodeStatus>();

        public DynamicJsonValue ToJson()
        {
            var nodesByDbIdJson = new DynamicJsonValue();
            foreach (var kvp in NodesById)
                nodesByDbIdJson[kvp.Key] = kvp.Value.ToJson();

            var failedToReachJson = new DynamicJsonArray();
            foreach (var kvp in FailedToReach)
                failedToReachJson.Add(kvp.ToJson());

            return new DynamicJsonValue
            {
                [nameof(DatabaseId)] = DatabaseId,
                [nameof(NodesById)] = nodesByDbIdJson,
                [nameof(FailedToReach)] = failedToReachJson
            };
        }
    }

    public class NodeTopologyInfo
    {
        public string DatabaseId;

        public List<ActiveNodeStatus> Outgoing;
        public List<ActiveNodeStatus> Incoming;
        public List<InactiveNodeStatus> Offline;

        public NodeTopologyInfo()
        {
            Outgoing = new List<ActiveNodeStatus>();
            Incoming = new List<ActiveNodeStatus>();
            Offline = new List<InactiveNodeStatus>();
        }

        public DynamicJsonValue ToJson()
        {
            var outgoingJson = new DynamicJsonArray();
            foreach (var outgoing in Outgoing)
            {
                outgoingJson.Add(outgoing.ToJson());
            }

            var incomingJson = new DynamicJsonArray();
            foreach (var incoming in Incoming)
            {
                incomingJson.Add(incoming.ToJson());
            }

            var offlineJson = new DynamicJsonArray();
            foreach (var offline in Offline)
            {
                offlineJson.Add(offline.ToJson());
            }

            return new DynamicJsonValue
            {
                [nameof(Outgoing)] = outgoingJson,
                [nameof(Incoming)] = incomingJson,
                [nameof(Offline)] = offlineJson
            };
        }
    }

    public class InactiveNodeStatus
    {
        public string Exception;

        public string Url;

        public string Message;

        public string Database;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Exception)] = Exception,
                [nameof(Message)] = Message,
                [nameof(Url)] = Url,
                [nameof(Database)] = Database
            };
        }
    }

    public class ActiveNodeStatus
    {
        public string DbId;

        public bool IsCurrentlyConnected;

        public long LastDocumentEtag;

        public long LastIndexTransformerEtag;

        public long LastHeartbeatTicks;

        public string LastException;

        public string Url;

        public string Database;

        public Status NodeStatus;

        public enum Status
        {
            Online,
            Timeout,
            Error
        }


        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(IsCurrentlyConnected)] = IsCurrentlyConnected,
                [nameof(DbId)] = DbId,
                [nameof(Database)] = Database,
                [nameof(Url)] = Url,
                [nameof(LastException)] = LastException,
                [nameof(LastHeartbeatTicks)] = LastHeartbeatTicks,
                [nameof(LastDocumentEtag)] = LastDocumentEtag,
                [nameof(LastIndexTransformerEtag)] = LastIndexTransformerEtag,
                [nameof(NodeStatus)] = NodeStatus.ToString(),
                ["LastHeartbeat"] = new DateTime(LastHeartbeatTicks).GetDefaultRavenFormat(),
            };
        }
    }

}
