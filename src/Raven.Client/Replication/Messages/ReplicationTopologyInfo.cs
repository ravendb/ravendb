using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Replication.Messages
{
    public class FullTopologyInfo
    {
        public string LeaderDbId;

        public Dictionary<string, NodeTopologyInfo> NodesByDbId;

        internal FullTopologyInfo()
        {
        }

        public FullTopologyInfo(string leaderDbId)
        {
            LeaderDbId = leaderDbId;
            NodesByDbId = new Dictionary<string, NodeTopologyInfo>();
        }

        public DynamicJsonValue ToJson()
        {
            var nodesByDbIdJson = new DynamicJsonValue();
            foreach (var kvp in NodesByDbId)
                nodesByDbIdJson[kvp.Key] = kvp.Value.ToJson();

            return new DynamicJsonValue
            {
                [nameof(LeaderDbId)] = LeaderDbId,
                [nameof(NodesByDbId)] = nodesByDbIdJson
            };
        }
    }

    public class NodeTopologyInfo
    {
        public string OriginDbId;

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
                outgoingJson.Add(outgoing.ToJson());

            var incomingJson = new DynamicJsonArray();
            foreach (var incoming in Incoming)
                incomingJson.Add(incoming.ToJson());

            var offlineJson = new DynamicJsonArray();
            foreach (var offline in Offline)
                offlineJson.Add(offline.ToJson());

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

        public string Database;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Exception)] = Exception,
                [nameof(Url)] = Url,
                [nameof(Database)] = Database
            };
        }
    }

    public class ActiveNodeStatus
    {
        public string DbId;

        public bool IsOnline; //is being replicated to actively

        public long LastDocumentEtag;

        public long LastIndexTransformerEtag;

        public long LastHeartbeatTicks;

        public Dictionary<string, long> GlobalChangeVector;

        public string LastException;

        public Status NodeStatus;

        public enum Status
        {
            Online,
            Timeout,
            Error
        }

        public ActiveNodeStatus()
        {
            GlobalChangeVector = new Dictionary<string, long>();
        }

        public DynamicJsonValue ToJson()
        {
            var globalChangeVector = new DynamicJsonValue();
            foreach (var kvp in GlobalChangeVector)
                globalChangeVector[kvp.Key] = kvp.Value;

            return new DynamicJsonValue
            {
                [nameof(IsOnline)] = IsOnline,
                [nameof(DbId)] = DbId,
                [nameof(LastException)] = LastException,
                [nameof(LastHeartbeatTicks)] = LastHeartbeatTicks,
                [nameof(LastDocumentEtag)] = LastDocumentEtag,
                [nameof(LastIndexTransformerEtag)] = LastIndexTransformerEtag,
                [nameof(NodeStatus)] = (int)NodeStatus,
                [nameof(GlobalChangeVector)] = globalChangeVector
            };
        }
    }

}
