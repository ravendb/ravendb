using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Replication.Messages
{
    public class FullTopologyInfo
    {
        public Dictionary<string, NodeTopologyInfo> NodesByDbId;
    }

    /// <summary>
    /// contains all adjacent (both incoming and outgoing) connections of a specific node
    /// </summary>
    public class NodeTopologyInfo
    {
        public Dictionary<string, ActiveNodeStatus> OutgoingByDbId;
        public Dictionary<string, ActiveNodeStatus> IncomingByIncomingDbId;

        public Dictionary<string, InactiveNodeStatus> OfflineByUrlAndDatabase;

        public NodeTopologyInfo()
        {
            OutgoingByDbId = new Dictionary<string, ActiveNodeStatus>();
            IncomingByIncomingDbId = new Dictionary<string, ActiveNodeStatus>();
            OfflineByUrlAndDatabase = new Dictionary<string, InactiveNodeStatus>();
        }

        public DynamicJsonValue ToJson()
        {
            var outgoingByDbIdJson = new DynamicJsonValue();
            foreach (var kvp in OutgoingByDbId)
                outgoingByDbIdJson[kvp.Key] = kvp.Value.ToJson();

            var incomingByDbIdJson = new DynamicJsonValue();
            foreach (var kvp in IncomingByIncomingDbId)
                incomingByDbIdJson[kvp.Key] = kvp.Value.ToJson();

            var offlineByUrlAndDatabaseJson = new DynamicJsonValue();
            foreach (var kvp in OfflineByUrlAndDatabase)
                offlineByUrlAndDatabaseJson[kvp.Key] = kvp.Value.ToJson();

            return new DynamicJsonValue
            {
                [nameof(OutgoingByDbId)] = outgoingByDbIdJson,
                [nameof(IncomingByIncomingDbId)] = incomingByDbIdJson,
                [nameof(OfflineByUrlAndDatabase)] = offlineByUrlAndDatabaseJson
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
