using System;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public sealed class ExternalReplicationState : IDatabaseTaskStatus
    {
        public ReplicationStateType Type { get; set; }

        public long TaskId { get; set; }

        public string NodeTag { get; set; }

        public long LastSentEtag { get; set; }

        public string SourceChangeVector { get; set; }

        public string DestinationChangeVector { get; set; }

        public string FromToString { get; set; }

        public enum ReplicationStateType
        {
            ExternalReplication,
            HubCursor,
            SinkCursor
        }

        private static string GetByType(ReplicationStateType type) => type switch
        {
            ReplicationStateType.ExternalReplication => "external-replication",
            ReplicationStateType.HubCursor => "hub-cursor",
            ReplicationStateType.SinkCursor => "sink-cursor",
            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };

        public static string GenerateItemName(string databaseName, long taskId, ReplicationStateType type = ReplicationStateType.ExternalReplication)
        {
            return $"values/{databaseName}/{GetByType(type)}/{taskId}";
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TaskId)] = TaskId,
                [nameof(Type)] = Type,
                [nameof(NodeTag)] = NodeTag,
                [nameof(LastSentEtag)] = LastSentEtag,
                [nameof(SourceChangeVector)] = SourceChangeVector,
                [nameof(DestinationChangeVector)] = DestinationChangeVector,
                [nameof(FromToString)] = FromToString
            };
        }
    }
}
