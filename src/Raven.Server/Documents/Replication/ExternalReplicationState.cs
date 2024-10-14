using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public sealed class ExternalReplicationState : IDatabaseTaskStatus
    {
        public long TaskId { get; set; }

        public string NodeTag { get; set; }

        public long LastSentEtag { get; set; }

        public string SourceChangeVector { get; set; }

        public string DestinationChangeVector { get; set; }

        public string FromToString { get; set; }

        public static string GenerateItemName(string databaseName, long taskId)
        {
            return $"values/{databaseName}/external-replication/{taskId}";
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(TaskId)] = TaskId,
                [nameof(NodeTag)] = NodeTag,
                [nameof(LastSentEtag)] = LastSentEtag,
                [nameof(SourceChangeVector)] = SourceChangeVector,
                [nameof(DestinationChangeVector)] = DestinationChangeVector,
                [nameof(FromToString)] = FromToString
            };
        }
    }
}
