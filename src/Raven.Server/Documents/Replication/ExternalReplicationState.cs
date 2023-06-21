using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class ExternalReplicationState : IDatabaseTaskStatus
    {
        public long TaskId { get; set; }

        public string NodeTag { get; set; }

        public long LastSentEtag { get; set; }

        public string SourceChangeVector { get; set; }

        public string DestinationChangeVector { get; set; }

        public string SourceDatabaseName { get; set; }

        public string SourceDatabaseId { get; set; }

        public static string GenerateItemName(string databaseName, long taskId)
        {
            return $"values/{databaseName}/external-replication/{taskId}";
        }

        public static string GenerateItemName(string databaseName, string sourceDatabaseName, string sourceDatabaseId)
        {
            return $"values/{databaseName}/sharded-incoming-external-replication/{sourceDatabaseName}/{sourceDatabaseId}";
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
                [nameof(SourceDatabaseName)] = SourceDatabaseName,
                [nameof(SourceDatabaseId)] = SourceDatabaseId
            };
        }
    }
}
