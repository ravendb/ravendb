using System.Collections.Generic;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication
{
    public class ShardedExternalReplicationState : IDatabaseTaskStatus
    {
        public string NodeTag { get; set; }

        public string SourceShardedDatabaseId { get; set; }

        public string SourceDatabaseName { get; set; }

        public Dictionary<string, ShardedExternalReplicationStateForSingleSource> ReplicationStates { get; set; }

        public static string GenerateShardedItemName(string databaseName, string sourceDatabaseName, string sourceDatabaseId)
        {
            return $"values/{databaseName}/sharded-incoming-external-replication/{sourceDatabaseName}/{sourceDatabaseId}";
        }

        private DynamicJsonValue BuildReplicationStatesJson()
        {
            var json = new DynamicJsonValue();
            foreach (var item in ReplicationStates)
            {
                json[item.Key] = item.Value.ToJson();
            }

            return json;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(SourceShardedDatabaseId)] = SourceShardedDatabaseId,
                [nameof(SourceDatabaseName)] = SourceDatabaseName,
                [nameof(ReplicationStates)] = BuildReplicationStatesJson()
            };
        }
    }

    public class ShardedExternalReplicationStateForSingleSource
    {
        public long LastSourceEtag { get; set; }

        public string LastSourceChangeVector { get; set; }

        public Dictionary<string, ExternalReplicationState> DestinationStates { get; set; }

        public DynamicJsonValue BuildDestinationStatesJson()
        {
            var json = new DynamicJsonValue();
            foreach (var item in DestinationStates)
            {
                json[item.Key] = item.Value.ToJson();
            }

            return json;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastSourceEtag)] = LastSourceEtag,
                [nameof(LastSourceChangeVector)] = LastSourceChangeVector,
                [nameof(DestinationStates)] = BuildDestinationStatesJson()
            };
        }
    }
}
