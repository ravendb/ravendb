using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class SetIndexPriorityCommand : UpdateDatabaseCommand
    {
        public string IndexName;

        public IndexPriority Priority;

        public SetIndexPriorityCommand()
        {
            // for deserialization
        }

        public SetIndexPriorityCommand(string name, IndexPriority priority, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            IndexName = name;
            Priority = priority;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            if (record.Indexes.TryGetValue(IndexName, out IndexDefinition staticIndex))
            {
                staticIndex.Priority = Priority;
                record.ClusterState.LastIndexesIndex = index;
            }

            if (record.AutoIndexes.TryGetValue(IndexName, out AutoIndexDefinition autoIndex))
            {
                autoIndex.Priority = Priority;
                record.ClusterState.LastAutoIndexesIndex = index;
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
            json[nameof(Priority)] = Priority;
        }
    }
}
