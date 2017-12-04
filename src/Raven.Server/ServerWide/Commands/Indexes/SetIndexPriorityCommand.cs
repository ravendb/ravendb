using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class SetIndexPriorityCommand : UpdateDatabaseCommand
    {
        public string IndexName;

        public IndexPriority Priority;

        public SetIndexPriorityCommand() : base(null)
        {
            // for deserialization
        }

        public SetIndexPriorityCommand(string name, IndexPriority priority, string databaseName)
            : base(databaseName)
        {
            IndexName = name;
            Priority = priority;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.Indexes.TryGetValue(IndexName, out IndexDefinition staticIndex))
            {
                staticIndex.Priority = Priority;
            }

            if (record.AutoIndexes.TryGetValue(IndexName, out AutoIndexDefinition autoIndex))
            {
                autoIndex.Priority = Priority;
            }

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
            json[nameof(Priority)] = Priority;
        }
    }
}
