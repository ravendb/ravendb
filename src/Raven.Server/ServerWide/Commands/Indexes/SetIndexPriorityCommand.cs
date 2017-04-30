using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Server;
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

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.Indexes.TryGetValue(IndexName, out IndexDefinition staticIndex))
            {
                staticIndex.Priority = Priority;
                staticIndex.Etag = etag;
            }

            if (record.AutoIndexes.TryGetValue(IndexName, out AutoIndexDefinition autoIndex))
            {
                autoIndex.Priority = Priority;
                autoIndex.Etag = etag;
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
            json[nameof(Priority)] = Priority;
        }
    }
}