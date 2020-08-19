using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class SetIndexStateCommand : UpdateDatabaseCommand
    {
        public string IndexName;

        public IndexState State;

        public SetIndexStateCommand()
        {
            // for deserialization
        }

        public SetIndexStateCommand(string name, IndexState state, string databaseName, string uniqueRequestId)
            : base(databaseName, uniqueRequestId)
        {
            if (string.IsNullOrEmpty(name))
                throw new RachisApplyException($"Index name cannot be null or empty");

            State = state;
            IndexName = name;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (record.AutoIndexes.TryGetValue(IndexName, out AutoIndexDefinition autoIndex))
            {
                autoIndex.State = State;
            }

        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(IndexName)] = IndexName;
            json[nameof(State)] = State;
        }
    }
}
