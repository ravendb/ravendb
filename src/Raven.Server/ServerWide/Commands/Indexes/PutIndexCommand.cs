using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class PutIndexCommand : UpdateDatabaseCommand
    {
        public IndexDefinition Definition;

        public PutIndexCommand() : base(null)
        {
            // for deserialization
        }

        public PutIndexCommand(IndexDefinition definition, string databaseName)
            : base(databaseName)
        {
            Definition = definition;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.AddIndex(Definition);
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = TypeConverter.ToBlittableSupportedType(Definition);
        }
    }
}
