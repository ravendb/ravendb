using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Server;
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

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Definition.Etag = etag;
            record.AddIndex(Definition);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = TypeConverter.ToBlittableSupportedType(Definition);
        }
    }
}