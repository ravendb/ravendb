using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Indexes
{
    public class PutIndexesCommand : UpdateDatabaseCommand
    {
        public List<IndexDefinition> Static = new List<IndexDefinition>();

        public List<AutoIndexDefinition> Auto = new List<AutoIndexDefinition>();

        public PutIndexesCommand() : base(null)
        {
            // for deserialization
        }

        public PutIndexesCommand(string databaseName)
            : base(databaseName)
        {
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (Static != null)
            {
                foreach (var definition in Static)
                    record.AddIndex(definition);
            }

            if (Auto != null)
            {
                foreach (var definition in Auto)
                    record.AddIndex(definition);
            }

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Static)] = TypeConverter.ToBlittableSupportedType(Static);
            json[nameof(Auto)] = TypeConverter.ToBlittableSupportedType(Auto);
        }
    }
}
