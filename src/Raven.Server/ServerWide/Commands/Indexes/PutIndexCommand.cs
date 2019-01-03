using System;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
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
            try
            {
                record.AddIndex(Definition);
            }
            catch (Exception e)
            {
                throw new RachisApplyException("Failed to update index", e);
            }
            
            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Definition)] = TypeConverter.ToBlittableSupportedType(Definition);
        }
    }
}
