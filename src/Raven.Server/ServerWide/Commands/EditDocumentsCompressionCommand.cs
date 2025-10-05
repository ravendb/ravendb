using System;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class EditDocumentsCompressionCommand : UpdateDatabaseRecordFeaturesCommand
    {
        public DocumentsCompressionConfiguration Configuration;
        
        public EditDocumentsCompressionCommand()
        {
        }
        
        public void UpdateDatabaseRecord(DatabaseRecord databaseRecord)
        {
            databaseRecord.DocumentsCompression = Configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DocumentsCompression = Configuration;
        }
        
        public EditDocumentsCompressionCommand(DocumentsCompressionConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            if (configuration?.Collections.Length > 0)
            {
                configuration.Collections = configuration.Collections.ToHashSet(StringComparer.OrdinalIgnoreCase).ToArray();
            }

            Configuration = configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }

        public override bool Disabled => Configuration.CompressAllCollections == false && Configuration.CompressRevisions == false && Configuration.Collections?.Length == 0;
    }
}
