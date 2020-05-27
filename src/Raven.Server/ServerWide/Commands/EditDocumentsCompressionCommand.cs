using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class EditDocumentsCompressionCommand : UpdateDatabaseCommand
    {
        public DocumentsCompressionConfiguration Configuration;
        
        public EditDocumentsCompressionCommand()
        {
        }
        
        public void UpdateDatabaseRecord(DatabaseRecord databaseRecord)
        {
            databaseRecord.DocumentsCompression = Configuration;
        }

        public override string UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DocumentsCompression = Configuration;
            return null;
        }
        
        public EditDocumentsCompressionCommand(DocumentsCompressionConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
