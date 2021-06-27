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

        public override void UpdateDatabaseRecord(DatabaseRecord record, long index)
        {
            record.DocumentsCompression = Configuration;
            record.ClusterState.LastDocumentsCompressionIndex = index;
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
