using Raven.Client.Documents.Attachments;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class EditRetireAttachmentsCommand : UpdateDatabaseCommand
    {
        public const int CommandVersion = 60_002;
        public RetireAttachmentsConfiguration Configuration;

        public EditRetireAttachmentsCommand()
        {
        }

        public EditRetireAttachmentsCommand(RetireAttachmentsConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.RetireAttachments = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
        }
    }
}
