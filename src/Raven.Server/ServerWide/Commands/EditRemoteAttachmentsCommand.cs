using Raven.Client.Documents.Attachments;
using Raven.Client.ServerWide;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public sealed class EditRemoteAttachmentsCommand : UpdateDatabaseCommand
    {
        public const int CommandVersion = 72_000;
        public RemoteAttachmentsConfiguration Configuration;

        public EditRemoteAttachmentsCommand()
        {
        }

        public EditRemoteAttachmentsCommand(RemoteAttachmentsConfiguration configuration, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Configuration = configuration;
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.RemoteAttachments = Configuration;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Configuration)] = Configuration.ToJson();
        }
    }
}
