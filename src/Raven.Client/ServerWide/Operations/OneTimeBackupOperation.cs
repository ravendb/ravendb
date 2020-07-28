using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class OneTimeBackupOperation : IServerOperation<OperationIdResult>
    {
        private readonly BackupConfiguration _backupConfiguration;
        public string NodeTag;

        public OneTimeBackupOperation(BackupConfiguration backupConfiguration)
        {
            _backupConfiguration = backupConfiguration;
        }

        public OneTimeBackupOperation(BackupConfiguration backupConfiguration, string nodeTag)
        {
            _backupConfiguration = backupConfiguration;
            NodeTag = nodeTag;
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new OneTimeBackupCommand(_backupConfiguration, NodeTag);
        }

        private class OneTimeBackupCommand : RavenCommand<OperationIdResult>
        {
            public override bool IsReadRequest => false;
            private readonly BackupConfiguration _backupConfiguration;

            public OneTimeBackupCommand(BackupConfiguration backupConfiguration, string nodeTag = null)
            {
                _backupConfiguration = backupConfiguration;
                SelectedNodeTag = nodeTag;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/backup/database";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_backupConfiguration, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.OperationIdResult(response);
            }
        }
    }
}
