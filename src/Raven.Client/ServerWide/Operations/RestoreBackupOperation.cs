using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class RestoreBackupOperation : IServerOperation<OperationIdResult>
    {
        private readonly RestoreBackupConfiguration _restoreConfiguration;

        public RestoreBackupOperation(RestoreBackupConfiguration restoreConfiguration)
        {
            _restoreConfiguration = restoreConfiguration;
        }

        public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new RestoreBackupCommand(conventions, _restoreConfiguration);
        }

        private class RestoreBackupCommand : RavenCommand<OperationIdResult>
        {
            public override bool IsReadRequest => false;
            private readonly DocumentConventions _conventions;
            private readonly RestoreBackupConfiguration _restoreConfiguration;

            public RestoreBackupCommand(DocumentConventions conventions, RestoreBackupConfiguration restoreConfiguration)
            {
                _conventions = conventions;
                _restoreConfiguration = restoreConfiguration;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/restore/database";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertCommandToBlittable(_restoreConfiguration, ctx);
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
