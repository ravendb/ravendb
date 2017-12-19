using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.PeriodicBackup;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class RestoreBackupOperation : IServerOperation<RestoreBackupOperationResult>
    {
        private readonly RestoreBackupConfiguration _restoreConfiguration;

        public RestoreBackupOperation(RestoreBackupConfiguration restoreConfiguration)
        {
            _restoreConfiguration = restoreConfiguration;
        }

        public RavenCommand<RestoreBackupOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new RestoreBackupCommand(_restoreConfiguration);
        }
    }

    public class RestoreBackupCommand : RavenCommand<RestoreBackupOperationResult>
    {
        public override bool IsReadRequest => false;
        private readonly RestoreBackupConfiguration _restoreConfiguration;

        public RestoreBackupCommand(RestoreBackupConfiguration restoreConfiguration)
        {
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
                    var config = EntityToBlittable.ConvertEntityToBlittable(_restoreConfiguration, DocumentConventions.Default, ctx);
                    ctx.Write(stream, config);
                })
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if(response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.RestoreResultOperationResult(response);
        }
    }

    public class RestoreBackupOperationResult
    {
        public long OperationId { get; set; }
    }
}
