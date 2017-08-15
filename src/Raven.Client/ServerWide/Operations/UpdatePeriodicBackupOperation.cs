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
    public class UpdatePeriodicBackupOperation : IServerOperation<UpdatePeriodicBackupOperationResult>
    {
        private readonly PeriodicBackupConfiguration _configuration;
        private readonly string _databaseName;

        public UpdatePeriodicBackupOperation(PeriodicBackupConfiguration configuration, string databaseName)
        {
            _configuration = configuration;
            _databaseName = databaseName;
        }

        public RavenCommand<UpdatePeriodicBackupOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdatePeriodicBackupCommand(_configuration, _databaseName);
        }

        public class UpdatePeriodicBackupCommand : RavenCommand<UpdatePeriodicBackupOperationResult>
        {
            private readonly PeriodicBackupConfiguration _configuration;
            private readonly string _databaseName;

            public UpdatePeriodicBackupCommand(PeriodicBackupConfiguration configuration, string databaseName)
            {
                _configuration = configuration;
                _databaseName = databaseName;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/periodic-backup?name={_databaseName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var config = EntityToBlittable.ConvertEntityToBlittable(_configuration, DocumentConventions.Default, ctx);
                        ctx.Write(stream, config);
                    })
                };

                return request;
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ConfigurePeriodicBackupOperationResult(response);
            }
        }
    }
}
