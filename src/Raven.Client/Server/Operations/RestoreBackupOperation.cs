using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.PeriodicBackup;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class RestoreBackupOperation : IServerOperation<RestoreBackupOperationResult>
    {
        private readonly RestoreBackupConfiguration _restoreConfiguration;

        public RestoreBackupOperation(RestoreBackupConfiguration restoreConfiguration)
        {
            _restoreConfiguration = restoreConfiguration;
        }

        public RavenCommand<RestoreBackupOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new RestoreBackupCommand(_restoreConfiguration, context);
        }
    }

    public class RestoreBackupCommand : RavenCommand<RestoreBackupOperationResult>
    {
        public override bool IsReadRequest => false;
        private readonly RestoreBackupConfiguration _restoreConfiguration;
        private readonly JsonOperationContext _context;

        public RestoreBackupCommand(RestoreBackupConfiguration restoreConfiguration, JsonOperationContext context)
        {
            _restoreConfiguration = restoreConfiguration;
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/database-restore";
            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(stream =>
                {
                    var config = EntityToBlittable.ConvertEntityToBlittable(_restoreConfiguration, DocumentConventions.Default, _context);
                    _context.Write(stream, config);
                })
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
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
