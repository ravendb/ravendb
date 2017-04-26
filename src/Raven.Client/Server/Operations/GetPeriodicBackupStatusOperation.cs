using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Server.PeriodicExport;
using Sparrow.Json;

namespace Raven.Client.Server.Operations
{
    public class GetPeriodicBackupStatusOperation : IServerOperation<GetPeriodicBackupStatusOperationResult>
    {

        public RavenCommand<GetPeriodicBackupStatusOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetPeriodicBackupStatusCommand();
        }
    }

    public class GetPeriodicBackupStatusCommand : RavenCommand<GetPeriodicBackupStatusOperationResult>
    {
        public override bool IsReadRequest => true;
        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/periodic-backup/status";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };

            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {
            if(response == null)
                ThrowInvalidResponse();
            Result = JsonDeserializationClient.GetPeriodicBackupStatusOperationResult(response);
        }
    }

    public class GetPeriodicBackupStatusOperationResult
    {
        public PeriodicExportStatus Status;
    }
}
