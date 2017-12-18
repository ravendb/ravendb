using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.PeriodicBackup;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class GetPeriodicBackupStatusOperation : IMaintenanceOperation<GetPeriodicBackupStatusOperationResult>
    {
        private readonly long _taskId;
        
        public GetPeriodicBackupStatusOperation(long taskId)
        {
            _taskId = taskId;
        }

        public RavenCommand<GetPeriodicBackupStatusOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetPeriodicBackupStatusCommand(_taskId);
        }
    }

    public class GetPeriodicBackupStatusCommand : RavenCommand<GetPeriodicBackupStatusOperationResult>
    {
        public override bool IsReadRequest => true;
        private readonly long _taskId;

        public GetPeriodicBackupStatusCommand(long taskId)
        {
            _taskId = taskId;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/periodic-backup/status?taskId={_taskId}";
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
        public PeriodicBackupStatus Status;
    }
}
