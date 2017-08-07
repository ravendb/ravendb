using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide.PeriodicBackup;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class GetPeriodicBackupStatusOperation : IServerOperation<GetPeriodicBackupStatusOperationResult>
    {
        private readonly string _databaseName;

        private readonly long _taskId;
        
        public GetPeriodicBackupStatusOperation(string databaseName, long taskId)
        {
            _databaseName = databaseName;
            _taskId = taskId;
        }

        public RavenCommand<GetPeriodicBackupStatusOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetPeriodicBackupStatusCommand(_databaseName, _taskId);
        }
    }

    public class GetPeriodicBackupStatusCommand : RavenCommand<GetPeriodicBackupStatusOperationResult>
    {
        public override bool IsReadRequest => true;
        private readonly string _databaseName;
        private readonly long _taskId;

        public GetPeriodicBackupStatusCommand(string databaseName, long taskId)
        {
            _databaseName = databaseName;
            _taskId = taskId;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}/periodic-backup/status?name={_databaseName}&taskId={_taskId}";
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
