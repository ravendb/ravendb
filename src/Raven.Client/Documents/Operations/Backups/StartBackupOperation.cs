using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Backups
{
    public class StartBackupOperation : IMaintenanceOperation<StartBackupOperationResult>
    {
        private readonly bool _isFullBackup;
        private readonly long _taskId;

        public StartBackupOperation(bool isFullBackup, long taskId)
        {
            _isFullBackup = isFullBackup;
            _taskId = taskId;
        }

        public RavenCommand<StartBackupOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new StartBackupCommand(_isFullBackup, _taskId);
        }

        private class StartBackupCommand : RavenCommand<StartBackupOperationResult>
        {
            public override bool IsReadRequest => true;

            private readonly bool _isFullBackup;
            private readonly long _taskId;

            public StartBackupCommand(bool isFullBackup, long taskId)
            {
                _isFullBackup = isFullBackup;
                _taskId = taskId;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/backup/database?isFullBackup={_isFullBackup}&taskId={_taskId}";
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.BackupDatabaseNowResult(response);
            }
        }
    }

    public class StartBackupOperationResult
    {
        public string ResponsibleNode { get; set; }

        public int OperationId { get; set; }
    }
}
