using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Commands
{
    public class StartBackupCommand : RavenCommand
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
    }
}
