using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Backups;

public sealed class DelayBackupOperation : IMaintenanceOperation<OperationState>
{
    private readonly long _runningBackupTaskId;
    private readonly TimeSpan _duration;

    public DelayBackupOperation(long runningBackupTaskId, TimeSpan duration)
    {
        _runningBackupTaskId = runningBackupTaskId;
        _duration = duration;
    }

    public RavenCommand<OperationState> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new DelayBackupCommand(_runningBackupTaskId, _duration);
    }

    private class DelayBackupCommand : RavenCommand<OperationState>
    {
        private readonly long _taskId;
        private readonly TimeSpan? _duration;

        public DelayBackupCommand(long taskId, TimeSpan duration)
        {
            _taskId = taskId;
            _duration = duration;
        }

        public override bool IsReadRequest => true;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/backup-task/delay?taskId={_taskId}&duration={_duration}&database={node.Database}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
        }
    }
}
