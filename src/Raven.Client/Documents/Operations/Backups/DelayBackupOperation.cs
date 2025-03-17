using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Backups;

/// <summary>
/// Operation to delay the execution of a running backup task in the database.
/// </summary>
public sealed class DelayBackupOperation : IMaintenanceOperation<OperationState>
{
    private readonly long _runningBackupTaskId;
    private readonly TimeSpan _duration;

    /// <inheritdoc cref="DelayBackupOperation"/>
    /// <param name="runningBackupTaskId">The identifier of the running backup task to delay.</param>
    /// <param name="duration">The duration for which the backup task should be delayed.</param>
    public DelayBackupOperation(long runningBackupTaskId, TimeSpan duration)
    {
        _runningBackupTaskId = runningBackupTaskId;
        _duration = duration;
    }

    public RavenCommand<OperationState> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new DelayBackupCommand(_runningBackupTaskId, _duration);
    }

    private sealed class DelayBackupCommand : RavenCommand<OperationState>
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
