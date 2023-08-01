using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Backups.Sharding;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Backups
{
    public sealed class GetPeriodicBackupStatusOperation : IMaintenanceOperation<GetPeriodicBackupStatusOperationResult>
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

        private sealed class GetPeriodicBackupStatusCommand : RavenCommand<GetPeriodicBackupStatusOperationResult>
        {
            public override bool IsReadRequest => true;
            private readonly long _taskId;

            public GetPeriodicBackupStatusCommand(long taskId)
            {
                _taskId = taskId;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/periodic-backup/status?name={node.Database}&taskId={_taskId}";
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetPeriodicBackupStatusOperationResult(response);
                if (Result.IsSharded)
                    throw new InvalidOperationException($"Database is sharded, can't use {nameof(GetPeriodicBackupStatusOperation)}, " +
                                                        $"use {nameof(GetShardedPeriodicBackupStatusOperation)} instead");
            }
        }
    }

    public abstract class AbstractGetPeriodicBackupStatusOperationResult
    {
        [ForceJsonSerialization]
        internal bool IsSharded;
    }

    public sealed class GetPeriodicBackupStatusOperationResult : AbstractGetPeriodicBackupStatusOperationResult
    {
        public PeriodicBackupStatus Status;
    }
}
