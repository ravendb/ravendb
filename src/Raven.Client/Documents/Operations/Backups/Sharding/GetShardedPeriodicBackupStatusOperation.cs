using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Backups.Sharding
{
    public sealed class GetShardedPeriodicBackupStatusOperation : IMaintenanceOperation<GetShardedPeriodicBackupStatusOperationResult>
    {
        private readonly long _taskId;

        public GetShardedPeriodicBackupStatusOperation(long taskId)
        {
            _taskId = taskId;
        }

        public RavenCommand<GetShardedPeriodicBackupStatusOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetShardedPeriodicBackupStatusCommand(_taskId);
        }

        private class GetShardedPeriodicBackupStatusCommand : RavenCommand<GetShardedPeriodicBackupStatusOperationResult>
        {
            public override bool IsReadRequest => true;
            private readonly long _taskId;

            public GetShardedPeriodicBackupStatusCommand(long taskId)
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

                Result = JsonDeserializationClient.GetShardedPeriodicBackupStatusOperationResult(response);

                if (Result.IsSharded == false)
                    throw new InvalidOperationException($"Database is not sharded, can't use {nameof(GetShardedPeriodicBackupStatusOperation)}, " +
                                                        $"use {nameof(GetPeriodicBackupStatusOperation)} instead");
            }
        }
    }
    public sealed class GetShardedPeriodicBackupStatusOperationResult : AbstractGetPeriodicBackupStatusOperationResult
    {
        public Dictionary<int, PeriodicBackupStatus> Statuses;
    }
}
