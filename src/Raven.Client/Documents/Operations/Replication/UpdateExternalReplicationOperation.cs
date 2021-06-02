using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class UpdateExternalReplicationOperation : IMaintenanceOperation<ModifyOngoingTaskResult>
    {
        private readonly ExternalReplication _newWatcher;

        public UpdateExternalReplicationOperation(ExternalReplication newWatcher)
        {
            _newWatcher = newWatcher;
        }

        public RavenCommand<ModifyOngoingTaskResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdateExternalReplication(_newWatcher);
        }

        private class UpdateExternalReplication : RavenCommand<ModifyOngoingTaskResult>, IRaftCommand
        {
            private readonly ExternalReplication _newWatcher;

            public UpdateExternalReplication(ExternalReplication newWatcher)
            {
                _newWatcher = newWatcher ?? throw new ArgumentNullException(nameof(newWatcher));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/external-replication";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream =>
                    {
                        var json = new DynamicJsonValue
                        {
                            ["Watcher"] = _newWatcher.ToJson()
                        };

                        await ctx.WriteAsync(stream, ctx.ReadObject(json, "update-replication")).ConfigureAwait(false);
                    })
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.ModifyOngoingTaskResult(response);
            }

            public override bool IsReadRequest => false;
            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
