using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class UpdatePullReplicationAsEdgeOperation : IMaintenanceOperation<ModifyOngoingTaskResult>
    {
        private readonly PullReplicationAsEdge _pullReplication;

        public UpdatePullReplicationAsEdgeOperation(PullReplicationAsEdge pullReplication)
        {
            _pullReplication = pullReplication;
        }

        public RavenCommand<ModifyOngoingTaskResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdatePullEdgeReplication(_pullReplication);
        }

        private class UpdatePullEdgeReplication : RavenCommand<ModifyOngoingTaskResult>
        {
            private readonly PullReplicationAsEdge _pullReplication;

            public UpdatePullEdgeReplication(PullReplicationAsEdge pullReplication)
            {
                _pullReplication = pullReplication;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/tasks/edge-pull-replication";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream =>
                    {
                        var json = new DynamicJsonValue
                        {
                            ["PullReplicationAsEdge"] = _pullReplication.ToJson()
                        };

                        ctx.Write(stream, ctx.ReadObject(json, "update-pull-replication"));
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
        }
    }
}
