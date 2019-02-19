using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.OngoingTasks
{
    public class GetPullReplicationTasksInfoOperation : IMaintenanceOperation<PullReplicationDefinitionAndCurrentConnections>
    {
        private readonly long _taskId;

        public GetPullReplicationTasksInfoOperation(long taskId)
        {
            _taskId = taskId;
        }

        public RavenCommand<PullReplicationDefinitionAndCurrentConnections> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new GetPullReplicationTasksInfoCommand(_taskId);
        }

        private class GetPullReplicationTasksInfoCommand : RavenCommand<PullReplicationDefinitionAndCurrentConnections>
        {
            private readonly long _taskId;

            public GetPullReplicationTasksInfoCommand(long taskId)
            {
                _taskId = taskId;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/tasks/pull-replication/hub?key={_taskId}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response != null)
                {
                    Result = JsonDeserializationClient.PullReplicationDefinitionAndCurrentConnectionsResult(response);
                }
            }

            public override bool IsReadRequest => false;
        }
    }
}
