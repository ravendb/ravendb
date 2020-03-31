using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class GetServerWideExternalReplicationsOperation : IServerOperation<ServerWideExternalReplication[]>
    {
        public RavenCommand<ServerWideExternalReplication[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetServerWideExternalReplicationsCommand();
        }

        private class GetServerWideExternalReplicationsCommand : RavenCommand<ServerWideExternalReplication[]>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/tasks?type={OngoingTaskType.Replication}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

            }
        }
    }
}
