using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Configuration
{
    public class GetServerWideExternalReplicationOperation : IServerOperation<ServerWideExternalReplication>
    {
        private readonly string _name;

        public GetServerWideExternalReplicationOperation(string name)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public RavenCommand<ServerWideExternalReplication> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetServerWideExternalReplicationCommand(_name);
        }

        private class GetServerWideExternalReplicationCommand : RavenCommand<ServerWideExternalReplication>
        {
            private readonly string _name;

            public GetServerWideExternalReplicationCommand(string name)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/configuration/server-wide/tasks?type={OngoingTaskType.Replication}&name={Uri.EscapeDataString(_name)}";

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

                var results = JsonDeserializationClient.GetServerWideExternalReplicationResponse(response).Results;
                if (results.Length == 0)
                    return;

                if (results.Length > 1)
                    ThrowInvalidResponse();

                Result = results[0];
            }
        }
    }
}
