using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;

public class DeleteAiAgentOperation : IMaintenanceOperation<AiAgentConfigurationResult>
{
    private readonly string _id;
    public DeleteAiAgentOperation(string id)
    {
        ValidationMethods.AssertNotNullOrEmpty(id, nameof(id));
        _id = id;
    }

    public RavenCommand<AiAgentConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new DeleteAiAgentOperationCommand(_id);
    }

    private sealed class DeleteAiAgentOperationCommand : RavenCommand<AiAgentConfigurationResult>, IRaftCommand
    {
        private readonly string _id;

        public DeleteAiAgentOperationCommand(string id)
        {
            _id = id;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/ai/agent?id={Uri.EscapeDataString(_id)}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.AiAgentConfigurationResult(response);
        }

        public string RaftUniqueRequestId => RaftIdGenerator.NewId();
    }
}
