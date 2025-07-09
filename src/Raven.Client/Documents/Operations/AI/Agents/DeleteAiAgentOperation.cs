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
    private readonly string _identifier;
    public DeleteAiAgentOperation(string identifier)
    {
        ValidationMethods.AssertNotNullOrEmpty(identifier, nameof(identifier));
        _identifier = identifier;
    }

    public RavenCommand<AiAgentConfigurationResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new DeleteAiAgentOperationCommand(_identifier);
    }

    private sealed class DeleteAiAgentOperationCommand : RavenCommand<AiAgentConfigurationResult>, IRaftCommand
    {
        private readonly string _identifier;

        public DeleteAiAgentOperationCommand(string identifier)
        {
            _identifier = identifier;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/ai/agent?id={Uri.EscapeDataString(_identifier)}";

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
