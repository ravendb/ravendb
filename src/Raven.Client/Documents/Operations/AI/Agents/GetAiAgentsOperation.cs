using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;

public sealed class GetAiAgentsOperation : IMaintenanceOperation<GetAiAgentsResponse>
{
    private readonly string _agentId;

    public GetAiAgentsOperation()
    {
    }

    public GetAiAgentsOperation(string agentId)
    {
        ValidationMethods.AssertNotNullOrEmpty(agentId, nameof(agentId));
        _agentId = agentId;
    }

    public RavenCommand<GetAiAgentsResponse> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetAiAgentOperationCommand(_agentId);
    }

    private sealed class GetAiAgentOperationCommand : RavenCommand<GetAiAgentsResponse>
    {
        private readonly string _agentId;

        public GetAiAgentOperationCommand(string agentId)
        {
            _agentId = agentId;
        }
        public override bool IsReadRequest => true;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/ai/agent";

            if (string.IsNullOrEmpty(_agentId) == false)
            {
                url += $"?agentId={Uri.EscapeDataString(_agentId)}";
            }

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.GetAiAgentsResponse(response);
        }
    }
}
