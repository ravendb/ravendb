using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;
public class RunConversationOperation<TSchema> : IMaintenanceOperation<ConversationResult<TSchema>>
{
    private readonly string _agentId;
    private readonly string _userPrompt;
    private readonly AiConversationCreationOptions _options;

    private readonly string _conversationId;
    private readonly List<AiAgentActionResponse> _actionResponses;
    private readonly string _changeVector;

    public RunConversationOperation(
        string agentId, 
        string conversationId, 
        string userPrompt, 
        List<AiAgentActionResponse> actionResponses,
        AiConversationCreationOptions options, 
        string changeVector)
    {
        ValidationMethods.AssertNotNullOrEmpty(agentId, nameof(agentId));
        ValidationMethods.AssertNotNullOrEmpty(conversationId, nameof(conversationId));

        _agentId = agentId;
        _conversationId = conversationId;
        _userPrompt = userPrompt;
        _changeVector = changeVector;
        _actionResponses = actionResponses;
        _options = options;
    }

    public RavenCommand<ConversationResult<TSchema>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new RunConversationOperationCommand(_conversationId, _agentId, _userPrompt, _actionResponses, _options, _changeVector, conventions);
    }

    internal sealed class RunConversationOperationCommand : RavenCommand<ConversationResult<TSchema>>, IRaftCommand
    {
        private readonly string _conversationId;
        private readonly string _agentId;
        private readonly string _prompt;
        private readonly List<AiAgentActionResponse> _actionResponses;
        private readonly string _changeVector;
        private readonly AiConversationCreationOptions _options;
        private readonly DocumentConventions _conventions;

        public RunConversationOperationCommand(string conversationId, string agentId, string prompt,
            List<AiAgentActionResponse> actionResponses, AiConversationCreationOptions options, string changeVector, DocumentConventions conventions)
        {
            _conversationId = conversationId;
            _agentId = agentId;
            _prompt = prompt;
            _actionResponses = actionResponses;
            _changeVector = changeVector;
            _options = options;
            _conventions = conventions;
        }
        public override bool IsReadRequest => false;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent" +
                  $"?conversationId={Uri.EscapeDataString(_conversationId)}&agentId={Uri.EscapeDataString(_agentId)}";

            if (_conversationId[_conversationId.Length - 1] == '|')
            {
                _raftId = Guid.NewGuid().ToString();
            }

            if (_changeVector != null)
                url += $"&changeVector={Uri.EscapeDataString(_changeVector)}";

            var body = new ConversionRequestBody
            {
                ActionResponses = _actionResponses,
                UserPrompt = _prompt,
                CreationOptions = _options
            };

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await ctx.WriteAsync(stream, ctx.ReadObject(body.ToJson(),"conversation-params")).ConfigureAwait(false);
                }, _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = ConversationResult<TSchema>.Convert(response, _conventions);
        }

        private string _raftId = string.Empty;
        public string RaftUniqueRequestId => _raftId;
    }
}
