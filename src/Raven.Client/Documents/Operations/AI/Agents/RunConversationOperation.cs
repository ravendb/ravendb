using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

internal class ConversionRequestBody : IDynamicJson
{
    public List<AiAgentActionResponse> ActionResponses { get; set; }
    public string UserPrompt { get; set; }
    public AiConversationCreationOptions CreationOptions { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ActionResponses)] = ActionResponses == null ? null : new DynamicJsonArray(ActionResponses.Select(r => r.ToJson())),
            [nameof(UserPrompt)] = UserPrompt,
            [nameof(CreationOptions)] = (CreationOptions ?? new AiConversationCreationOptions()).ToJson()
        };
    }
}

public class ConversationResult<TSchema>
{
    public string ConversationId { get; set; }
    public string ChangeVector { get; set; }
    public TSchema Response { get; set; }
    public AiUsage TotalUsage { get; set; }
    public List<AiAgentActionRequest> ActionRequests { get; set; }

    internal static ConversationResult<TSchema> Convert(BlittableJsonReaderObject response, DocumentConventions conventions)
    {
        response.TryGet(nameof(TotalUsage), out BlittableJsonReaderObject usage);
        response.TryGet(nameof(Response), out BlittableJsonReaderObject result);
        response.TryGet(nameof(ConversationId), out string conversationId);
        response.TryGet(nameof(ChangeVector), out string changeVector);

        List<AiAgentActionRequest> requests = null;
        if (response.TryGet(nameof(ActionRequests), out BlittableJsonReaderArray actionRequests) && actionRequests != null)
        {
            requests = [];
            foreach (BlittableJsonReaderObject actionRequest in actionRequests)
            {
                var r = JsonDeserializationClient.ActionRequest(actionRequest);
                requests.Add(r);
            }
        }

        return new ConversationResult<TSchema>
        {
            ConversationId = conversationId,
            ChangeVector = changeVector,
            ActionRequests = requests,
            TotalUsage = JsonDeserializationClient.AiUsage(usage),
            Response = result == null ? default : conventions.Serialization.DefaultConverter.FromBlittable<TSchema>(result, conversationId)
        };
    }
}

public class AiAgentActionRequest : IDynamicJson
{
    public string Name;
    public string ToolId;
    public string Arguments;
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(ToolId)] = ToolId,
            [nameof(Arguments)] = Arguments
        };
    }
}

public class AiAgentActionResponse : IDynamicJson
{
    public string ToolId;
    public string Content;
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ToolId)] = ToolId,
            [nameof(Content)] = Content
        };
    }
}

public class AiUsage : IDynamicJsonValueConvertible
{
    public int PromptTokens { get; set; }
    public int CompletionTokens { get; set; }
    public int TotalTokens { get; set; }
    public int CachedTokens { get; set; }

    internal void UpdateFrom(BlittableJsonReaderObject json)
    {
        json.TryGet("prompt_tokens", out int promptTokens);
        json.TryGet("completion_tokens", out int completionTokens);
        json.TryGet("total_tokens", out int totalTokens);

        PromptTokens += promptTokens;
        CompletionTokens += completionTokens;
        TotalTokens += totalTokens;

        if (json.TryGet("prompt_tokens_details", out BlittableJsonReaderObject promptDetails))
        {
            if (promptDetails.TryGet("cached_tokens", out int cachedTokens))
                CachedTokens += cachedTokens;
        }
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(PromptTokens)] = PromptTokens,
            [nameof(CompletionTokens)] = CompletionTokens,
            [nameof(TotalTokens)] = TotalTokens,
            [nameof(CachedTokens)] = CachedTokens,
        };
    }

    internal  void Write(AsyncBlittableJsonTextWriter writer)
    {
        writer.WriteStartObject();

        writer.WritePropertyName(nameof(PromptTokens));
        writer.WriteInteger(PromptTokens);
        writer.WriteComma();
        
        writer.WritePropertyName(nameof(CompletionTokens));
        writer.WriteInteger(CompletionTokens);
        writer.WriteComma();
        
        writer.WritePropertyName(nameof(TotalTokens));
        writer.WriteInteger(TotalTokens);
        writer.WriteComma();
        
        writer.WritePropertyName(nameof(CachedTokens));
        writer.WriteInteger(CachedTokens);
        writer.WriteEndObject();
    }

}
