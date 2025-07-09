using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;
public class RunChatOperation<TSchema> : IMaintenanceOperation<ChatResult<TSchema>> where TSchema : new()
{
    private readonly string _identifier;
    private readonly string _userPrompt;
    private readonly Dictionary<string, object> _parameters;

    private readonly string _chatId;
    private readonly List<ToolResponse> _toolResponses;
    public RunChatOperation(string identifier, string userPrompt, Dictionary<string, object> parameters)
    {
        ValidationMethods.AssertNotNullOrEmpty(identifier, nameof(identifier));
        ValidationMethods.AssertNotNullOrEmpty(userPrompt, nameof(userPrompt));

        _identifier = identifier;
        _userPrompt = userPrompt;
        _parameters = parameters;
    }

    public RunChatOperation(string chatId, string userPrompt = null, List<ToolResponse> toolResponses = null)
    {
        ValidationMethods.AssertNotNullOrEmpty(chatId, nameof(chatId));
     
        _chatId = chatId;
        _userPrompt = userPrompt;
        _toolResponses = toolResponses;
    }

    public RavenCommand<ChatResult<TSchema>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new RunChatOperationCommand(_chatId, _identifier, _userPrompt, _parameters, _toolResponses, conventions);
    }

    internal sealed class RunChatOperationCommand : RavenCommand<ChatResult<TSchema>>
    {
        private readonly string _chatId;
        private readonly string _identifier;
        private readonly string _prompt;
        private readonly Dictionary<string, object> _parameters;
        private readonly List<ToolResponse> _toolResponses;
        private readonly DocumentConventions _conventions;

        public RunChatOperationCommand(string chatId, string identifier, string prompt, Dictionary<string, object> parameters, List<ToolResponse> toolResponses, DocumentConventions conventions)
        {
            _chatId = chatId;
            _identifier = identifier;
            _prompt = prompt;
            _parameters = parameters;
            _toolResponses = toolResponses;
            _conventions = conventions;
        }
        public override bool IsReadRequest => false;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent";
            if (string.IsNullOrEmpty(_identifier) == false)
            {
                url += $"?id={Uri.EscapeDataString(_identifier)}";

            }
            if (string.IsNullOrEmpty(_chatId) == false)
            {
                url += $"?chatId={Uri.EscapeDataString(_chatId)}";
            }

            var body = new ChatRequestBody
            {
                Parameters = _parameters ?? new Dictionary<string, object>(),
                ToolResponses = _toolResponses,
                UserPrompt = _prompt
            };

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    await ctx.WriteAsync(stream, ctx.ReadObject(body.ToJson(),"chat-params")).ConfigureAwait(false);
                }, _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = ChatResult<TSchema>.Convert(response, _conventions);
        }
    }
}

internal class ChatRequestBody : IDynamicJson
{
    public Dictionary<string, object> Parameters { get; set; }
    public List<ToolResponse> ToolResponses { get; set; }
    public string UserPrompt { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Parameters)] = DynamicJsonValue.Convert(Parameters),
            [nameof(ToolResponses)] = ToolResponses == null ? null : new DynamicJsonArray(ToolResponses.Select(r => r.ToJson())), 
            [nameof(UserPrompt)] = UserPrompt,
        };
    }
}

public class ChatResult<TSchema>
{
    public string ChatId { get; set; }
    public TSchema Response { get; set; }
    public AiUsage Usage { get; set; }
    public List<ToolRequest> ToolRequests { get; set; }

    internal static ChatResult<TSchema> Convert(BlittableJsonReaderObject response, DocumentConventions conventions)
    {
        response.TryGet(nameof(Usage), out BlittableJsonReaderObject usage);
        response.TryGet(nameof(Response), out BlittableJsonReaderObject result);
        response.TryGet(nameof(ChatId), out string chatId);

        List<ToolRequest> requests = null;
        if (response.TryGet(nameof(ToolRequests), out BlittableJsonReaderArray toolRequests) && toolRequests != null)
        {
            requests = [];
            foreach (BlittableJsonReaderObject toolRequest in toolRequests)
            {
                var r = JsonDeserializationClient.ToolRequest(toolRequest);
                requests.Add(r);
            }
        }

        return new ChatResult<TSchema>
        {
            ChatId = chatId,
            ToolRequests = requests,
            Usage = JsonDeserializationClient.AiUsage(usage),
            Response = result == null ? default : conventions.Serialization.DefaultConverter.FromBlittable<TSchema>(result, chatId)
        };
    }
}

public class ToolRequest : IDynamicJson
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

public class ToolResponse : IDynamicJson
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
            if(promptDetails.TryGet("cached_tokens", out int cachedTokens))
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
