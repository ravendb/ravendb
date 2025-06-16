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
public class StartChatOperation<TSchema> : IMaintenanceOperation<ChatResult<TSchema>> where TSchema : new()
{
    private readonly string _name;
    private readonly string _prompt;
    private readonly Dictionary<string, object> _parameters;

    public StartChatOperation(string agentName, string prompt, Dictionary<string, object> parameters = null)
    {
        ValidationMethods.AssertNotNullOrEmpty(agentName, nameof(agentName));
        ValidationMethods.AssertNotNullOrEmpty(prompt, nameof(prompt));

        _name = agentName;
        _prompt = prompt;
        _parameters = parameters;
    }

    public RavenCommand<ChatResult<TSchema>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new StartChatOperationCommand(_name, _prompt, _parameters, conventions);
    }

    internal sealed class StartChatOperationCommand : RavenCommand<ChatResult<TSchema>>
    {
        private readonly string _name;
        private readonly string _prompt;
        private readonly Dictionary<string, object> _parameters;
        private readonly DocumentConventions _conventions;

        public StartChatOperationCommand(string agentName, string prompt, Dictionary<string, object> parameters, DocumentConventions conventions)
        {
            _name = agentName;
            _prompt = prompt;
            _parameters = parameters;
            _conventions = conventions;
        }
        public override bool IsReadRequest => false;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent/start?name={Uri.EscapeDataString(_name)}";
            var body = new StartChatBody { Prompt = _prompt, Parameters = _parameters };


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

internal class StartChatBody : IDynamicJson
{
    public Dictionary<string, object> Parameters { get; set; }
    public string Prompt { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Parameters)] = DynamicJsonValue.Convert(Parameters),
            [nameof(Prompt)] = Prompt
        };
    }
}

internal class ResumeChatBody : IDynamicJson
{
    public List<ToolResponse> ToolResponse { get; set; }
    public string UserPrompt { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(ToolResponse)] = ToolResponse == null ? null : new DynamicJsonArray(ToolResponse.Select(r => r.ToJson())), 
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
