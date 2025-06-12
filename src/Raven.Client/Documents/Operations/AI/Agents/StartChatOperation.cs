using System;
using System.Collections.Generic;
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

            response.TryGet(nameof(ChatResult<TSchema>.Usage), out BlittableJsonReaderObject usage);
            response.TryGet(nameof(ChatResult<TSchema>.Response), out BlittableJsonReaderObject result);
            response.TryGet(nameof(ChatResult<TSchema>.ChatId), out string chatId);

            Result = new ChatResult<TSchema>
            {
                ChatId = chatId,
                Usage = JsonDeserializationClient.AiUsage(usage),
                Response = _conventions.Serialization.DefaultConverter.FromBlittable<TSchema>(result, chatId)
            };
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

public class ChatResult<T>
{
    public string ChatId { get; set; }
    public T Response { get; set; }
    public AiUsage Usage { get; set; }
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
