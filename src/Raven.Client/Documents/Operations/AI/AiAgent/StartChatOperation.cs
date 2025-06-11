using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.AiAgent;

internal class StartChatOperation<T> : IMaintenanceOperation<ChatResult<T>> where T : new()
{
    private readonly string _agent;
    private readonly string _prompt;
    private readonly Dictionary<string, string> _parameters;

    public StartChatOperation(string agent, string prompt, Dictionary<string, string> parameters = null)
    {
        _agent = agent ?? throw new ArgumentNullException(nameof(agent));
        _prompt = prompt ?? throw new ArgumentNullException(nameof(prompt));
        _parameters = parameters;
    }

    public RavenCommand<ChatResult<T>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new StartChatOperationCommand(_agent, _prompt, _parameters, conventions);
    }

    private sealed class StartChatOperationCommand : RavenCommand<ChatResult<T>>
    {
        private readonly string _agent;
        private readonly string _prompt;
        private readonly Dictionary<string, string> _parameters;
        private readonly DocumentConventions _conventions;

        public StartChatOperationCommand(string agent, string prompt, Dictionary<string, string> parameters, DocumentConventions conventions)
        {
            _agent = agent;
            _prompt = prompt;
            _parameters = parameters;
            _conventions = conventions;
        }
        public override bool IsReadRequest => false;
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/ai-agent/start?agent={_agent}&prompt={_prompt}";

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new BlittableJsonContent(async stream =>
                {
                    var json = new DynamicJsonValue { ["Parameters"] = DynamicJsonValue.Convert(_parameters) };
                    await ctx.WriteAsync(stream, ctx.ReadObject(json,"chat-params")).ConfigureAwait(false);
                }, _conventions)
            };

            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            response.TryGet(nameof(ChatResult<T>.Usage), out BlittableJsonReaderObject usage);
            response.TryGet(nameof(ChatResult<T>.Response), out BlittableJsonReaderObject result);
            response.TryGet(nameof(ChatResult<T>.ChatId), out string chatId);

            Result = new ChatResult<T>
            {
                ChatId = chatId,
                Usage = JsonDeserializationClient.AiUsage(usage),
                Response = _conventions.Serialization.DefaultConverter.FromBlittable<T>(result, chatId)
            };
        }
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
