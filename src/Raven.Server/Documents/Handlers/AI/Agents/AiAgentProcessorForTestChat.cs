using System;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Json.Serialization;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class AiAgentProcessorForTestChat : AbstractAiAgentProcessor
{
    public AiAgentProcessorForTestChat([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var _ = ContextPool.AllocateOperationContext(out JsonOperationContext context);
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var options = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token.Token);
        
        var body = JsonDeserializationServer.AiAgentTestRequest(options);
        body.Validate();

        ChatDocument chatDocument = null;
        if (options.TryGet("Document", out BlittableJsonReaderObject doc))
        {
            chatDocument = ChatDocument.ToDocument("test", doc);
        }

        if (chatDocument == null)
        {
            chatDocument = new ChatDocument("test", body.Parameters);
            chatDocument.Initialize(context, body.Configuration, body.UserPrompt);
        }

        // ensure we don't persist the chat in test mode
        body.Configuration.Persistence = null;

        await HandleRequest(context, body.Configuration, "test", chatDocument, body.RequestBody, token.Token);
    }

    public override async Task WriteResponseAsync(JsonOperationContext context, string conversationId, (BlittableJsonReaderObject Response, ChatDocument Document) r)
    {
        var output = new DynamicJsonValue
        {
            ["Document"] = r.Document.ToJson(),
            [nameof(ChatResult<object>.Response)] = r.Response,
            [nameof(ChatResult<object>.ToolRequests)] = new DynamicJsonArray(r.Document.OpenToolCalls.Select(t => t.Value.ToJson())),
            [nameof(ChatResult<object>.Usage)] = r.Document.TotalUsage.ToJson()
        };

        await using var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream());
        context.Write(writer, output);
    }

    public class AiAgentTestRequest
    {
        public string UserPrompt;
        public BlittableJsonReaderObject Parameters;
        public AiAgentConfiguration Configuration;
        public BlittableJsonReaderArray ActionResponses;

        public RequestBody RequestBody => new RequestBody
        {
            UserPrompt = UserPrompt,
            Parameters = Parameters,
            ActionResponses = ActionResponses
        };

        public void Validate()
        {
            if (string.IsNullOrEmpty(UserPrompt))
                throw new ArgumentException("Prompt is required for AI Agent test.");
            if (Configuration == null)
                throw new ArgumentException("Configuration is required for AI Agent test.");
            if (Parameters == null)
                throw new ArgumentException("Parameters are required for AI Agent test.");
        }
    }
}
