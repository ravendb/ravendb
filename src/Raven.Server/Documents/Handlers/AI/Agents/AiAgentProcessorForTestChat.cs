using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Json.Serialization;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class AiAgentProcessorForTestChat : AbstractAiAgentProcessor
{
    public AiAgentProcessorForTestChat([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var options = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token.Token);
        var test = JsonDeserializationServer.AiAgentTestRequest(options);

        var body = GetStartChatOptions(options);
        var chat = new ChatDocument("test", body.Parameter);
        chat.Initialize(context, test.Configuration, body.UserPrompt);

        var r = await TalkAsync(context, test.Configuration, chat, token);

        await WriteResponseAsync(context, "test", r);
    }

    public class AiAgentTestRequest
    {
        public string Prompt;
        public BlittableJsonReaderObject Parameters;
        public AiAgentConfiguration Configuration;

        public void Validate()
        {
            if (string.IsNullOrEmpty(Prompt))
                throw new ArgumentException("Prompt is required for AI Agent test.");
            if (Configuration == null)
                throw new ArgumentException("Configuration is required for AI Agent test.");
            if (Parameters == null)
                throw new ArgumentException("Parameters are required for AI Agent test.");
        }
    }
}
