using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Json.Serialization;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class AiAgentProcessorForTestChat : AbstractSingleAiAgentTalkProcessor
{
    public AiAgentProcessorForTestChat([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var _ = ContextPool.AllocateOperationContext(out DocumentsOperationContext context);
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var options = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token.Token);
        var cfg = JsonDeserializationClient.AiAgentConfiguration(options);

        var body = GetStartChatOptions(options);
        var chat = new ChatDocument("test", body.Parameter);
        chat.Initialize(context, cfg.SystemPrompt, body.UserPrompt);

        var r = await TalkAsync(context, cfg, chat, token);

        await WriteResponseAsync(context, "test", r);
    }
}
