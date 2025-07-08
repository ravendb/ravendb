using System.Threading.Tasks;
using Sparrow.Json;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class AiAgentProcessorForStartChat : AbstractAiAgentProcessor
{
    public AiAgentProcessorForStartChat([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var identifier = RequestHandler.GetStringQueryString("identifier", required: true);

        var configuration = GetAiAgentConfiguration(identifier);

        using var _ = ContextPool.AllocateOperationContext(out JsonOperationContext context);
        var options = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token.Token);
        var body = GetStartChatOptions(options);
        var chat = new ChatDocument(identifier, body.Parameter);

        chat.Initialize(context, configuration, body.UserPrompt);
        var r = await TalkAsync(context, configuration, chat, token);

        var conversationId = await TryPersistAsync(configuration, $"{configuration.Persistence.Collection}{RequestHandler.Database.IdentityPartsSeparator}", r.Document);

        await WriteResponseAsync(context, conversationId, r);
    }
}
