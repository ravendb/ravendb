using System.Threading.Tasks;
using Sparrow.Json;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal class AiAgentProcessorForStartChat : AbstractSingleAiAgentProcessor
{
    public AiAgentProcessorForStartChat([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var name = RequestHandler.GetStringQueryString("name", required: true);

        var configuration = GetAiAgentConfiguration(name);

        using var _ = ContextPool.AllocateOperationContext(out JsonOperationContext context);
        var options = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "ai-agent", token.Token);
        var body = GetStartChatOptions(options);
        var chat = new ChatDocument(name, body.Parameter);

        chat.Initialize(context, configuration.SystemPrompt, body.UserPrompt);
        var r = await TalkAsync(context, configuration, chat, token);

        var conversationId = await TryPersistAsync(configuration, $"{configuration.Persistence.Collection}{RequestHandler.Database.IdentityPartsSeparator}", r.Document);

        await WriteResponseAsync(context, conversationId, r);
    }
}
