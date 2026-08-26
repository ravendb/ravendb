using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.AI;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

/// <summary>
/// Processor for POST /ai/agent/snapshots — creates a snapshot of the current conversation state.
/// </summary>
internal sealed class AiAgentProcessorForCreateConversationSnapshot : AbstractDatabaseHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AiAgentProcessorForCreateConversationSnapshot([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var conversationId = RequestHandler.GetStringQueryString("conversationId");

        var (snapshotToken, createdAt, _) = await ConversationHandler.CreateSnapshotForConversationAsync(RequestHandler.Database, conversationId);

        if (snapshotToken == null)
        {
            RequestHandler.HttpContext.Response.StatusCode = (int)System.Net.HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(AiConversationSnapshot.Token));
            writer.WriteString(snapshotToken);

            writer.WriteComma();
            writer.WritePropertyName(nameof(AiConversationSnapshot.CreatedAt));
            writer.WriteDateTime(createdAt, isUtc: true);

            writer.WriteEndObject();
        }
    }
}
