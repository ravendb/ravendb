using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.AI.Agents;

/// <summary>
/// Processor for DELETE /ai/agent/snapshots — purges revisions for a conversation.
/// </summary>
internal sealed class AiAgentProcessorForPurgeConversationSnapshots : AbstractDatabaseHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AiAgentProcessorForPurgeConversationSnapshots([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var conversationId = RequestHandler.GetStringQueryString("conversationId");
        var before = RequestHandler.GetDateTimeQueryString("before", required: false);

        var cmd = new PurgeConversationSnapshotsCommand(RequestHandler.Database, conversationId, before);
        await RequestHandler.Database.TxMerger.Enqueue(cmd);

        RequestHandler.NoContentStatus();
    }
}
