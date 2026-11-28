using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.AI.Agents;

internal sealed class AiAgentProcessorForForkConversation : AbstractDatabaseHandlerProcessor<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AiAgentProcessorForForkConversation([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var body = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "fork-conversation");

            body.TryGet(nameof(ForkConversationRequest.SnapshotToken), out string snapshotToken);
            body.TryGet(nameof(ForkConversationRequest.NewConversationId), out string newConversationId);
            body.TryGet(nameof(ForkConversationRequest.ExpectedChangeVector), out string expectedChangeVector);

            if (string.IsNullOrEmpty(snapshotToken))
                throw new ArgumentException("SnapshotToken is required.");

            var token = SnapshotToken.Parse(snapshotToken);

            newConversationId = await ConversationHandler.ResolveConversationIdAsync(RequestHandler.ServerStore, RequestHandler.Database,
                newConversationId, RequestHandler.GetRaftRequestIdFromQuery());

            // Execute the fork
            var cmd = new ForkConversationCommand(RequestHandler.Database, token, newConversationId, expectedChangeVector);
            await RequestHandler.Database.TxMerger.Enqueue(cmd);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(AiForkConversationResult.ConversationId));
                writer.WriteString(cmd.ResultConversationId);

                writer.WriteComma();
                writer.WritePropertyName(nameof(AiForkConversationResult.ChangeVector));
                writer.WriteString(cmd.ResultChangeVector);

                writer.WriteEndObject();
            }
        }
    }
}
