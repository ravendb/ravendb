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

            body.TryGet("SnapshotToken", out string snapshotToken);
            body.TryGet("NewConversationId", out string newConversationId);
            body.TryGet("ExpectedChangeVector", out string expectedChangeVector);

            if (string.IsNullOrEmpty(snapshotToken))
                throw new ArgumentException("SnapshotToken is required.");

            // Parse the token using the DTO
            var dto = SnapshotTokenDto.Parse(context, snapshotToken);

            // Resolve cluster identity before entering the transaction
            if (newConversationId?.EndsWith("|") == true)
            {
                var r = await RequestHandler.ServerStore.GenerateClusterIdentityAsync(
                    newConversationId,
                    RequestHandler.Database.IdentityPartsSeparator,
                    RequestHandler.Database.Name,
                    RequestHandler.GetRaftRequestIdFromQuery());
                newConversationId = r.ClusterId;
            }

            // Execute the fork
            var cmd = new ForkConversationCommand(RequestHandler.Database, dto.ConversationId, newConversationId, dto.Revisions, expectedChangeVector);
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
