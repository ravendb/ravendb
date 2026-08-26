using System;
using System.Net.Http;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Creates a snapshot of the current conversation state without running a conversation turn.
/// Returns a snapshot token that can later be passed to <see cref="ForkConversationOperation"/>.
/// Throws if the conversation does not exist.
/// </summary>
public class CreateConversationSnapshotOperation : IMaintenanceOperation<AiConversationSnapshot>
{
    private readonly string _conversationId;

    public CreateConversationSnapshotOperation(string conversationId)
    {
        ValidationMethods.AssertNotNullOrEmpty(conversationId, nameof(conversationId));
        _conversationId = conversationId;
    }

    public RavenCommand<AiConversationSnapshot> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new CreateConversationSnapshotCommand(_conversationId);
    }

    private sealed class CreateConversationSnapshotCommand : RavenCommand<AiConversationSnapshot>
    {
        private readonly string _conversationId;

        public CreateConversationSnapshotCommand(string conversationId)
        {
            _conversationId = conversationId;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent/snapshots?conversationId={Uri.EscapeDataString(_conversationId)}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                throw new InvalidOperationException($"Cannot create snapshot for conversation '{_conversationId}' because the conversation does not exist.");

            response.TryGet(nameof(AiConversationSnapshot.Token), out string token);
            response.TryGet(nameof(AiConversationSnapshot.CreatedAt), out DateTime createdAt);
            response.TryGet(nameof(AiConversationSnapshot.ChangeVector), out string changeVector);

            Result = new AiConversationSnapshot
            {
                Token = token,
                CreatedAt = createdAt,
                ChangeVector = changeVector
            };
        }
    }
}
