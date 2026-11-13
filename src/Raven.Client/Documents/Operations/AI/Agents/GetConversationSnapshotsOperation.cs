using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Returns available snapshots for a conversation, ordered by creation time (newest first).
/// Supports cursor-based paging via a <c>before</c> timestamp.
/// </summary>
public class GetConversationSnapshotsOperation : IMaintenanceOperation<List<AiConversationSnapshot>>
{
    private readonly string _conversationId;
    private readonly DateTime? _before;
    private readonly int _pageSize;

    public GetConversationSnapshotsOperation(string conversationId, DateTime? before = null, int pageSize = 25)
    {
        ValidationMethods.AssertNotNullOrEmpty(conversationId, nameof(conversationId));
        _conversationId = conversationId;
        _before = before;
        _pageSize = pageSize;
    }

    public RavenCommand<List<AiConversationSnapshot>> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetConversationSnapshotsCommand(_conversationId, _before, _pageSize);
    }

    private sealed class GetConversationSnapshotsCommand : RavenCommand<List<AiConversationSnapshot>>
    {
        private readonly string _conversationId;
        private readonly DateTime? _before;
        private readonly int _pageSize;

        public GetConversationSnapshotsCommand(string conversationId, DateTime? before, int pageSize)
        {
            _conversationId = conversationId;
            _before = before;
            _pageSize = pageSize;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent/snapshots?conversationId={Uri.EscapeDataString(_conversationId)}&pageSize={_pageSize}";

            if (_before.HasValue)
                url += $"&before={_before.Value.GetDefaultRavenFormat()}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = [];

            if (response == null)
                return;

            if (response.TryGet("Snapshots", out BlittableJsonReaderArray snapshots) == false || snapshots == null)
                return;

            foreach (BlittableJsonReaderObject item in snapshots)
            {
                item.TryGet(nameof(AiConversationSnapshot.Token), out string token);
                item.TryGet(nameof(AiConversationSnapshot.CreatedAt), out DateTime createdAt);

                Result.Add(new AiConversationSnapshot
                {
                    Token = token,
                    CreatedAt = createdAt
                });
            }
        }
    }
}
