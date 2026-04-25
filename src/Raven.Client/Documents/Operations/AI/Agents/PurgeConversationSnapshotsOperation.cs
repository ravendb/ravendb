using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Deletes snapshots for the specified conversation, invalidating corresponding snapshot tokens.
/// The conversation itself is not affected.
/// </summary>
public class PurgeConversationSnapshotsOperation : IMaintenanceOperation
{
    private readonly string _conversationId;
    private readonly DateTime? _before;

    public PurgeConversationSnapshotsOperation(string conversationId)
    {
        ValidationMethods.AssertNotNullOrEmpty(conversationId, nameof(conversationId));
        _conversationId = conversationId;
    }

    public PurgeConversationSnapshotsOperation(string conversationId, DateTime before)
    {
        ValidationMethods.AssertNotNullOrEmpty(conversationId, nameof(conversationId));
        _conversationId = conversationId;
        _before = before;
    }

    public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new PurgeConversationSnapshotsCommand(_conversationId, _before);
    }

    private sealed class PurgeConversationSnapshotsCommand : RavenCommand
    {
        private readonly string _conversationId;
        private readonly DateTime? _before;

        public PurgeConversationSnapshotsCommand(string conversationId, DateTime? before)
        {
            _conversationId = conversationId;
            _before = before;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/ai/agent/snapshots?conversationId={Uri.EscapeDataString(_conversationId)}";

            if (_before.HasValue)
                url += $"&before={_before.Value.GetDefaultRavenFormat()}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete
            };
        }
    }
}
