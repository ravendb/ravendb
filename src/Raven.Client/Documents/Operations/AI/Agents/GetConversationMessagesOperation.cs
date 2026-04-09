using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Parameters for reading messages from an AI agent conversation.
/// </summary>
public sealed class GetConversationMessagesOptions
{
    /// <summary>
    /// The conversation document ID.
    /// </summary>
    public string ConversationId { get; set; }

    /// <summary>
    /// Return messages older than this timestamp (exclusive upper bound).
    /// Used for backward paging (scrolling up in a chatbot UI).
    /// </summary>
    public DateTime? Before { get; set; }

    /// <summary>
    /// Return messages newer than this timestamp (exclusive lower bound).
    /// Used for catching up on new messages (e.g., after a Changes() notification).
    /// </summary>
    public DateTime? After { get; set; }

    /// <summary>
    /// Maximum number of messages to return. Default: 25.
    /// </summary>
    public int PageSize { get; set; } = 25;

    /// <summary>
    /// Controls the level of detail in returned messages.
    /// Simple: only user + assistant content messages.
    /// Detailed (default): all messages including system, tool calls, summaries, usage.
    /// </summary>
    public AiConversationDetailLevel DetailLevel{ get; set; } = AiConversationDetailLevel.Simple;

    internal void Validate()
    {
        if (string.IsNullOrEmpty(ConversationId))
            throw new ArgumentNullException(nameof(ConversationId));

        if (Before.HasValue && After.HasValue)
            throw new ArgumentException($"{nameof(Before)} and {nameof(After)} cannot both be specified.");
    }
}

/// <summary>
/// Reads messages from an AI agent conversation, with optional timestamp-based paging and view filtering.
/// </summary>
public sealed class GetConversationMessagesOperation : IMaintenanceOperation<AiConversationMessagesResult>
{
    private readonly GetConversationMessagesOptions _parameters;

    public GetConversationMessagesOperation(string conversationId)
        : this(new GetConversationMessagesOptions { ConversationId = conversationId })
    {
    }

    public GetConversationMessagesOperation(GetConversationMessagesOptions parameters)
    {
        _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        _parameters.Validate();
    }

    public RavenCommand<AiConversationMessagesResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetConversationMessagesCommand(_parameters);
    }

    private sealed class GetConversationMessagesCommand : RavenCommand<AiConversationMessagesResult>
    {
        private readonly GetConversationMessagesOptions _params;

        public GetConversationMessagesCommand(GetConversationMessagesOptions parameters)
        {
            _params = parameters;
        }

        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/ai/conversation/messages")
                .Append($"?conversationId={Uri.EscapeDataString(_params.ConversationId)}");

            if (_params.Before.HasValue)
                sb.Append($"&before={_params.Before.Value:o}");
            if (_params.After.HasValue)
                sb.Append($"&after={_params.After.Value:o}");

            sb.Append($"&pageSize={_params.PageSize}");
            sb.Append($"&detailLevel={_params.DetailLevel}");

            url = sb.ToString();

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get,
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = JsonDeserializationClient.AiConversationMessagesResult(response);
        }
    }
}
