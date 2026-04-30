using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow;
using Sparrow.Extensions;
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
    /// Simple (default): user messages (including attachment-only) and assistant messages with content.
    /// Detailed: adds system messages and tool calls with results.
    /// Full: no filtering, includes summaries and internal messages.
    /// </summary>
    public AiConversationDetailLevel DetailLevel { get; set; } = AiConversationDetailLevel.Simple;

    internal void Validate()
    {
        if (string.IsNullOrEmpty(ConversationId))
            throw new ArgumentNullException(nameof(ConversationId));

        if (Before.HasValue && After.HasValue)
            throw new ArgumentException($"{nameof(Before)} and {nameof(After)} cannot both be specified.");

        if (PageSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(PageSize), "PageSize must be greater than 0.");
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
                sb.Append("&before=").Append(Uri.EscapeDataString(_params.Before.Value.EnsureUtc().GetDefaultRavenFormat()));
            if (_params.After.HasValue)
                sb.Append("&after=").Append(Uri.EscapeDataString(_params.After.Value.EnsureUtc().GetDefaultRavenFormat()));

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
                return; // 404 — conversation not found

            Result = JsonDeserializationClient.AiConversationMessagesResult(response);

            // LazyStringValue / BlittableJsonReaderArray point into the context's byte buffer which
            // is returned to the pool after this method returns. Materialize to managed types now.
            if (Result.Parameters != null)
            {
                foreach (var param in Result.Parameters.Values)
                    param.Value = Materialize(param.Value);
            }
        }

        private static object Materialize(object value)
        {
            switch (value)
            {
                case LazyStringValue lsv:
                    return lsv.ToString(CultureInfo.InvariantCulture);
                case LazyCompressedStringValue lcsv:
                    return lcsv.ToString();
                case LazyNumberValue lnv:
                    var numStr = lnv.ToString(CultureInfo.InvariantCulture);
                    if (int.TryParse(numStr, NumberStyles.Any, CultureInfo.InvariantCulture, out int i))
                        return i;
                    if (long.TryParse(numStr, NumberStyles.Any, CultureInfo.InvariantCulture, out long l))
                        return l;
                    return double.Parse(numStr, NumberStyles.Any, CultureInfo.InvariantCulture);
                case BlittableJsonReaderArray arr:
                    var items = new List<object>(arr.Length);
                    foreach (var item in arr)
                        items.Add(Materialize(item));
                    return ToTypedList(items);
                default:
                    return value;
            }
        }

        private static object ToTypedList(List<object> items)
        {
            if (items.Count == 0)
                return items;

            bool allString = true, allBool = true, allNumber = true;
            bool hasLong = false, hasDouble = false;

            foreach (var item in items)
            {
                if (item is not string) allString = false;
                if (item is not bool)   allBool   = false;

                if (item is int)        { }
                else if (item is long)  hasLong   = true;
                else if (item is double) hasDouble = true;
                else                    allNumber = false;

                if (allString == false && allBool == false && allNumber == false)
                    break;
            }

            if (allString)
            {
                var typed = new List<string>(items.Count);
                foreach (var item in items) typed.Add((string)item);
                return typed;
            }

            if (allBool)
            {
                var typed = new List<bool>(items.Count);
                foreach (var item in items) typed.Add((bool)item);
                return typed;
            }

            if (allNumber)
            {
                if (hasDouble)
                {
                    var typed = new List<double>(items.Count);
                    foreach (var item in items) typed.Add(Convert.ToDouble(item));
                    return typed;
                }

                if (hasLong)
                {
                    var typed = new List<long>(items.Count);
                    foreach (var item in items) typed.Add(item is long l ? l : (long)(int)item);
                    return typed;
                }

                var ints = new List<int>(items.Count);
                foreach (var item in items) ints.Add((int)item);
                return ints;
            }

            return items;
        }
    }
}
