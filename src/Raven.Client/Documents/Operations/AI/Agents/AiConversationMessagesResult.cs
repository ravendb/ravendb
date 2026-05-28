using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// The result of fetching conversation messages.
/// </summary>
public class AiConversationMessagesResult : IDynamicJson
{
    /// <summary>
    /// The conversation document ID.
    /// </summary>
    public string ConversationId { get; set; }

    /// <summary>
    /// The identifier of the AI agent this conversation belongs to.
    /// </summary>
    public string Agent { get; set; }

    /// <summary>
    /// The conversation parameters as a name -> value map, normalized from the stored format.
    /// </summary>
    /// <remarks>
    /// Ignored by the auto-deserializer because values can be heterogeneous (primitives, arrays).
    /// Populated manually in <c>GetConversationMessagesCommand.SetResponse</c>.
    /// </remarks>
    [JsonDeserializationIgnore]
    public Dictionary<string, object> Parameters { get; set; }

    /// <summary>
    /// Cumulative token usage across all turns of this conversation.
    /// </summary>
    public AiUsage TotalUsage { get; set; }

    /// <summary>
    /// When the last message was added to the conversation.
    /// </summary>
    public DateTime LastMessageAt { get; set; }

    /// <summary>
    /// Messages in chronological order (oldest first).
    /// </summary>
    public List<AiConversationMessage> Messages { get; set; }

    /// <summary>
    /// True if there are more messages beyond the returned page.
    /// For backward/default paging: older messages exist.
    /// For forward (After) paging: newer messages exist.
    /// </summary>
    public bool HasMoreMessages { get; set; }

    /// <summary>
    /// IDs of sub-agent conversations spawned during this conversation.
    /// Each can be queried separately via GetConversationMessages.
    /// </summary>
    public List<string> SubConversationIds { get; set; }

    /// <summary>
    /// All attachment file names referenced across the returned messages, deduplicated.
    /// Aggregated from per-message <see cref="AiConversationMessage.Attachments"/>.
    /// </summary>
    public List<string> Attachments { get; set; }

    /// <summary>
    /// Serializes this result to a JSON structure.
    /// </summary>
    public DynamicJsonValue ToJson()
    {
        DynamicJsonValue parameters = null;
        if (Parameters != null)
        {
            parameters = new DynamicJsonValue();
            foreach (var kv in Parameters)
                parameters[kv.Key] = kv.Value;
        }

        return new DynamicJsonValue
        {
            [nameof(ConversationId)] = ConversationId,
            [nameof(Agent)] = Agent,
            [nameof(Parameters)] = parameters,
            [nameof(TotalUsage)] = TotalUsage?.ToJson(),
            [nameof(LastMessageAt)] = LastMessageAt,
            [nameof(HasMoreMessages)] = HasMoreMessages,
            [nameof(SubConversationIds)] = SubConversationIds != null ? new DynamicJsonArray(SubConversationIds) : null,
            [nameof(Attachments)] = Attachments != null ? new DynamicJsonArray(Attachments) : null,
            [nameof(Messages)] = Messages != null ? new DynamicJsonArray(Messages) : null
        };
    }
}
