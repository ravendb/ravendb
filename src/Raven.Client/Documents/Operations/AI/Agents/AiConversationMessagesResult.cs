using System;
using System.Collections.Generic;
using Raven.Client.Documents.AI;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// The result of fetching conversation messages.
/// </summary>
public class AiConversationMessagesResult
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
    /// The conversation parameters, normalized from the stored format.
    /// </summary>
    public Dictionary<string, AiConversationParameter> Parameters { get; set; }

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
    /// True if any message in the conversation contains attachments (images, files).
    /// </summary>
    public bool HasAttachments { get; set; }

    /// <summary>
    /// IDs of sub-agent conversations spawned during this conversation.
    /// Each can be queried separately via GetConversationMessages.
    /// </summary>
    public List<string> SubConversationIds { get; set; }
}
