using System;
using System.Collections.Generic;

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
    /// True if there are messages older than the ones returned.
    /// </summary>
    public bool HasOlderMessages { get; set; }
}
