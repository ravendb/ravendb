namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// Controls the level of detail when reading conversation messages.
/// </summary>
public enum AiConversationDetailLevel
{
    /// <summary>
    /// User messages and assistant messages that have content only.
    /// System prompts, tool calls, summaries, and internal messages are excluded.
    /// </summary>
    Simple,

    /// <summary>
    /// Includes system messages, tool calls with results, and per-message usage.
    /// Summaries and internal messages are excluded.
    /// </summary>
    Detailed,

    /// <summary>
    /// No filtering. Includes all messages: system, tool calls, summaries, internal.
    /// Intended for debugging and future-proofing.
    /// </summary>
    Full
}
