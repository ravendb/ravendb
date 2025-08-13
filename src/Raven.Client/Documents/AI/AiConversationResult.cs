namespace Raven.Client.Documents.AI;

/// <summary>
/// Represents the result of a single conversation turn.
/// </summary>
public enum AiConversationResult
{
    /// <summary>
    /// The conversation has completed and a final answer is available.
    /// </summary>
    Done,

    /// <summary>
    /// Further interaction is required, such as responding to tool requests.
    /// </summary>
    ActionRequired,
}
