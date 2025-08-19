namespace Raven.Client.Documents.AI;

/// <summary>
/// Represents a typed answer returned from an AI conversation turn.
/// Contains the model-produced content and a status indicating whether
/// the conversation is complete or requires additional action.
/// </summary>
public class AiAnswer<TAnswer>
{
    /// <summary>
    /// The answer content produced by the AI.
    /// </summary>
    public TAnswer Answer;

    /// <summary>
    /// The status of the conversation.
    /// </summary>
    public AiConversationResult Status;
}
