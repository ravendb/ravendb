namespace Raven.Client.Documents.AI;

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
