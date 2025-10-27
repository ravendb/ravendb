using System;
using Raven.Client.Documents.Operations.AI;

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

    /// <summary>
    /// Token usage reported by the model for generating this answer (prompt/completion/total).
    /// </summary>
    public AiUsage Usage;

    /// <summary>
    /// The time the answer was produced, model-provided creation time if available, otherwise server time.
    /// </summary>
    public DateTime? Time;
}
