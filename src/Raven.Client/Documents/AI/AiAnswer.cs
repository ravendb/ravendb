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
    /// The total time elapsed to produce the answer(measured from the server's request to the LLM until the response was received).
    /// </summary>
    public TimeSpan Elapsed;
}
