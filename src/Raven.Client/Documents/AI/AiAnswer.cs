using System;
using Raven.Client.Documents.Operations.AI;

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

    /// <summary>
    /// Token usage reported by the model for generating this answer (prompt/completion/total).
    /// </summary>
    public AiUsage Usage;

    /// <summary>
    /// The total time elapsed to produce the answer(measured from the server's request to the LLM until the response was received).
    /// </summary>
    public TimeSpan Elapsed;

    /// <summary>
    /// An opaque token that captures the conversation state <em>before</em> the current turn was processed.
    /// Pass this token to <see cref="AiOperations.ForkConversationAsync"/> to create a new conversation
    /// that branches from this point — the forked conversation will contain all messages up to (but not
    /// including) the prompt that produced this answer.
    ///
    /// <para>
    /// This property is only populated when the conversation was opened with
    /// <see cref="AiConversationCreationOptions.SnapshotBeforeRunning"/> set to <c>true</c>.
    /// It is <c>null</c> when snapshots are disabled or when the conversation is brand-new
    /// (no prior state to snapshot).
    /// </para>
    ///
    /// <para>
    /// The token references document revisions. If those revisions are purged (by the revisions
    /// retention policy or by <see cref="AiOperations.PurgeConversationSnapshotsAsync"/>),
    /// the token becomes invalid and <see cref="AiOperations.ForkConversationAsync"/> will fail.
    /// </para>
    /// </summary>
    public string SnapshotToken;
}
