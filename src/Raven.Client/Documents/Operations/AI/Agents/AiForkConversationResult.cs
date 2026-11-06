using Sparrow.Json;

namespace Raven.Client.Documents.Operations.AI.Agents;

/// <summary>
/// The result of a <see cref="ForkConversationOperation"/>.
/// </summary>
public sealed class AiForkConversationResult
{
    /// <summary>
    /// When the server generated the ID (because <c>newConversationId</c> was <c>null</c>,
    /// or ended with <c>"/"</c> or <c>"|"</c>), this contains the server-assigned ID.
    /// Otherwise, this is the same as the <c>newConversationId</c> that was passed.
    /// </summary>
    public string ConversationId { get; set; }

    /// <summary>
    /// The change vector of the forked conversation document.
    /// Pass this to <see cref="AI.AiOperations.Conversation"/> for optimistic concurrency control
    /// on the first turn after forking.
    /// </summary>
    public string ChangeVector { get; set; }

    internal static AiForkConversationResult Convert(BlittableJsonReaderObject response)
    {
        response.TryGet(nameof(ConversationId), out string conversationId);
        response.TryGet(nameof(ChangeVector), out string changeVector);

        return new AiForkConversationResult
        {
            ConversationId = conversationId,
            ChangeVector = changeVector
        };
    }
}
