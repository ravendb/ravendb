using System;

namespace Raven.Client.Documents.AI;

/// <summary>
/// Describes a single conversation snapshot available for forking.
/// </summary>
public sealed class AiConversationSnapshot
{
    /// <summary>
    /// The opaque token to pass to <see cref="AiOperations.ForkConversationAsync"/>.
    /// </summary>
    public string Token { get; set; }

    /// <summary>
    /// The date and time (UTC) when this snapshot was created, taken from the root
    /// conversation revision's timestamp.
    /// </summary>
    public DateTime CreatedAt { get; set; }

    /// <summary>
    /// The change vector of the conversation document after the snapshot was created.
    /// Creating a snapshot force-creates a revision, which advances the document's change
    /// vector; this is the resulting value, used to re-baseline a conversation's cached
    /// change vector so a subsequent turn does not fail with a concurrency conflict.
    /// </summary>
    public string ChangeVector { get; set; }
}
