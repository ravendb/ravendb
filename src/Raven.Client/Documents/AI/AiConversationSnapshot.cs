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
}
