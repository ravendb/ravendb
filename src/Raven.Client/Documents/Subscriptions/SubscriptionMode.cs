namespace Raven.Client.Documents.Subscriptions;

/// <summary>
/// Defines how a subscription may be connected by clients.
/// </summary>
public enum SubscriptionMode
{
    /// <summary>
    /// No specific mode was set.
    /// </summary>
    None,
    /// <summary>
    /// A single client may be connected to the subscription at a time.
    /// </summary>
    Single,
    /// <summary>
    /// Multiple clients may connect concurrently (when supported by server configuration).
    /// </summary>
    Concurrent
}
