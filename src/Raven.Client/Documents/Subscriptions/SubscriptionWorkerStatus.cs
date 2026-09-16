using System;

namespace Raven.Client.Documents.Subscriptions;

/// <summary>
/// An immutable snapshot of what a subscription worker was doing at a single point in time. Taking a snapshot
/// keeps the state and the failure that produced it consistent with each other, which reading two separate
/// properties would not.
/// </summary>
public sealed class SubscriptionWorkerStatus
{
    /// <summary>
    /// The stage the worker was in.
    /// </summary>
    public SubscriptionWorkerState State { get; }

    /// <summary>
    /// The failure that led to the current state. Set for <see cref="SubscriptionWorkerState.Retrying"/> and
    /// <see cref="SubscriptionWorkerState.Faulted"/>, <c>null</c> otherwise. On a worker that keeps failing this
    /// is the most recent failure, which is not necessarily the one that started the trouble - see
    /// <see cref="FailingSinceUtc"/>.
    /// </summary>
    public Exception Exception { get; }

    /// <summary>
    /// When the worker entered this state, and nothing more than that. Reaching the same state again does not
    /// move it, so a worker that keeps processing batches reports the time its current batch started. A worker
    /// that cannot reach the server, however, alternates between <see cref="SubscriptionWorkerState.Connecting"/>
    /// and <see cref="SubscriptionWorkerState.Retrying"/>, so this moves on every attempt. Use
    /// <see cref="FailingSinceUtc"/> to tell how long it has been in trouble.
    /// </summary>
    public DateTime SinceUtc { get; }

    /// <summary>
    /// When the worker last stopped being able to talk to the server, or <c>null</c> while it is connected.
    /// Unlike <see cref="SinceUtc"/> this survives the whole reconnect cycle, so it answers "how long has this
    /// worker been broken?" for a worker that is retrying in a loop. It is cleared as soon as the worker is
    /// connected again, which makes a non-<c>null</c> value the thing to alert on.
    /// </summary>
    public DateTime? FailingSinceUtc { get; }

    internal SubscriptionWorkerStatus(SubscriptionWorkerState state, Exception exception, DateTime sinceUtc, DateTime? failingSinceUtc)
    {
        State = state;
        Exception = exception;
        SinceUtc = sinceUtc;
        FailingSinceUtc = failingSinceUtc;
    }

    public override string ToString()
    {
        string text = $"{State} since {SinceUtc:O}";

        if (FailingSinceUtc != null)
            text += $", failing since {FailingSinceUtc:O}";

        if (Exception != null)
            text += $" because of {Exception.GetType().Name}: {Exception.Message}";

        return text;
    }
}
