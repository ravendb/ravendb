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
    /// <see cref="SubscriptionWorkerState.Faulted"/>, <c>null</c> otherwise.
    /// </summary>
    public Exception Exception { get; }

    /// <summary>
    /// When the worker entered this state. Repeatedly reaching the same state does not move it, so a worker that
    /// keeps alternating between processing batches will report the time its current batch started, and one that
    /// cannot get past connecting will report how long it has been stuck.
    /// </summary>
    public DateTime SinceUtc { get; }

    internal SubscriptionWorkerStatus(SubscriptionWorkerState state, Exception exception, DateTime sinceUtc)
    {
        State = state;
        Exception = exception;
        SinceUtc = sinceUtc;
    }

    public override string ToString()
    {
        if (Exception == null)
            return $"{State} since {SinceUtc:O}";

        return $"{State} since {SinceUtc:O} because of {Exception.GetType().Name}: {Exception.Message}";
    }
}
