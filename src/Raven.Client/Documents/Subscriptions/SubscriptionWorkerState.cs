namespace Raven.Client.Documents.Subscriptions;

/// <summary>
/// The stage a subscription worker is currently in. Read it through
/// <see cref="AbstractSubscriptionWorker{TBatch,TType}.Status"/>.
/// </summary>
public enum SubscriptionWorkerState
{
    /// <summary>
    /// The worker was created but none of the Run overloads was called yet, so it is not doing anything.
    /// </summary>
    NotStarted,

    /// <summary>
    /// Opening a connection to the server, which includes resolving the node that owns the subscription and
    /// the TCP handshake. This is also the state while reconnecting after a failure.
    /// </summary>
    Connecting,

    /// <summary>
    /// Connected to the server and waiting for it to send the next batch. The worker is healthy and idle -
    /// there is nothing for it to process right now.
    /// </summary>
    WaitingForDocuments,

    /// <summary>
    /// A batch was received and handed to the subscriber callback. The state stays here until the callback
    /// returned and the batch was acknowledged to the server.
    /// </summary>
    Processing,

    /// <summary>
    /// The connection failed and the worker is going to reconnect.
    /// <see cref="SubscriptionWorkerStatus.Exception"/> holds the failure that caused it.
    /// </summary>
    Retrying,

    /// <summary>
    /// The worker gave up - the failure it hit cannot be recovered from by reconnecting, so it stopped and the
    /// task returned from Run completed with that failure. <see cref="SubscriptionWorkerStatus.Exception"/>
    /// holds it. This state is terminal.
    /// </summary>
    Faulted,

    /// <summary>
    /// The worker stopped on request - it was disposed, or the cancellation token passed to Run was cancelled.
    /// This state is terminal.
    /// </summary>
    Stopped
}
