namespace Raven.Quill.Hosting;

/// A wake signal for a single consuming loop: <see cref="Set"/> is called from anywhere and coalesces any
/// number of wakes into one, while <see cref="Reset"/> and <see cref="WaitAsync"/> belong to the loop.
/// Reset before doing the work, so a Set arriving mid-pass lands on the fresh source and the next wait
/// returns immediately rather than losing the wakeup.
/// Sparrow.Server's AsyncManualResetEvent is the same primitive, but it reaches into Sparrow internals that
/// are not visible here.
internal sealed class AsyncWakeSignal
{
    private TaskCompletionSource _signal = Fresh();

    private static TaskCompletionSource Fresh() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Set() => Volatile.Read(ref _signal).TrySetResult();

    public void Reset()
    {
        var signal = Volatile.Read(ref _signal);

        // an unsignalled source is already the fresh one
        if (signal.Task.IsCompleted)
            Interlocked.CompareExchange(ref _signal, Fresh(), signal);
    }

    /// True when woken by <see cref="Set"/>, false when the timeout expired first. Throws when
    /// <paramref name="ct"/> is cancelled.
    public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct)
    {
        var signalled = Volatile.Read(ref _signal).Task;

        using var timer = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var expiry = Task.Delay(timeout, timer.Token);

        if (await Task.WhenAny(signalled, expiry).ConfigureAwait(false) == expiry)
        {
            await expiry.ConfigureAwait(false);   // surfaces cancellation rather than reporting a tick
            return false;
        }

        // drop the timer instead of leaving a callback registered for the rest of the interval
        await timer.CancelAsync().ConfigureAwait(false);
        return true;
    }
}
