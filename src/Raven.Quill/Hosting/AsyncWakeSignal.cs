namespace Raven.Quill.Hosting;

internal sealed class AsyncWakeSignal
{
    private TaskCompletionSource _signal = Fresh();

    private static TaskCompletionSource Fresh() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public void Set() => Volatile.Read(ref _signal).TrySetResult();

    public void Reset()
    {
        var signal = Volatile.Read(ref _signal);

        if (signal.Task.IsCompleted)
            Interlocked.CompareExchange(ref _signal, Fresh(), signal);
    }

    public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken ct)
    {
        var signalled = Volatile.Read(ref _signal).Task;

        using var timer = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var expiry = Task.Delay(timeout, timer.Token);

        if (await Task.WhenAny(signalled, expiry).ConfigureAwait(false) == expiry)
        {
            await expiry.ConfigureAwait(false);
            return false;
        }

        await timer.CancelAsync().ConfigureAwait(false);
        return true;
    }
}
