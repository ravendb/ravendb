using System.Collections.Concurrent;

namespace Raven.Quill.AiHelper.Migration;

/// <summary>
/// One writer at a time per app's plan. A planner turn mirrors its frames into the stored plan from the copy it
/// loaded when it began, so an edit saved meanwhile would be overwritten without a trace. A turn holds the plan
/// from before it loads it until its stream ends. A turn or an edit that finds the plan still held after a short grace is refused.
/// </summary>
public sealed class MigrationTurns
{
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _plans = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// A turn the browser just stopped still holds the plan until the server notices the aborted request; what
    /// the operator does right after (start over, remove) waits that long rather than being refused.
    /// </summary>
    private static readonly TimeSpan ReleaseGrace = TimeSpan.FromSeconds(2);

    /// <summary>Null while another turn or an edit holds the plan.</summary>
    public Task<IDisposable?> TryBeginTurnAsync(string slug, CancellationToken token) => TryHoldAsync(slug, token);

    /// <summary>Null while a turn holds the plan.</summary>
    public Task<IDisposable?> TryBeginEditAsync(string slug, CancellationToken token) => TryHoldAsync(slug, token);

    private async Task<IDisposable?> TryHoldAsync(string slug, CancellationToken token)
    {
        var gate = Gate(slug);
        return await gate.WaitAsync(ReleaseGrace, token) ? new Held(gate) : null;
    }

    private SemaphoreSlim Gate(string slug) => _plans.GetOrAdd(slug, _ => new SemaphoreSlim(1, 1));

    private sealed class Held(SemaphoreSlim gate) : IDisposable
    {
        private int _released;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                gate.Release();
        }
    }
}
