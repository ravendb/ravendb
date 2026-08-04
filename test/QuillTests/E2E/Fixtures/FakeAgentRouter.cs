using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;

namespace QuillTests.E2E.Fixtures;

/// A resettable <see cref="IAgentRouter"/> that records every request and streams a scripted chunk sequence;
/// reused across the Telegram collection so pipeline tests assert on what the poller dispatched without a live
/// LLM. <see cref="BeforeRun"/> gates a run (per-chat serialization tests); <see cref="Failure"/> makes it throw.
internal sealed class FakeAgentRouter : IAgentRouter
{
    private readonly object _lock = new();
    private readonly List<AgentRequest> _requests = [];

    public IReadOnlyList<AgentRequest> Requests
    {
        get { lock (_lock) return _requests.ToArray(); }
    }

    public string[] Chunks { get; set; } = ["Hello ", "from the fake agent."];

    /// The full reply; defaults to the concatenated chunks.
    public string? Reply { get; set; }

    /// Pause between chunks — longer than the host's edit debounce, it forces edit-in-place flushes.
    public TimeSpan ChunkDelay { get; set; }

    /// Awaited before streaming starts — a TaskCompletionSource here holds a run open.
    public Func<AgentRequest, Task>? BeforeRun { get; set; }

    public Exception? Failure { get; set; }

    public async Task<AgentRunResult> RunAsync(
        AgentRequest request, Func<string, ValueTask> onChunk, AiAgentConfiguration config, CancellationToken ct)
    {
        lock (_lock)
            _requests.Add(request);

        if (BeforeRun is not null)
            await BeforeRun(request);

        if (Failure is not null)
            throw Failure;

        foreach (var chunk in Chunks)
        {
            await onChunk(chunk);
            if (ChunkDelay > TimeSpan.Zero)
                await Task.Delay(ChunkDelay, ct);
        }

        var reply = Reply ?? string.Concat(Chunks);
        return new AgentRunResult(new { reply }, request.ConversationId, reply);
    }

    public void Reset()
    {
        lock (_lock)
            _requests.Clear();
        Chunks = ["Hello ", "from the fake agent."];
        Reply = null;
        ChunkDelay = TimeSpan.Zero;
        BeforeRun = null;
        Failure = null;
    }
}
