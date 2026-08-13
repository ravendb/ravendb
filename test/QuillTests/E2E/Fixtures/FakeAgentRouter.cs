using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;

namespace QuillTests.E2E.Fixtures;

internal sealed class FakeAgentRouter : IAgentRouter
{
    private readonly object _lock = new();
    private readonly List<AgentRequest> _requests = [];

    public IReadOnlyList<AgentRequest> Requests
    {
        get { lock (_lock) return _requests.ToArray(); }
    }

    public string[] Chunks { get; set; } = ["Hello ", "from the fake agent."];

    public TimeSpan ChunkDelay { get; set; }

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

        var reply = string.Concat(Chunks);
        return new AgentRunResult(new { reply }, request.ConversationId);
    }

    public void Reset()
    {
        lock (_lock)
            _requests.Clear();
        Chunks = ["Hello ", "from the fake agent."];
        ChunkDelay = TimeSpan.Zero;
        BeforeRun = null;
        Failure = null;
    }
}
