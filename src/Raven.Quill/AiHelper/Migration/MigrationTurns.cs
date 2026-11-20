using System.Collections.Concurrent;
using Raven.Quill.Contracts;

namespace Raven.Quill.AiHelper.Migration;

public sealed class MigrationTurns
{
    private const string SaveKey = "Save";

    private static readonly MigrationService.Refusal TurnAlreadyRunning = new("a planner turn is already running for this conversation", IsConflict: true);

    private static readonly MigrationService.Refusal SaveRunning = new("the plan is being saved; try again when the save finishes", IsConflict: true);

    private static readonly MigrationService.Refusal TurnsRunning = new("the planner is working on the plan; save again when it finishes", IsConflict: true);

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, Turn>> _apps = new(StringComparer.OrdinalIgnoreCase);

    public Turn TryBeginTurn(string slug, PlannerAgent agent, string? area)
    {
        var running = Running(slug);
        var turn = new Turn(running, GetKey(agent, area));
        if (running.TryAdd(turn.Key, turn) == false)
            return Refuse(turn, TurnAlreadyRunning);

        if (running.ContainsKey(SaveKey))
            return Refuse(turn, SaveRunning);

        return turn;
    }

    public Turn TryBeginSave(string slug)
    {
        var running = Running(slug);
        var save = new Turn(running, SaveKey);
        if (running.TryAdd(SaveKey, save) == false)
            return Refuse(save, SaveRunning);

        if (running.Count > 1)
            return Refuse(save, TurnsRunning);

        return save;
    }

    public void Stop(string slug, PlannerAgent agent, string? area)
    {
        if (Running(slug).TryGetValue(GetKey(agent, area), out var turn))
            turn.Stop();
    }

    private static Turn Refuse(Turn turn, MigrationService.Refusal refusal)
    {
        turn.Dispose();
        turn.Refusal = refusal;
        return turn;
    }

    private static string GetKey(PlannerAgent agent, string? area) =>
        agent == PlannerAgent.Proposal ? nameof(PlannerAgent.Proposal) : $"{nameof(PlannerAgent.Area)}:{area}";

    private ConcurrentDictionary<string, Turn> Running(string slug) =>
        _apps.GetOrAdd(slug, _ => new ConcurrentDictionary<string, Turn>(StringComparer.OrdinalIgnoreCase));

    public sealed class Turn(ConcurrentDictionary<string, Turn> running, string key) : IDisposable
    {
        // not disposed: a stop may reach the turn after it ended, and it holds no timer
        private readonly CancellationTokenSource _stop = new();

        internal string Key => key;

        public MigrationService.Refusal? Refusal { get; internal set; }

        public CancellationToken Stopped => _stop.Token;

        internal void Stop() => _stop.Cancel();

        public void Dispose() => running.TryRemove(new KeyValuePair<string, Turn>(key, this));
    }
}
