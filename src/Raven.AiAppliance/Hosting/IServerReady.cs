namespace Raven.AiAppliance.Hosting;

/// <summary>
/// Internal probe-success signal — flips to true once
/// <see cref="RavenReadinessService"/> has successfully reached RavenDB at
/// least once. <b>Not the <c>/healthz</c> source of truth</b> — that's
/// <see cref="IBootstrapState"/>, which also models the pre-activation phases
/// (NeedsActivation / Redeeming). Both are written together by the readiness
/// service so internal callers that only care about "did we ever connect?"
/// can use this flag without parsing the bootstrap phase.
/// </summary>
public interface IServerReady
{
    bool IsReady { get; }
    string? LastError { get; }
    void MarkReady();
    void MarkFailed(string error);
}

public sealed class ServerReadyFlag : IServerReady
{
    private int _ready;
    private string? _lastError;

    public bool IsReady => Volatile.Read(ref _ready) == 1;
    public string? LastError => Volatile.Read(ref _lastError);

    public void MarkReady()
    {
        Volatile.Write(ref _lastError, null);
        Volatile.Write(ref _ready, 1);
    }

    public void MarkFailed(string error)
    {
        Volatile.Write(ref _lastError, error);
        Volatile.Write(ref _ready, 0);
    }
}
