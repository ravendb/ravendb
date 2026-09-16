using Microsoft.Extensions.Options;

namespace Raven.Quill.Hosting;

public enum BootstrapPhase
{
    NeedsActivation,

    Redeeming,

    Restarting,

    Ready,
}

public interface IBootstrapState
{
    BootstrapPhase Phase { get; }
    string? Reason { get; }

    bool StartedWithSetupPackage { get; }

    bool TryMarkRedeeming();

    bool TryMarkRestarting();

    void MarkRestarting(string? reason = null);
    void MarkReady();
    void MarkFailed(string reason);
}

public static class BootstrapPhaseExtensions
{
    public static string ToWire(this BootstrapPhase phase) => phase switch
    {
        BootstrapPhase.NeedsActivation => "needs-activation",
        BootstrapPhase.Redeeming => "redeeming",
        BootstrapPhase.Restarting => "restarting",
        BootstrapPhase.Ready => "ready",
        _ => phase.ToString().ToLowerInvariant(),
    };
}

public sealed class BootstrapStateFlag : IBootstrapState
{
    private int _phase;
    private string? _reason;
    private readonly bool _startedWithSetupPackage;

    public BootstrapStateFlag(IOptions<ApplianceOptions> options)
    {
        _startedWithSetupPackage = File.Exists(options.Value.SetupNodeSettingsPath);
        _phase = _startedWithSetupPackage
            ? (int)BootstrapPhase.Restarting
            : (int)BootstrapPhase.NeedsActivation;
    }

    public BootstrapPhase Phase => (BootstrapPhase)Volatile.Read(ref _phase);
    public string? Reason => Volatile.Read(ref _reason);
    public bool StartedWithSetupPackage => _startedWithSetupPackage;

    // CAS: only the winner extracts the setup package, exactly once
    public bool TryMarkRedeeming()
    {
        var previous = Interlocked.CompareExchange(
            ref _phase,
            (int)BootstrapPhase.Redeeming,
            (int)BootstrapPhase.NeedsActivation);

        if (previous != (int)BootstrapPhase.NeedsActivation)
            return false;

        Volatile.Write(ref _reason, null);
        return true;
    }

    public bool TryMarkRestarting()
    {
        var previous = Interlocked.CompareExchange(
            ref _phase,
            (int)BootstrapPhase.Restarting,
            (int)BootstrapPhase.Redeeming);

        return previous == (int)BootstrapPhase.Redeeming;
    }

    public void MarkRestarting(string? reason = null)
    {
        Volatile.Write(ref _reason, reason);
        Volatile.Write(ref _phase, (int)BootstrapPhase.Restarting);
    }

    public void MarkReady()
    {
        Volatile.Write(ref _reason, null);
        Volatile.Write(ref _phase, (int)BootstrapPhase.Ready);
    }

    public void MarkFailed(string reason)
    {
        Volatile.Write(ref _reason, reason);
        Volatile.Write(ref _phase, (int)BootstrapPhase.NeedsActivation);
    }
}
