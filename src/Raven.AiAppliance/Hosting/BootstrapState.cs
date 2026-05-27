using Microsoft.Extensions.Options;

namespace Raven.AiAppliance.Hosting;

/// <summary>
/// Top-level boot mode of the appliance, driven by whether a setup package
/// has been redeemed and applied yet.
/// </summary>
public enum BootstrapPhase
{
    /// <summary>
    /// `/setup/` is empty (or missing the appliance config). RavenDB has not
    /// started; only <c>/api/bootstrap/*</c> + the first-run UI are live.
    /// </summary>
    NeedsActivation,

    /// <summary>
    /// A license-redemption call is in flight — fetching the setup package,
    /// unpacking, initialising the secured `IDocumentStore`. Non-bootstrap
    /// endpoints still return 503.
    /// </summary>
    Redeeming,

    /// <summary>
    /// Setup package applied; RavenDB and the .NET host are restarting so the
    /// secure config takes effect. Non-bootstrap endpoints return 503; the
    /// frontend polls /api/bootstrap/status and waits for `ready`.
    /// </summary>
    Restarting,

    /// <summary>
    /// Setup package installed + RavenDB reachable; wizard endpoints live.
    /// </summary>
    Ready,
}

public interface IBootstrapState
{
    BootstrapPhase Phase { get; }
    string? Reason { get; }

    /// <summary>
    /// Attempts to transition from <see cref="BootstrapPhase.NeedsActivation"/>
    /// to <see cref="BootstrapPhase.Redeeming"/>. Returns <c>true</c> if this
    /// caller won the race; <c>false</c> if another redemption is already in
    /// flight or has completed. Implementations must use an atomic
    /// compare-and-swap so concurrent <c>POST /api/bootstrap/redeem-license</c>
    /// calls (e.g. an operator double-click) don't both extract zips into
    /// <c>/setup/</c>.
    /// </summary>
    bool TryMarkRedeeming();

    /// <summary>
    /// Attempts to transition from <see cref="BootstrapPhase.Redeeming"/> to
    /// <see cref="BootstrapPhase.Restarting"/>. Returns <c>true</c> on success.
    /// Used after the setup package is on disk but before the .NET host is
    /// signalled to exit, so the final HTTP response can carry the `restarting`
    /// state for the frontend's poll loop.
    /// </summary>
    bool TryMarkRestarting();

    void MarkRestarting(string? reason = null);
    void MarkReady();
    void MarkFailed(string reason);
}

/// <summary>
/// Single source of truth for the kebab-case wire spelling of each
/// <see cref="BootstrapPhase"/> value on non-JSON surfaces such as /healthz.
/// The JSON API returns the enum directly and relies on
/// <see cref="System.Text.Json.Serialization.JsonStringEnumConverter"/> to emit
/// the PascalCase name.
/// </summary>
public static class BootstrapPhaseExtensions
{
    public static string ToWire(this BootstrapPhase phase) => phase switch
    {
        BootstrapPhase.NeedsActivation => "needs-activation",
        BootstrapPhase.Redeeming       => "redeeming",
        BootstrapPhase.Restarting      => "restarting",
        BootstrapPhase.Ready           => "ready",
        _ => phase.ToString().ToLowerInvariant(),
    };
}

public sealed class BootstrapStateFlag : IBootstrapState
{
    private int _phase;
    private string? _reason;

    public BootstrapStateFlag(IOptions<ApplianceOptions> options)
    {
        var setupSettings = Path.Combine(options.Value.SetupPackagePath, "A", "settings.json");
        _phase = File.Exists(setupSettings)
            ? (int)BootstrapPhase.Restarting
            : (int)BootstrapPhase.NeedsActivation;
    }

    public BootstrapPhase Phase => (BootstrapPhase)Volatile.Read(ref _phase);
    public string? Reason => Volatile.Read(ref _reason);

    public bool TryMarkRedeeming()
    {
        // CAS NeedsActivation -> Redeeming. Only the winner clears _reason and
        // returns true; concurrent callers observe Redeeming/Ready and bail.
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
