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

    void MarkReady();
    void MarkFailed(string reason);
}

/// <summary>
/// Single source of truth for the kebab-case wire spelling of each
/// <see cref="BootstrapPhase"/> value. Shared by /api/bootstrap/status and the
/// /healthz description so the two stay in lock-step. (Both used to derive
/// their string from <c>Phase.ToString().ToLowerInvariant()</c> independently,
/// which produced `needsactivation` for one and `needs-activation` for the
/// other — drift the user could see.)
/// </summary>
public static class BootstrapPhaseExtensions
{
    public static string ToWire(this BootstrapPhase phase) => phase switch
    {
        BootstrapPhase.NeedsActivation => "needs-activation",
        BootstrapPhase.Redeeming       => "redeeming",
        BootstrapPhase.Ready           => "ready",
        _ => phase.ToString().ToLowerInvariant(),
    };
}

public sealed class BootstrapStateFlag : IBootstrapState
{
    private int _phase = (int)BootstrapPhase.NeedsActivation;
    private string? _reason;

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
