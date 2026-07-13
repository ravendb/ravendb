namespace Raven.Quill;

/// <summary>
/// Well-known appliance-level database identifiers. Per-app databases (one per
/// application provisioned via the wizard) carry the app's own name and are
/// not enumerated here.
/// </summary>
public static class ApplianceDatabases
{
    /// <summary>
    /// Central appliance-level "system" database. Holds the registry of
    /// provisioned apps (under the <c>apps/</c> id prefix, e.g. <c>apps/1-A</c>;
    /// not the <c>@</c>-prefixed RavenDB system-document namespace), the
    /// dashboard API key hash
    /// (Phase 1+), the cached signed license (Phase 5+), telemetry opt-in,
    /// magic-link nonces, and aggregated write counters. Per-app data lives
    /// inside each app's own database, not here. See design doc §1.1 / §1.2.
    /// </summary>
    public const string Config = "quill-config";
}
