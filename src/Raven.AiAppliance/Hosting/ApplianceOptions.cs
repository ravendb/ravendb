using System.ComponentModel.DataAnnotations;

namespace Raven.AiAppliance.Hosting;

public sealed class ApplianceOptions
{
    public const string SectionName = "Appliance";

    [Required] public string RavenUrl { get; set; } = "http://127.0.0.1:8080";
    [Required] public string WebListenUrl { get; set; } = "http://0.0.0.0:5000";
    [Required] public string ConfigDatabase { get; set; } = ApplianceDatabases.Config;

    /// <summary>
    /// Directory where the redeemed setup-package zip is unpacked and where the
    /// appliance reads its on-boot configuration from (admin client cert, license,
    /// RavenDB node settings). Empty / missing on first start puts the appliance
    /// into NEEDS-ACTIVATION; a successful POST /api/bootstrap/redeem-license
    /// populates it and flips to READY.
    /// </summary>
    public string SetupPackagePath { get; set; } = "/setup";

    /// <summary>
    /// Path to a local setup-package zip used by the activation endpoint instead of
    /// calling <see cref="LicenseApiUrl"/>. <b>Demo-only</b>: the 8-week demo ships a
    /// pre-baked zip mounted into the container; production will fetch
    /// <c>license.json</c> + <c>app-name</c> from the license API and run LE
    /// provisioning locally (no zip path involved). Bound from
    /// <c>RAVEN_AI_SETUP_PACKAGE_ZIP</c>; empty / missing file falls back to the
    /// HTTP path.
    /// </summary>
    public string? SetupPackageZipPath { get; set; }

    /// <summary>
    /// s6-rc service path to restart after the setup package is extracted (e.g.
    /// <c>/run/service/01-ravendb</c>). When set, the activation endpoint
    /// signals s6 to restart RavenDB into secure mode and then exits the .NET
    /// host so s6 brings it back wired to the secure <c>IDocumentStore</c>.
    /// When empty (WAF tests, local <c>dotnet run</c>, any unsupervised host)
    /// the endpoint stays in-process and flips bootstrap to Ready inline —
    /// there's no supervisor to restart us cleanly. Bound from
    /// <c>RAVEN_AI_RAVENDB_S6_SERVICE</c>.
    /// </summary>
    public string? RavenDbS6Service { get; set; }

    /// <summary>
    /// Default upstream license-redemption endpoint. Production uses this as-is;
    /// tests and local dev override via <c>RAVEN_AI_LICENSE_API_URL</c>. Exposed
    /// as a constant so the dev-mode startup warning in <c>Program.cs</c> can
    /// compare against the same string without drifting.
    /// </summary>
    public const string DefaultLicenseApiUrl = "https://api.ravendb.net";

    /// <summary>
    /// Upstream license-redemption endpoint. POST /api/bootstrap/redeem-license
    /// proxies <c>GET {LicenseApiUrl}/licenses/{key}</c> to fetch the signed
    /// setup-package zip on first run. Tests point this at an in-process mock.
    /// </summary>
    public string LicenseApiUrl { get; set; } = DefaultLicenseApiUrl;

    /// <summary>
    /// Default internal AI service endpoint. The AI Helper proxies LLM-backed config
    /// generation to <c>{AiApiUrl}/api/v1/ai/setup/*</c>; production uses this as-is,
    /// tests point it at an in-process <c>MockAiApi</c>.
    /// </summary>
    public const string DefaultAiApiUrl = "https://api.ravendb.net";

    /// <summary>
    /// Internal AI service endpoint for the AI Helper. Bound from <c>RAVEN_AI_API_URL</c>.
    /// <see cref="UrlAttribute"/> + the options pipeline's <c>ValidateOnStart</c> reject a
    /// malformed value (missing scheme, whitespace) at boot, so <c>new Uri(AiApiUrl)</c>
    /// cannot throw a <see cref="UriFormatException"/> on the first
    /// <c>AiHelperInternalClient</c> resolution.
    /// </summary>
    [Url]
    public string AiApiUrl { get; set; } = DefaultAiApiUrl;

    /// <summary>
    /// Silent grace period before the first readiness probe fires. RavenDB
    /// reliably takes ~10-15s to start, so pinging earlier just generates
    /// noise. Logged once at info level, then we wait.
    /// </summary>
    public TimeSpan ReadinessInitialDelay { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan ReadinessAttemptTimeout { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan ReadinessOverallTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
