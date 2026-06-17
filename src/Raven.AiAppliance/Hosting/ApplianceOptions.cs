using System.ComponentModel.DataAnnotations;

namespace Raven.AiAppliance.Hosting;

public sealed class ApplianceOptions
{
    public const string SectionName = "Appliance";

    [Required] public string RavenUrl { get; set; } = "http://127.0.0.1:8080";
    [Required] public string WebListenUrl { get; set; } = "http://0.0.0.0:5000";
    [Required] public string ConfigDatabase { get; set; } = ApplianceDatabases.Config;

    /// <summary>
    /// Loopback HTTPS port RavenDB binds inside the container once the nginx <c>:443</c> SNI front owns
    /// 443 (Phase 1). The secure store connects to RavenDB on this port; the s6 <c>01-ravendb</c> run
    /// script rewrites the setup package's <c>ServerUrl</c> to match. Bound from
    /// <c>RAVEN_AI_RAVENDB_INTERNAL_PORT</c>. Keep in sync with that script.
    /// </summary>
    public int RavenInternalPort { get; set; } = 8443;

    /// <summary>
    /// Directory where the redeemed setup-package zip is unpacked and where the
    /// appliance reads its on-boot configuration from (admin client cert, license,
    /// RavenDB node settings). Empty / missing on first start puts the appliance
    /// into NEEDS-ACTIVATION; startup activation (ApplianceActivationService)
    /// populates it and flips to READY.
    /// </summary>
    public string SetupPackagePath { get; set; } = "/setup";

    /// <summary>
    /// Activation token, bound from <c>QUILL_LICENSE_KEY</c>. At startup
    /// <see cref="ApplianceActivationService"/> pulls the setup-package zip for this token from
    /// <see cref="LicenseApiUrl"/> (RavenDB-26783, <c>GET /api/v{version}/quill/licenses/{token}</c>).
    /// Required in production; ignored in mock mode (the mounted zip answers any token). The value is
    /// never logged.
    /// </summary>
    public string? LicenseToken { get; set; }

    /// <summary>
    /// Operator API key, bound from <c>QUILL_API_KEY</c>. The single source of truth for admin auth in
    /// beta: the <c>api.*</c> header credential and the <c>dashboard.*</c> login both validate against
    /// it (see <see cref="Auth.ApiKeyStore"/>), and its salted hash is hard-overwritten into the config
    /// DB. Fail-closed when unset. The value is never logged.
    /// </summary>
    public string? ApiKey { get; set; }

    /// <summary>
    /// Path to a local setup-package zip. <b>Mock-only</b>: when set (file present) the appliance runs
    /// in mock mode — <see cref="AiHelper.MockLicenseClient"/> serves this zip instead of calling the
    /// real license API, and the AI Helper uses <see cref="AiHelper.MockAiHelperClient"/>. Bound from
    /// <c>RAVEN_AI_SETUP_PACKAGE_ZIP</c>; empty / missing file selects the real HTTP clients.
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
