namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// Abstraction over retrieving the appliance setup-package zip by activation token.
/// The production implementation (<see cref="LicenseHttpClient"/>) calls the public
/// license API on api.ravendb.net (RavenDB-26783: <c>GET /api/v{version}/quill/licenses/{token}</c>,
/// which returns the full setup-package zip — server cert + admin client cert + per-node
/// <c>settings.json</c> + <c>license.json</c> + <c>setup.json</c>). <see cref="MockLicenseClient"/>
/// serves a local zip in mock mode (the same toggle the AI Helper uses), so the appliance can
/// activate without the real API being available.
/// </summary>
public interface ILicenseClient
{
    /// <summary>
    /// Streams the setup-package zip for <paramref name="token"/> into <paramref name="destination"/>.
    /// The implementation owns the upstream response/file lifetime within the call and enforces a
    /// size cap (<see cref="SetupPackageDownload.MaxSetupPackageBytes"/>). Throws
    /// <see cref="LicenseRetrievalException"/> when the package cannot be retrieved.
    /// </summary>
    Task DownloadSetupPackageToAsync(string token, Stream destination, CancellationToken ct);
}

/// <summary>Raised when the setup package can't be retrieved (upstream non-success, transport failure).</summary>
public sealed class LicenseRetrievalException(string message) : Exception(message);
