namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// Mock-mode <see cref="ILicenseClient"/>: serves a local setup-package zip instead of calling the
/// real license API. Registered in <c>Program.cs</c> when the setup-package zip path is mounted into
/// the container. Mirrors what the real <see cref="LicenseHttpClient"/> downloads, so the activation
/// flow is identical end-to-end. The token is ignored — the mounted zip is the answer for any token.
/// </summary>
public sealed class MockLicenseClient(string setupPackageZipPath) : ILicenseClient
{
    public async Task DownloadSetupPackageToAsync(string token, Stream destination, CancellationToken ct)
    {
        if (File.Exists(setupPackageZipPath) == false)
            throw new LicenseRetrievalException(
                $"mock setup-package zip not found at '{setupPackageZipPath}'.");

        await using var source = File.OpenRead(setupPackageZipPath);
        await SetupPackageDownload.CopyCappedAsync(source, destination, ct);
    }
}
