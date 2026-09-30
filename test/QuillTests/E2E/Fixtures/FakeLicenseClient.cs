using Raven.Quill.AiHelper;

namespace QuillTests.E2E.Fixtures;

/// In-process stand-in for the license download: writes the setup-package bytes when the presented key
/// matches, throws <see cref="LicenseRetrievalException"/> otherwise (mirrors the 404 path).
public sealed class FakeLicenseClient(string licenseKey, byte[] setupPackageZipBytes) : ILicenseClient
{
    public async Task DownloadSetupPackageToAsync(string presentedKey, Stream destination, CancellationToken ct)
    {
        if (string.Equals(presentedKey, licenseKey, StringComparison.Ordinal) == false)
            throw new LicenseRetrievalException("license API returned 404 NotFound retrieving the setup package.");

        await destination.WriteAsync(setupPackageZipBytes, ct);
    }
}
