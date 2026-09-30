namespace Raven.Quill.AiHelper;

public interface ILicenseClient
{
    Task DownloadSetupPackageToAsync(string licenseKey, Stream destination, CancellationToken ct);
}

public sealed class LicenseRetrievalException(string message) : Exception(message);

public sealed class LicenseKeyNotFoundException(string message) : Exception(message);
