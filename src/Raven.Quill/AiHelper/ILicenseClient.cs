namespace Raven.Quill.AiHelper;

public interface ILicenseClient
{
    Task DownloadSetupPackageToAsync(string token, Stream destination, CancellationToken ct);
}

public sealed class LicenseRetrievalException(string message) : Exception(message);
