using Raven.Quill.Contracts;

namespace Raven.Quill.Licensing;

public interface ILicenseStatsProvider
{
    Task<LicenseResponse> GetLicenseAsync(CancellationToken token);

    Task<QuillUsageResponse> GetUsageAsync(int? year, int? month, CancellationToken token);
}
