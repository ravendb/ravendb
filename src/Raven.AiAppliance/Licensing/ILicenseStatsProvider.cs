using Raven.AiAppliance.Contracts;

namespace Raven.AiAppliance.Licensing;

public interface ILicenseStatsProvider
{
    Task<LicenseResponse> GetLicenseAsync(CancellationToken token);

    Task<QuillUsageResponse> GetUsageAsync(int? year, int? month, CancellationToken token);
}
