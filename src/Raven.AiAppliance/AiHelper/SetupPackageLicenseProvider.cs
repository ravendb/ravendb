using System.Text.Json;
using Microsoft.Extensions.Options;
using Raven.AiAppliance.Hosting;

namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// Reads the appliance license from <c>{SetupPackagePath}/license.json</c>: the license the
/// RavenDB server was activated with (written by the license API at activation; present on disk
/// in production as well as in the demo zip). Returns <c>false</c> when the file is absent or
/// malformed so callers degrade to <c>InvalidCredentials</c> rather than throwing.
/// </summary>
public sealed class SetupPackageLicenseProvider(IOptions<ApplianceOptions> options) : IApplianceLicenseProvider
{
    private static readonly JsonSerializerOptions LicenseJsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly ApplianceOptions _options = options.Value;

    public bool TryGetLicense(out ApplianceLicense license)
    {
        license = null!;

        // TODO: interim source: read license.json from the redeemed setup package on disk. When the
        // license API is available, pull the license from it instead (likely cache to a local file) and
        // attach that to the internal AI-helper request. See AI Appliance license API (RavenDB-26661).
        var path = Path.Combine(_options.SetupPackagePath, "license.json");
        if (!File.Exists(path))
            return false;

        try
        {
            license = JsonSerializer.Deserialize<ApplianceLicense>(File.ReadAllText(path), LicenseJsonOptions)!;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // Read/parse failure (TOCTOU delete, denied perms, missing dir, malformed JSON):
            // return false so callers degrade to InvalidCredentials rather than a 500.
            return false;
        }

        return license is not null && string.IsNullOrEmpty(license.Id) == false;
    }
}
