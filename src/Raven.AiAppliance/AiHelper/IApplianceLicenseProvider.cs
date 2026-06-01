namespace Raven.AiAppliance.AiHelper;

/// <summary>
/// Supplies the appliance license forwarded to the internal AI service. The cert thumbprint
/// is sourced separately by the client from <c>IDocumentStore.Certificate</c>,
/// mirroring the Studio "current client cert" path.
/// </summary>
public interface IApplianceLicenseProvider
{
    /// <summary>
    /// Returns <c>true</c> and the license when the redeemed setup package carries a usable
    /// <c>license.json</c>. Returns <c>false</c> when absent or unusable; the caller surfaces
    /// that as <c>InvalidCredentials</c> rather than a 500.
    /// </summary>
    bool TryGetLicense(out ApplianceLicense license);
}
