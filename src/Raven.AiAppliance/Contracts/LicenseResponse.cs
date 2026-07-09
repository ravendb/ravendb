using System.Text.Json;

namespace Raven.AiAppliance.Contracts;

public sealed record LicenseResponse(
    ServerLicenseResponse Response,
    LicensePlan[] Plans
    );


public sealed record ServerLicenseResponse(
    string ErrorMessage,
    string Expiration,
    string SubscriptionExpiration,
    bool Expired,
    string FirstServerStartDate,
    string Id,
    string LicensedTo,
    string Status,
    string Type,
    string Version
    );

public sealed record LicensePlan(
    string Slug,
    string Name,
    string Tagline,
    string PriceLabel,
    string PriceSuffix,
    bool Featured,
    string[] Features);
