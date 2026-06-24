using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Raven.AiAppliance.Contracts;
using Raven.AiAppliance.Licensing;

namespace Raven.AiAppliance.Endpoints;

/// <summary>
/// Settings surfaces — the prototype's <c>/settings/license</c> and
/// <c>/settings/usage</c> pages. License + monthly-writes are MOCK-backed via
/// <see cref="ILicenseStatsProvider"/> (real license API: RavenDB-26661/26783).
/// Per-app token totals stay on <c>/api/usage/by-app</c> (already real).
/// </summary>
public static class SettingsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/settings").WithTags("settings").RequireAuthorization();

        group.MapGet("/license", (string? demoState, ILicenseStatsProvider provider) =>
                Results.Ok(provider.GetLicense(demoState)))
            .WithName("settings.license")
            .Produces<LicenseResponse>();

        group.MapGet("/usage", (int? year, int? month, ILicenseStatsProvider provider) =>
            {
                var now = DateTime.UtcNow;
                var y = year ?? now.Year;
                var m = month ?? now.Month;
                if (m is < 1 or > 12 || y is < 1 or > 9999)
                    return Results.BadRequest(new ApiErrorResponse("year must be 1-9999 and month 1-12"));
                return Results.Ok(provider.GetMonthlyWrites(y, m, now));
            })
            .WithName("settings.usage")
            .Produces<MonthlyWritesResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
    }
}
