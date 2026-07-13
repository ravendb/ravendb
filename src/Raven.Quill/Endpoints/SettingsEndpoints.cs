using Raven.Quill.Contracts;
using Raven.Quill.Licensing;

namespace Raven.Quill.Endpoints;

public static class SettingsEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/settings").WithTags("settings").RequireAuthorization();

        group.MapGet("/license", async (ILicenseStatsProvider provider, CancellationToken token) =>
                Results.Ok(await provider.GetLicenseAsync(token)))
            .WithName("settings.license")
            .Produces<LicenseResponse>();

        group.MapGet("/usage", async (int? year, int? month, ILicenseStatsProvider provider, CancellationToken token) => 
                Results.Ok(await provider.GetUsageAsync(year, month, token)))
            .WithName("settings.usage")
            .Produces<QuillUsageResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest);
    }
}
