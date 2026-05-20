using Microsoft.AspNetCore.Builder;

namespace Raven.AiAppliance.Endpoints;

public static class HealthEndpoints
{
    public static void Map(WebApplication app)
    {
        app.MapHealthChecks("/healthz");
    }
}
