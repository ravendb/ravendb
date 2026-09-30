using Microsoft.AspNetCore.Builder;

namespace Raven.Quill.Endpoints;

public static class HealthEndpoints
{
    public static void Map(WebApplication app)
    {
        app.MapHealthChecks("/healthz");
    }
}
