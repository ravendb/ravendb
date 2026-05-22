using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Raven.AiAppliance.Endpoints;

public static class StaticAssetEndpoints
{
    /// Serves the Vite-built React UI from wwwroot/. The Docker image replaces
    /// the checked-in placeholder wwwroot with Raven.AiAppliance.Web/dist.
    public static void Map(WebApplication app)
    {
        app.UseDefaultFiles(new DefaultFilesOptions
        {
            DefaultFileNames = new List<string> { "index.html" },
        });

        app.UseStaticFiles();
    }

    public static void MapSpaFallback(WebApplication app)
    {
        app.MapFallback("{*path:nonfile}", async context =>
        {
            var path = context.Request.Path;
            if (path.StartsWithSegments("/api") || path.StartsWithSegments("/healthz"))
            {
                context.Response.StatusCode = StatusCodes.Status404NotFound;
                return;
            }

            var indexFile = app.Environment.WebRootFileProvider.GetFileInfo("index.html");
            if (!indexFile.Exists)
            {
                context.Response.StatusCode = StatusCodes.Status404NotFound;
                return;
            }

            context.Response.ContentType = "text/html";
            await context.Response.SendFileAsync(indexFile, context.RequestAborted);
        });
    }
}
