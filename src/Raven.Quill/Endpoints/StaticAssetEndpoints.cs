using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Raven.Quill.Endpoints;

public static class StaticAssetEndpoints
{
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
            if (path.StartsWithSegments("/api") || path.StartsWithSegments("/healthz") ||
                path.StartsWithSegments("/embed") || IsAppsEmbedSubPath(path))
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

    private static bool IsAppsEmbedSubPath(PathString path)
    {
        var segments = path.Value?.Split('/', StringSplitOptions.RemoveEmptyEntries);
        return segments is { Length: >= 3 } &&
               string.Equals(segments[0], "apps", StringComparison.OrdinalIgnoreCase) &&
               string.Equals(segments[2], "embed", StringComparison.OrdinalIgnoreCase);
    }
}
