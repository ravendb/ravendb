using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Net.Http.Headers;

namespace Raven.Quill.Endpoints;

public static class StaticAssetEndpoints
{
    private const string WidgetAssetsPrefix = "/widget/assets/";
    private const string WidgetIndexPath = "/widget/index.html";

    public static void Map(WebApplication app)
    {
        app.UseDefaultFiles(new DefaultFilesOptions
        {
            DefaultFileNames = new List<string> { "index.html" },
        });

        app.UseStaticFiles(new StaticFileOptions { OnPrepareResponse = SetWidgetCacheHeaders });
    }

    /// The widget's assets are content-hashed, so they can be cached forever; the entry document that
    /// references them cannot be, or a rebuild would keep serving the previous bundle's URLs.
    private static void SetWidgetCacheHeaders(StaticFileResponseContext context)
    {
        var path = context.Context.Request.Path;
        if (path.StartsWithSegments("/widget") == false)
            return;

        var value = path.Value ?? "";
        if (value.StartsWith(WidgetAssetsPrefix, StringComparison.OrdinalIgnoreCase))
            context.Context.Response.Headers[HeaderNames.CacheControl] = "public, max-age=31536000, immutable";
        else if (value.Equals(WidgetIndexPath, StringComparison.OrdinalIgnoreCase))
            context.Context.Response.Headers[HeaderNames.CacheControl] = "no-cache";
    }

    public static void MapSpaFallback(WebApplication app)
    {
        app.MapFallback("{*path:nonfile}", async context =>
        {
            var path = context.Request.Path;
            // /widget is the embeddable bundle, not the dashboard SPA: a missing asset there has to stay a
            // 404 rather than resolving to the dashboard's index.html.
            if (path.StartsWithSegments("/api") || path.StartsWithSegments("/healthz") ||
                path.StartsWithSegments("/embed") || path.StartsWithSegments("/widget") || IsAppsEmbedSubPath(path))
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
