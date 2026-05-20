using Microsoft.AspNetCore.Builder;

namespace Raven.AiAppliance.Endpoints;

public static class StaticAssetEndpoints
{
    /// Serves the static demo chat UI from wwwroot/. We deliberately pin the
    /// classic UseDefaultFiles + UseStaticFiles middleware rather than .NET 10's
    /// MapStaticAssets pipeline, which serves *fingerprinted* assets
    /// (e.g. chat.7wqpx38jhl.css) — wrong for plain HTML that references
    /// "/chat.css" literally. Once T-2's Next.js export takes over, the same
    /// pattern still works.
    ///
    /// We don't pass a custom FileProvider — the framework's default
    /// WebRootFileProvider resolves to the project content root's wwwroot/ in
    /// dev (`dotnet run` / VS F5) and the publish output's wwwroot/ in
    /// Docker / `dotnet publish` deployments. Both layouts work.
    public static void Map(WebApplication app)
    {
        app.UseDefaultFiles(new DefaultFilesOptions
        {
            DefaultFileNames = new List<string> { "index.html" },
        });

        app.UseStaticFiles();
    }
}
