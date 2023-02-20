using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Server.Routing;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.ServerWide
{
    public static class DebugInfoPackageUtils
    {
        public static readonly IReadOnlyList<RouteInformation> Routes = RouteScanner.DebugRoutes;

        public static string GetOutputPathFromRouteInformation(RouteInformation route, string prefix, string extension) => GetOutputPathFromRouteInformation(route.Path, prefix, extension);

        public static string GetOutputPathFromRouteInformation(string path, string prefix, string extension)
        {
            if (path.StartsWith("/debug/"))
                path = path.Replace("/debug/", string.Empty);
            else if (path.StartsWith("debug/"))
                path = path.Replace("debug/", string.Empty);

            path = path.Replace("/databases/*/", string.Empty)
                .Replace("debug/", string.Empty) //if debug/ left in the middle, remove it as well
                .Replace("/", ".");

            if (path.StartsWith("."))
                path = path.Substring(1);

            path = string.IsNullOrWhiteSpace(prefix) == false ?
                $"{prefix}/{path}" : // .ZIP File Format Specification 4.4.17 file name: (Variable)
                path;

            path = string.IsNullOrWhiteSpace(extension) ? path : $"{path}.{extension}";

            return path;
        }

        public static IEnumerable<RouteInformation> GetAuthorizedRoutes(RavenServer server, HttpContext httpContext, string databaseName = null)
        {
            var routes = Routes.Where(x => server._forTestingPurposes == null || server._forTestingPurposes.DebugPackage.RoutesToSkip.Contains(x.Path) == false);
            var feature = (RavenServer.AuthenticateConnection)httpContext.Features.Get<IHttpAuthenticationFeature>();

            foreach (var route in routes)
            {
                if (server.Certificate.Certificate != null)
                {
                    if (server.Router.CanAccessRoute(route, httpContext, databaseName, feature))
                    {
                        yield return route;
                    }
                }
                else
                {
                    yield return route;
                }
            }
        }

        public static async Task WriteExceptionAsZipEntryAsync(Exception e, ZipArchive archive, string entryName)
        {
            var entry = archive.CreateEntry($"{entryName}.error");
            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

            await using (var entryStream = entry.Open())
            await using (var sw = new StreamWriter(entryStream))
            {
                await sw.WriteAsync($"Exception throw time: UTC {DateTime.UtcNow:yyyy-MM-dd H:mm:ss}");
                await sw.WriteLineAsync();
                await sw.WriteAsync(e.ToString());
                await sw.FlushAsync();
            }
        }

        public static async Task WriteDebugInfoTimesAsZipEntryAsync(Dictionary<string, TimeSpan> debugInfoTimeSpans, ZipArchive archive, string prefix)
        {
            var entryName = GetOutputPathFromRouteInformation("requestTimes", prefix, "txt");
            var entry = archive.CreateEntry(entryName);
            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

            var sorted = debugInfoTimeSpans.OrderByDescending(o => o.Value);
            await using (var entryStream = entry.Open())
            await using (var sw = new StreamWriter(entryStream))
            {
                foreach (var timeInfo in sorted)
                {
                    await sw.WriteLineAsync(timeInfo.Value + ", " + timeInfo.Key);
                }
                await sw.FlushAsync();
            }
        }
    }
}
