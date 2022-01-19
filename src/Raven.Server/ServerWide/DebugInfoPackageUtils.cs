using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Server.Routing;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.ServerWide
{
    public static class DebugInfoPackageUtils
    {
        public static readonly IReadOnlyList<RouteInformation> Routes = RouteScanner.DebugRoutes;


        public static string GetOutputPathFromRouteInformation(RouteInformation route, string prefix) => GetOutputPathFromRouteInformation(route.Path, prefix);

        public static string GetOutputPathFromRouteInformation(string path, string prefix)
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
                $"{prefix}/{path}.json" : // .ZIP File Format Specification 4.4.17 file name: (Variable)
                $"{path}.json";

            return path;
        }

        public static IEnumerable<RouteInformation> GetAuthorizedRoutes(RavenServer server, HttpContext httpContext, string databaseName = null)
        {
            var routes = Routes.Where(x => server._forTestingPurposes == null || server._forTestingPurposes.DebugPackage.RoutesToSkip.Contains(x.Path) == false);

            if (server.Certificate.Certificate != null)
            {
                var feature = httpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
                Debug.Assert(feature != null);
                routes = routes.Where(route => server.Router.CanAccessRoute(route, httpContext, databaseName, feature, out _));
            }

            return routes;
        }

        public static void WriteExceptionAsZipEntry(Exception e, ZipArchive archive, string entryName)
        {
            var entry = archive.CreateEntry($"{entryName}.error");
            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

            using (var entryStream = entry.Open())
            using (var sw = new StreamWriter(entryStream))
            {
                sw.Write(e);
                sw.Flush();
            }
        }
    }
}
