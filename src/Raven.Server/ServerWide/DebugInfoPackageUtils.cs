using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
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

        public static bool CanAccessRoute(RavenServer.AuthenticateConnection authenticateConnection, RouteInformation route, string db = null)
        {
            switch (authenticateConnection.Status)
            {
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                    return true;
                case RavenServer.AuthenticationStatus.Operator:
                    return (route.AuthorizationStatus != AuthorizationStatus.ClusterAdmin);
                case RavenServer.AuthenticationStatus.Allowed:
                    if (route.AuthorizationStatus == AuthorizationStatus.ClusterAdmin || route.AuthorizationStatus == AuthorizationStatus.Operator)
                        return false;
                    if (route.TypeOfRoute == RouteInformation.RouteType.Databases
                        && (db == null || authenticateConnection.CanAccess(db, route.AuthorizationStatus == AuthorizationStatus.DatabaseAdmin) == false))
                        return false;
                    return true;
                default:
                    if (route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients)
                        return true;
                    return false;
            }
        }
    }
}
