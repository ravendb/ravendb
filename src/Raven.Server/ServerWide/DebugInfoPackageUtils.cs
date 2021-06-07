using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Sparrow.Server.Platform.Posix;

namespace Raven.Server.ServerWide
{
    public static class DebugInfoPackageUtils
    {
        public static readonly IReadOnlyList<RouteInformation> Routes = RouteScanner.DebugRoutes;

        public static string GetOutputPathFromRouteInformation(RouteInformation route, string prefix)
        {
            var path = route.Path;
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

        public static async Task WriteExceptionAsZipEntryAsync(Exception e, ZipArchive archive, string entryName)
        {
            var entry = archive.CreateEntry($"{entryName}.error");
            entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

            await using (var entryStream = entry.Open())
            await using (var sw = new StreamWriter(entryStream))
            {
                await sw.WriteAsync(e.ToString());
                await sw.FlushAsync();
            }
        }

        public static IEnumerable<RouteInformation> GetAuthorizedRoutes(RavenServer.AuthenticateConnection authenticateConnection, string db = null)
        {
            return Routes.Where(route =>
            {
                bool authorized = false;
                switch (authenticateConnection.Status)
                {
                    case RavenServer.AuthenticationStatus.ClusterAdmin:
                        authorized = true;
                        break;

                    case RavenServer.AuthenticationStatus.Operator:
                        if (route.AuthorizationStatus != AuthorizationStatus.ClusterAdmin)
                            authorized = true;
                        break;

                    case RavenServer.AuthenticationStatus.Allowed:
                        if (route.AuthorizationStatus == AuthorizationStatus.ClusterAdmin || route.AuthorizationStatus == AuthorizationStatus.Operator)
                            break;
                        if (route.TypeOfRoute == RouteInformation.RouteType.Databases
                            && (db == null || authenticateConnection.CanAccess(db, requireAdmin: route.AuthorizationStatus == AuthorizationStatus.DatabaseAdmin, requireWrite: route.EndpointType == EndpointType.Write) == false))
                            break;
                        authorized = true;
                        break;

                    default:
                        if (route.AuthorizationStatus == AuthorizationStatus.UnauthenticatedClients)
                            authorized = true;
                        break;
                }

                return authorized;
            });
        }
    }
}
