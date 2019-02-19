using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
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
                Path.Combine(prefix, $"{path}.json") :
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

    }
}
