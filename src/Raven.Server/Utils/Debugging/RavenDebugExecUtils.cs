using System;
using System.IO;
using Sparrow.Platform;

namespace Raven.Server.Utils.Debugging;

public static class RavenDebugExecUtils
{
    internal static string RavenDebugFileName = PlatformDetails.RunningOnPosix ? "Raven.Debug" : "Raven.Debug.exe";

    internal static readonly string[] FileSystemLookupPaths =
    {
        ".",
        "../../../../../tools/Raven.Debug/bin/Debug/net8.0",
        "../../../../../tools/Raven.Debug/bin/Release/net8.0"
    };

    public static string GetRavenDebugExecPath()
    {
        foreach (var lookupPath in FileSystemLookupPaths)
        {
            var candidate = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, lookupPath, RavenDebugFileName));
            if (File.Exists(candidate))
                return candidate;
        }

        throw new FileNotFoundException($"Could not find '{RavenDebugFileName}' in any of the lookup paths relative to '{AppContext.BaseDirectory}'");
    }
}
