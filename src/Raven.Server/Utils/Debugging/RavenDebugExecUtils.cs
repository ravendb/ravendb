using System;
using System.IO;
using Sparrow.Platform;

namespace Raven.Server.Utils.Debugging;

public static class RavenDebugExecUtils
{
    public static string GetRavenDebugExecPath()
    {
        var ravenDebugFileName = PlatformDetails.RunningOnPosix ? "Raven.Debug" : "Raven.Debug.exe";
        var ravenDebugExec = Path.Combine(AppContext.BaseDirectory, ravenDebugFileName);

        if (File.Exists(ravenDebugExec))
            return ravenDebugExec;

        // In a dev environment, Raven.Debug is not in the same output directory as Raven.Server.
        // Raven.Server runs from: src/Raven.Server/bin/{config}/net8.0
        // Raven.Debug is built to: tools/Raven.Debug/bin/{config}/net8.0
        // In dev, AppContext.BaseDirectory is: <root>/src/Raven.Server/bin/{config}/{tfm}/
        // We need to go up 5 levels to reach the repo root, then look in: tools/Raven.Debug/bin/{config}/{tfm}/
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        var tfm = baseDir.Name;                   // e.g. "net8.0"
        var config = baseDir.Parent?.Name;        // e.g. "Debug" or "Release"
        var repoRoot = baseDir.Parent?.Parent?.Parent?.Parent?.Parent; // {config} -> bin -> Raven.Server -> src -> root

        if (repoRoot != null && config != null)
        {
            // Try same configuration first, then the opposite one
            var configs = new[] { config, string.Equals(config, "Debug", StringComparison.OrdinalIgnoreCase) ? "Release" : "Debug" };
            foreach (var cfg in configs)
            {
                var candidate = Path.Combine(repoRoot.FullName, "tools", "Raven.Debug", "bin", cfg, tfm, ravenDebugFileName);
                if (File.Exists(candidate))
                    return candidate;
            }
        }

        throw new FileNotFoundException($"Could not find debugger tool at '{ravenDebugExec}'");
    }
}
