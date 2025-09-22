using System;
using System.Collections.Generic;
using System.Timers;
using Raven.Client.Util;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public static class DebugPackageReportsContainer
{
    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<DebugPackageReport>();

    private static readonly TimeSpan TimerInterval = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan InactivityThresholdMs = TimeSpan.FromMinutes(30);

    private static readonly object Lock = new();

    private static readonly Dictionary<string, DebugPackageReport> DebugPackageReports = new();

    private static Timer PackageCleanupTimer;

    public static bool TryAdd(string packageId, DebugPackageReport report)
    {
        lock (Lock)
        {
            var result = DebugPackageReports.TryAdd(packageId, report);

            if (result)
                report.LastAccessTime = SystemTime.UtcNow;

            if (DebugPackageReports.Count == 1 && PackageCleanupTimer == null)
            {
                PackageCleanupTimer = new Timer(TimerInterval);
                PackageCleanupTimer.Elapsed += CleanupPackages;
                PackageCleanupTimer.AutoReset = true;
                PackageCleanupTimer.Start();
            }

            return result;
        }
    }

    public static bool TryGet(string packageId, out DebugPackageReport report)
    {
        lock (Lock)
        {
            var result = DebugPackageReports.TryGetValue(packageId, out report);

            if (result)
                report.LastAccessTime = SystemTime.UtcNow;

            return result;
        }
    }

    private static void CleanupPackages(object sender, ElapsedEventArgs e)
    {
        try
        {
            lock (Lock)
            {
                var now = SystemTime.UtcNow;
                var packagesToRemove = new List<string>();

                foreach (var report in DebugPackageReports)
                {
                    var timeSinceLastAccess = now - report.Value.LastAccessTime;

                    if (timeSinceLastAccess >= InactivityThresholdMs)
                        packagesToRemove.Add(report.Key);
                }

                foreach (var packageId in packagesToRemove)
                {
                    DebugPackageReports.Remove(packageId);
                }

                if (DebugPackageReports.Count == 0)
                {
                    PackageCleanupTimer.Dispose();
                    PackageCleanupTimer = null;
                }
            }
        }
        catch (Exception exception)
        {
            if (Logger.IsErrorEnabled)
                Logger.Error("Failed to cleanup Debug Package analysis reports", exception);
        }
    }

    public static bool TryRemove(string packageId)
    {
        lock (Lock)
        {
            return DebugPackageReports.Remove(packageId);
        }
    }
}
