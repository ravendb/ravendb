// -----------------------------------------------------------------------
//  <copyright file="CheckFreeDiskSpace.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Server;

namespace Raven.Database.Plugins.Builtins
{
    public class CheckFreeDiskSpace : IServerStartupTask
    {
        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        [return: MarshalAs(UnmanagedType.Bool)]
        static extern bool GetDiskFreeSpaceEx(string lpDirectoryName,
           out ulong lpFreeBytesAvailable,
           out ulong lpTotalNumberOfBytes,
           out ulong lpTotalNumberOfFreeBytes);

        private RavenDBOptions options;

        private Timer timer = null;

        const double FreeThreshold = 0.15;

        public void Execute(RavenDBOptions serverOptions)
        {
            options = serverOptions;
            timer = options.SystemDatabase.TimerManager.NewTimer(ExecuteCheck, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(5));
        }

        private void ExecuteCheck(object state)
        {
            if (options.Disposed)
            {
                Dispose();
                return;
            }

            var pathsToCheck = new HashSet<PathToCheck>();

            options.DatabaseLandlord.ForAllDatabases(database =>
            {
                pathsToCheck.Add(new PathToCheck { Path = database.Configuration.IndexStoragePath, PathType = PathType.Index, ResourceName = database.Name, ResourceType = ResourceType.Database });
                pathsToCheck.Add(new PathToCheck { Path = database.Configuration.Storage.Esent.JournalsStoragePath, PathType = PathType.Journal, ResourceName = database.Name, ResourceType = ResourceType.Database });
                pathsToCheck.Add(new PathToCheck { Path = database.Configuration.Storage.Voron.JournalsStoragePath, PathType = PathType.Journal, ResourceName = database.Name, ResourceType = ResourceType.Database });
                pathsToCheck.Add(new PathToCheck { Path = database.Configuration.DataDirectory, PathType = PathType.Data, ResourceName = database.Name, ResourceType = ResourceType.Database });
            });

            options.FileSystemLandlord.ForAllFileSystems(filesystem =>
            {
                pathsToCheck.Add(new PathToCheck { Path = filesystem.Configuration.FileSystem.DataDirectory, PathType = PathType.Data, ResourceName = filesystem.Name, ResourceType = ResourceType.FileSystem });
                pathsToCheck.Add(new PathToCheck { Path = filesystem.Configuration.FileSystem.IndexStoragePath, PathType = PathType.Index, ResourceName = filesystem.Name, ResourceType = ResourceType.FileSystem });
                pathsToCheck.Add(new PathToCheck { Path = filesystem.Configuration.Storage.Esent.JournalsStoragePath, PathType = PathType.Journal, ResourceName = filesystem.Name, ResourceType = ResourceType.FileSystem });
                pathsToCheck.Add(new PathToCheck { Path = filesystem.Configuration.Storage.Voron.JournalsStoragePath, PathType = PathType.Journal, ResourceName = filesystem.Name, ResourceType = ResourceType.FileSystem });
            });

            options.CountersLandlord.ForAllCounters(cs =>
            {
                pathsToCheck.Add(new PathToCheck { Path = cs.Configuration.Counter.DataDirectory, PathType = PathType.Data, ResourceName = cs.Name, ResourceType = ResourceType.Counters });
                pathsToCheck.Add(new PathToCheck { Path = cs.Configuration.Storage.Esent.JournalsStoragePath, PathType = PathType.Journal, ResourceName = cs.Name, ResourceType = ResourceType.Counters });
                pathsToCheck.Add(new PathToCheck { Path = cs.Configuration.Storage.Voron.JournalsStoragePath, PathType = PathType.Journal, ResourceName = cs.Name, ResourceType = ResourceType.Counters });
                pathsToCheck.Add(new PathToCheck { Path = cs.Configuration.DataDirectory, PathType = PathType.Data, ResourceName = cs.Name, ResourceType = ResourceType.Counters });
            });

            options.TimeSeriesLandlord.ForAllTimeSeries(ts =>
            {
                pathsToCheck.Add(new PathToCheck { Path = ts.Configuration.TimeSeries.DataDirectory, PathType = PathType.Data, ResourceName = ts.Name, ResourceType = ResourceType.TimeSeries});
                pathsToCheck.Add(new PathToCheck { Path = ts.Configuration.Storage.Esent.JournalsStoragePath, PathType = PathType.Journal, ResourceName = ts.Name, ResourceType = ResourceType.TimeSeries });
                pathsToCheck.Add(new PathToCheck { Path = ts.Configuration.Storage.Voron.JournalsStoragePath, PathType = PathType.Journal, ResourceName = ts.Name, ResourceType = ResourceType.TimeSeries });
                pathsToCheck.Add(new PathToCheck { Path = ts.Configuration.DataDirectory, PathType = PathType.Data, ResourceName = ts.Name, ResourceType = ResourceType.TimeSeries });
            });

            var roots = new List<PathToCheck>();
            var unc = new List<PathToCheck>();
            foreach (var pathToCheck in pathsToCheck.Where(pathToCheck => pathToCheck.Path != null))
            {
                if (Path.IsPathRooted(pathToCheck.Path) && pathToCheck.Path.StartsWith("\\\\") == false)
                {
                    pathToCheck.Path = Path.GetPathRoot(pathToCheck.Path);
                    roots.Add(pathToCheck);
                    continue;
                }

                if (pathToCheck.Path.StartsWith("\\\\"))
                {
                    pathToCheck.Path = Path.GetPathRoot(pathToCheck.Path);
                    unc.Add(pathToCheck);
                }
            }

            var groupedRoots = roots
                .GroupBy(x => x.Path)
                .ToList();

            var groupedUncRoots = unc
                .GroupBy(x => x.Path)
                .ToList();

            var lacksFreeSpace = new List<string>();
            var drives = DriveInfo.GetDrives();

            foreach (var drive in drives)
            {
                var group = groupedRoots.FirstOrDefault(x => string.Equals(x.Key, drive.Name, StringComparison.OrdinalIgnoreCase));
                if (group == null)
                    continue;

                var freeSpaceInPercentage = drive.TotalFreeSpace * 1.0 / drive.TotalSize;
                if (freeSpaceInPercentage < FreeThreshold)
                    lacksFreeSpace.Add(drive.Name);

                group.ForEach(x =>
                {
                    x.FreeSpaceInPercentage = freeSpaceInPercentage;
                    x.FreeSpaceInBytes = drive.TotalFreeSpace;
                });
            }

            foreach (var group in groupedUncRoots)
            {
                var result = DiskSpaceChecker.GetFreeDiskSpace(group.Key, drives);
                if (result == null)
                    continue;

                var freeSpaceInPercentage = result.TotalFreeSpaceInBytes * 1.0 / result.TotalSize;
                if (freeSpaceInPercentage < FreeThreshold)
                    lacksFreeSpace.Add(group.Key);

                group.ForEach(x =>
                {
                    x.FreeSpaceInPercentage = freeSpaceInPercentage;
                    x.FreeSpaceInBytes = result.TotalFreeSpaceInBytes;
                });
            }

            if (lacksFreeSpace.Any())
            {
                options.SystemDatabase.AddAlert(new Alert
                {
                    AlertLevel = AlertLevel.Warning,
                    CreatedAt = SystemTime.UtcNow,
                    Title = string.Format("Database disk{0} ({1}) has less than {2}% free space.", lacksFreeSpace.Count() > 1 ? "s" : string.Empty, string.Join(", ", lacksFreeSpace), (int)(FreeThreshold * 100)),
                    UniqueKey = "Free space"
                });
            }

            options.DatabaseLandlord.ForAllDatabases(database =>
            {
                foreach (var path in pathsToCheck.Where(x => x.FreeSpaceInPercentage.HasValue && x.FreeSpaceInBytes.HasValue && x.ResourceType == ResourceType.Database && x.ResourceName == database.Name))
                {
                    database.OnDiskSpaceChanged(new DiskSpaceNotification(path.Path, path.PathType, path.FreeSpaceInBytes.Value, path.FreeSpaceInPercentage.Value));
                }
            });
        }

        public void Dispose()
        {
            var copy = timer;
            if (copy != null)
            {
                copy.Dispose();
                timer = null;
            }
        }

        private class PathToCheck
        {
            public string Path { get; set; }

            public PathType PathType { get; set; }

            public string ResourceName { get; set; }

            public ResourceType ResourceType { get; set; }

            public double? FreeSpaceInBytes { get; set; }

            public double? FreeSpaceInPercentage { get; set; }
        }

        private enum ResourceType
        {
            Database,
            FileSystem,
            Counters,
            TimeSeries
        }

        public static class DiskSpaceChecker
        {
            public static DiskSpaceResult GetFreeDiskSpace(string pathToCheck, DriveInfo[] driveInfo)
            {
                if (string.IsNullOrEmpty(pathToCheck))
                    return null;

                if (Path.IsPathRooted(pathToCheck) && pathToCheck.StartsWith("\\\\") == false)
                {
                    var root = Path.GetPathRoot(pathToCheck);

                    foreach (var drive in driveInfo)
                    {
                        if (root.Contains(drive.Name) == false)
                            continue;

                        return new DiskSpaceResult
                        {
                            DriveName = root,
                            TotalFreeSpaceInBytes = drive.TotalFreeSpace,
                            TotalSize = drive.TotalSize
                        };
                    }

                    return null;
                }

                if (pathToCheck.StartsWith("\\\\"))
                {
                    var uncRoot = Path.GetPathRoot(pathToCheck);

                    ulong freeBytesAvailable;
                    ulong totalNumberOfBytes;
                    ulong totalNumberOfFreeBytes;
                    var success = GetDiskFreeSpaceEx(uncRoot, out freeBytesAvailable, out totalNumberOfBytes, out totalNumberOfFreeBytes);

                    if (success == false)
                        return null;

                    return new DiskSpaceResult
                    {
                        DriveName = uncRoot,
                        TotalFreeSpaceInBytes = (long)freeBytesAvailable,
                        TotalSize = (long)totalNumberOfBytes
                    };
                }

                return null;
            }

            public class DiskSpaceResult
            {
                public string DriveName { get; set; }

                public long TotalFreeSpaceInBytes { get; set; }

                public long TotalSize { get; set; }
            }
        }
    }
}
