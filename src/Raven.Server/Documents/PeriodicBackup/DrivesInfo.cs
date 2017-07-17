using System;
using System.Collections.Generic;
using System.IO;
using Raven.Server.Config;
using Voron.Platform.Win32;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class DrivesInfo
    {
        public DrivesInfo()
        {
            AllDriveNames = new HashSet<string>();
            DatabaseDriveNames = new HashSet<string>();
        }

        public HashSet<string> AllDriveNames { get; set; }

        public HashSet<string> DatabaseDriveNames { get; set; }

        public static void GetDrivesInfoForWindows(RavenConfiguration ravenConfiguration, DrivesInfo drivesInfo)
        {
            var dataDirectoryDriveName = Path.GetPathRoot(ravenConfiguration.Core.DataDirectory.FullPath);
            var inUseDrives = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                dataDirectoryDriveName,
                ravenConfiguration.Storage.JournalsStoragePath != null
                    ? Path.GetPathRoot(ravenConfiguration.Storage.JournalsStoragePath.FullPath)
                    : dataDirectoryDriveName,
                Path.GetPathRoot(ravenConfiguration.Indexing.StoragePath.FullPath)
            };

            var usedPhysicalDriveIds = new HashSet<uint>();
            var driveNameToDriveId = new Dictionary<string, uint>();
            var drives = DriveInfo.GetDrives();
            foreach (var driveInfo in drives)
            {
                var driveName = driveInfo.RootDirectory.FullName.ToLower();
                drivesInfo.AllDriveNames.Add(driveName);
                if (driveInfo.DriveType != DriveType.Fixed)
                    continue;

                var physicalDriveId = WindowsMemoryMapPager.GetPhysicalDriveId(driveName.TrimEnd('\\'));
                if (inUseDrives.Contains(driveName))
                {
                    usedPhysicalDriveIds.Add(physicalDriveId);
                }

                driveNameToDriveId[driveName] = physicalDriveId;
            }

            foreach (var keyValue in driveNameToDriveId)
            {
                if (usedPhysicalDriveIds.Contains(keyValue.Value) == false)
                    continue;

                drivesInfo.DatabaseDriveNames.Add(keyValue.Key);
            }
        }
    }
}