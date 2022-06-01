using System;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup
{
    public static class BackupHelper
    {
        public static void AssertFreeSpaceForSnapshot(string directoryPath, long sizeInBytes, string action, Logger logger)
        {
            var destinationDriveInfo = DiskUtils.GetDiskSpaceInfo(directoryPath);
            if (destinationDriveInfo == null)
            {
                if (logger.IsInfoEnabled)
                    logger.Info($"Couldn't find the disk space info for path: {directoryPath}");

                return;
            }

            var desiredFreeSpace = Size.Min(new Size(512, SizeUnit.Megabytes), destinationDriveInfo.TotalSize * 0.01) + new Size(sizeInBytes, SizeUnit.Bytes);
            if (destinationDriveInfo.TotalFreeSpace < desiredFreeSpace)
                throw new InvalidOperationException($"Not enough free space to {action}. " +
                                                    $"Required space {desiredFreeSpace}, available space: {destinationDriveInfo.TotalFreeSpace}");
        }
    }
}
