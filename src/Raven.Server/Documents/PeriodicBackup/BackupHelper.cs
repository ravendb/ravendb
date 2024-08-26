using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server.Utils;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.PeriodicBackup
{
    public static class BackupHelper
    {
        public static void AssertFreeSpaceForSnapshot(string directoryPath, long sizeInBytes, string action, RavenLogger logger)
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

        public static bool BackupTypeChanged(PeriodicBackupConfiguration previous, PeriodicBackupConfiguration current)
        {
            return previous.BackupType != current.BackupType;
        }

        public static async Task<long> RunWithRetriesAsync(
            int maxRetries,
            Func<Task<long>> action,
            string infoMessage,
            string errorMessage,
            SmugglerResult smugglerResult,
            Action<IOperationProgress> onProgress = default,
            OperationCancelToken operationCancelToken = default)
        {
            var retries = 0;

            while (true)
            {
                try
                {
                    if (retries > 0)
                        operationCancelToken?.Token.ThrowIfCancellationRequested();

                    smugglerResult?.AddInfo(infoMessage);
                    onProgress?.Invoke(smugglerResult?.Progress);

                    return await action();
                }
                catch (TimeoutException)
                {
                    if (++retries < maxRetries)
                    {
                        await Task.Delay(1_000);
                        continue;
                    }

                    smugglerResult?.AddError(errorMessage);
                    onProgress?.Invoke(smugglerResult?.Progress);
                    throw;
                }
            }
        }
    }
}
