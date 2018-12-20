using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Storage
{
    public class StorageSpaceMonitor : IDisposable
    {
        private static readonly TimeSpan CheckFrequency = TimeSpan.FromMinutes(10);

        private readonly Logger _logger = LoggingSource.Instance.GetLogger<StorageSpaceMonitor>(nameof(StorageSpaceMonitor));
        private readonly LinkedList<DocumentDatabase> _databases = new LinkedList<DocumentDatabase>();

        private readonly object _runLock = new object();
        private readonly object _subscribeLock = new object();

        private readonly NotificationCenter.NotificationCenter _notificationCenter;

        private Timer _timer;

        internal bool SimulateLowDiskSpace;

        public StorageSpaceMonitor(NotificationCenter.NotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter;

            _timer = new Timer(Run, null, CheckFrequency, CheckFrequency);
        }

        public void Subscribe(DocumentDatabase database)
        {
            if (database.Configuration.Core.RunInMemory)
                return;

            lock (_subscribeLock)
                _databases.AddLast(database);
        }

        public void Unsubscribe(DocumentDatabase database)
        {
            if (database.Configuration.Core.RunInMemory)
                return;

            lock (_subscribeLock)
                _databases.Remove(database);
        }

        internal void Run(object state)
        {
            if (_notificationCenter.IsInitialized == false)
                return;

            if (Monitor.TryEnter(_runLock) == false)
                return;

            try
            {
                var environmentsRunningOnLowSpaceDisks = GetLowSpaceDisksAndRelevantEnvironments().Environments;

                // let's try to cleanup first and maybe recover from low disk space issue

                foreach (var storageEnvironment in environmentsRunningOnLowSpaceDisks)
                {
                    try
                    {
                        storageEnvironment.Cleanup(tryCleanupRecycledJournals: true);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                            _logger.Operations($"Failed to cleanup storage environment after detecting low disk space. Environment: {storageEnvironment}", e);
                    }
                }

                var lowSpaceDisks = GetLowSpaceDisksAndRelevantEnvironments().Disks;

                if (lowSpaceDisks.Count == 0)
                    return;

                var message =
                    $"The following {(PlatformDetails.RunningOnPosix ? "mount points" : "disks")} are running out of space:{Environment.NewLine} {string.Join(Environment.NewLine, lowSpaceDisks)}";

                _notificationCenter.Add(AlertRaised.Create(null, "Low free disk space", message, AlertType.LowDiskSpace, NotificationSeverity.Warning,
                    "low-disk-space"));

                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message);
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Failed to run storage space monitor", e);
            }
            finally
            {
                Monitor.Exit(_runLock);
            }
        }

        private (HashSet<LowDiskSpace> Disks, HashSet<StorageEnvironment> Environments) GetLowSpaceDisksAndRelevantEnvironments()
        {
            var lowSpaceDisks = new HashSet<LowDiskSpace>();
            var environmentsRunningOnLowSpaceDisks = new HashSet<StorageEnvironment>();

            var current = _databases.First;

            while (current != null)
            {
                var database = current.Value;
                var storageConfig = database.Configuration.Storage;

                try
                {
                    foreach (var item in database.GetAllStoragesEnvironment())
                    {
                        var driveInfo = item.Environment.Options.DriveInfoByPath?.Value;

                        var options = (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)item.Environment.Options;

                        var dataDisk = DiskSpaceChecker.GetDiskSpaceInfo(options.BasePath.FullPath, driveInfo?.BasePath);

                        AddEnvironmentIfLowSpace(dataDisk);

                        if (options.JournalPath != null)
                        {
                            var journalDisk = DiskSpaceChecker.GetDiskSpaceInfo(options.JournalPath.FullPath, driveInfo?.JournalPath);

                            if (dataDisk?.DriveName != journalDisk?.DriveName)
                                AddEnvironmentIfLowSpace(journalDisk);
                        }

                        if (options.TempPath != null)
                        {
                            var tempDisk = DiskSpaceChecker.GetDiskSpaceInfo(options.TempPath.FullPath, driveInfo?.TempPath);

                            if (dataDisk?.DriveName != tempDisk?.DriveName)
                                AddEnvironmentIfLowSpace(tempDisk);
                        }

                        void AddEnvironmentIfLowSpace(DiskSpaceResult diskSpace)
                        {
                            if (diskSpace == null)
                                return;

                            if (IsLowSpace(diskSpace.TotalFreeSpace, diskSpace.TotalSize, storageConfig, out var reason, SimulateLowDiskSpace) == false)
                                return;

                            var lowSpace = new LowDiskSpace(diskSpace, reason);

                            lowSpaceDisks.Add(lowSpace);

                            environmentsRunningOnLowSpaceDisks.Add(item.Environment);
                        }
                    }
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations($"Failed to check disk usage for '{database.Name}' database", e);
                }

                current = current.Next;
            }

            return (lowSpaceDisks, environmentsRunningOnLowSpaceDisks);
        }
        
        internal static bool IsLowSpace(Size totalFreeSpace, Size diskSpace, StorageConfiguration config, out string reason, bool simulateLowDiskSpace = false)
        {
            if (config.FreeSpaceAlertThresholdInMb != null &&
                totalFreeSpace < config.FreeSpaceAlertThresholdInMb.Value)
            {
                reason = $"has {totalFreeSpace} free what is below configured threshold ({config.FreeSpaceAlertThresholdInMb.Value})";
                return true;
            }

            var availableInPercentages = totalFreeSpace.GetValue(SizeUnit.Bytes) * 100f / diskSpace.GetValue(SizeUnit.Bytes);

            if (config.FreeSpaceAlertThresholdInPercentages != null &&
                availableInPercentages < config.FreeSpaceAlertThresholdInPercentages.Value)
            {
                reason = $"has {availableInPercentages:#.#}% free what is below configured threshold ({config.FreeSpaceAlertThresholdInPercentages}%)";
                return true;
            }

            if (simulateLowDiskSpace)
            {
                reason = "low disk space simulation";
                return true;
            }

            reason = null;
            return false;
        }

        public void Dispose()
        {
            _timer?.Dispose();
            _timer = null;
        }

        private class LowDiskSpace
        {
            public LowDiskSpace(DiskSpaceResult diskSpace, string reason)
            {
                Debug.Assert(diskSpace != null);

                _diskSpace = diskSpace;
                _reason = reason;
            }

            private readonly DiskSpaceResult _diskSpace;

            private readonly string _reason;

            public override int GetHashCode()
            {
                return _diskSpace.DriveName.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                if (obj is LowDiskSpace lds)
                    return _diskSpace.DriveName == lds._diskSpace.DriveName;

                return false;
            }

            public override string ToString()
            {
                var drive = _diskSpace.DriveName;

                if (string.IsNullOrWhiteSpace(_diskSpace.VolumeLabel) == false)
                    drive += $" ({_diskSpace.VolumeLabel})";

                return $" - '{drive}' {_reason}";
            }
        }
    }
}
