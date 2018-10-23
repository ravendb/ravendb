using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Voron;

namespace Raven.Server.Storage
{
    public class StorageSpaceMonitor : IDisposable
    {
        private readonly DocumentDatabase _database;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private static readonly TimeSpan CheckFrequency = TimeSpan.FromMinutes(10);
        private readonly Logger _logger;
        private readonly object _lock = new object();
        private Timer _timer;
        internal bool SimulateLowDiskSpace;

        public StorageSpaceMonitor(DocumentDatabase database, NotificationCenter.NotificationCenter notificationCenter)
        {
            if (database.Configuration.Core.RunInMemory)
                return;

            if (database.Configuration.Storage.FreeSpaceAlertThresholdInPercentages == null &&
                database.Configuration.Storage.FreeSpaceAlertThresholdInMb == null)
                return;

            _database = database;
            _notificationCenter = notificationCenter;
            _logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);

            _timer = new Timer(Run, null, CheckFrequency, CheckFrequency);
        }

        internal void Run(object state)
        {
            if (_notificationCenter.IsInitialized == false)
                return;

            if (Monitor.TryEnter(_lock) == false)
                return;

            try
            {
                List<LowDiskSpace> lowSpaceDisks = null;

                var repeat = false;

                do
                {
                    var didCleanup = false;

                    foreach (var lowSpaceDisk in GetLowSpaceDisks())
                    {
                        if (repeat == false)
                        {
                            foreach (var storageEnvironment in lowSpaceDisk.Value)
                            {
                                storageEnvironment.Cleanup(deleteRecyclableJournals: true);
                            }

                            didCleanup = true;
                        }
                        else
                        {
                            if (lowSpaceDisks == null)
                                lowSpaceDisks = new List<LowDiskSpace>();

                            lowSpaceDisks.Add(lowSpaceDisk.Key);

                            repeat = false;
                        }

                        if (didCleanup)
                            repeat = true;
                    }

                } while (repeat);

                if (lowSpaceDisks != null && lowSpaceDisks.Count > 0)
                {
                    var message =
                        $"The following disks are running out of space:{Environment.NewLine} {string.Join(Environment.NewLine, lowSpaceDisks)}";

                    _notificationCenter.Add(AlertRaised.Create(_database.Name, "Low free disk space", message, AlertType.LowDiskSpace, NotificationSeverity.Warning,
                        "low-disk-space"));

                    if (_logger.IsOperationsEnabled)
                        _logger.Operations(message);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Failed to run storage space monitor", e);
            }
            finally
            {
                Monitor.Exit(_lock);
            }
        }

        private Dictionary<LowDiskSpace, List<StorageEnvironment>> GetLowSpaceDisks()
        {
            var result = new Dictionary<LowDiskSpace, List<StorageEnvironment>>();

            var drives = DriveInfo.GetDrives();
            
            foreach (var item in _database.GetAllStoragesEnvironment())
            {
                var dataDrive = DiskSpaceChecker.GetFreeDiskSpace(item.Environment.Options.BasePath.FullPath, drives);

                if (IsLowSpace(dataDrive, out var reason))
                {
                    var lowSpace = new LowDiskSpace(dataDrive, reason);

                    if (result.TryGetValue(lowSpace, out var environments) == false)
                    {
                        result[lowSpace] = environments = new List<StorageEnvironment>();
                    }

                    environments.Add(item.Environment);
                }

                if (item.Environment.Options.TempPath != null)
                {
                    var tempDrive = DiskSpaceChecker.GetFreeDiskSpace(item.Environment.Options.TempPath.FullPath, drives);

                    if (dataDrive.DriveName != tempDrive.DriveName && IsLowSpace(tempDrive, out reason))
                    {
                        var lowSpace = new LowDiskSpace(dataDrive, reason);

                        if (result.TryGetValue(lowSpace, out var environments) == false)
                        {
                            result[lowSpace] = environments = new List<StorageEnvironment>();
                        }

                        environments.Add(item.Environment);
                    }
                }
            }

            return result;

            bool IsLowSpace(DiskSpaceResult diskSpace, out string reason)
            {
                if (_database.Configuration.Storage.FreeSpaceAlertThresholdInMb != null &&
                    diskSpace.TotalFreeSpace < _database.Configuration.Storage.FreeSpaceAlertThresholdInMb.Value)
                {
                    reason = $"has {diskSpace.TotalFreeSpace} free what is below configured threshold ({_database.Configuration.Storage.FreeSpaceAlertThresholdInMb.Value})";
                    return true;
                }

                var availableInPercentages = diskSpace.TotalFreeSpace.GetValue(SizeUnit.Bytes) * 100f / diskSpace.TotalSize.GetValue(SizeUnit.Bytes);

                if (_database.Configuration.Storage.FreeSpaceAlertThresholdInPercentages != null &&
                    availableInPercentages < _database.Configuration.Storage.FreeSpaceAlertThresholdInPercentages.Value)
                {
                    reason = $"has {availableInPercentages:#.#}% free what is below configured threshold ({_database.Configuration.Storage.FreeSpaceAlertThresholdInPercentages}%)";
                    return true;
                }

                if (SimulateLowDiskSpace)
                {
                    reason = "low disk space simulation";
                    return true;
                }

                reason = null;
                return false;
            }
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
                DiskSpace = diskSpace;
                Reason = reason;
            }

            public readonly DiskSpaceResult DiskSpace;

            public readonly string Reason;

            public override int GetHashCode()
            {
                return DiskSpace.DriveName.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                if (obj is LowDiskSpace lds)
                    return DiskSpace.DriveName == lds.DiskSpace.DriveName;

                return false;
            }

            public override string ToString()
            {
                var drive = DiskSpace.DriveName;

                if (string.IsNullOrWhiteSpace(DiskSpace.VolumeLabel) == false)
                    drive += $" ({DiskSpace.VolumeLabel})";

                return $" - '{drive}' {Reason}";
            }
        }
    }
}
