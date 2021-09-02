using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;
using Sparrow.LowMemory;
using Voron;

namespace Raven.Server.NotificationCenter
{
    public class OutOfMemoryNotifications
    {
        private readonly TimeSpan _updateFrequency = TimeSpan.FromSeconds(15);
        private readonly ConditionalWeakTable<StorageEnvironment, ConcurrentDictionary<Type, NotificationTime>> _notificationsMetadataTable = 
            new ConditionalWeakTable<StorageEnvironment, ConcurrentDictionary<Type, NotificationTime>>();
        private readonly NotificationCenter _notificationsCenter;

        public OutOfMemoryNotifications(NotificationCenter notificationsCenter)
        {
            _notificationsCenter = notificationsCenter;
        }

        public void Add(StorageEnvironment environment, Exception exception)
        {
            var notificationsMetadata = _notificationsMetadataTable.GetOrCreateValue(environment);
            if (notificationsMetadata.TryGetValue(exception.GetType(), out var notificationMetadata))
            {
                if (DateTime.Now - notificationMetadata.Time < _updateFrequency)
                    return;

                if (Interlocked.CompareExchange(ref notificationMetadata.IsInProgress, 1, 0) == 1)
                    return;

                notificationMetadata.Time = DateTime.Now;
            }
            else
            {
                notificationMetadata = new NotificationTime
                {
                    Time = DateTime.Now,
                    IsInProgress = 1
                };
                if (notificationsMetadata.TryAdd(exception.GetType(), notificationMetadata) == false)
                    return;

                //We are in low of memory so we want to minimize allocations
                notificationMetadata.Key = $"{environment}:{exception.GetType()}";
                notificationMetadata.Title = $"Out of memory occurred for '{environment}'";
            }

            var alert = AlertRaised.Create(
                null,
                notificationMetadata.Title,
                exception.Message,
                AlertType.OutOfMemoryException,
                NotificationSeverity.Error,
                notificationMetadata.Key,
                OutOfMemoryDetails(exception));

            _notificationsCenter.Add(alert);

            Volatile.Write(ref notificationMetadata.IsInProgress, 0);
        }

        private static MessageDetails OutOfMemoryDetails(Exception exception)
        {
            MemoryInfoResult memoryInfo;

            if (exception is EarlyOutOfMemoryException eoome && eoome.MemoryInfo != null)
                memoryInfo = eoome.MemoryInfo.Value;
            else
                memoryInfo = MemoryInformation.GetMemoryInformationUsingOneTimeSmapsReader();

            return new MessageDetails
            {
                Message = $"Managed memory: {new Size(AbstractLowMemoryMonitor.GetManagedMemoryInBytes(), SizeUnit.Bytes)}, " +
                          $"Unmanaged allocations: {new Size(AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes(), SizeUnit.Bytes)}, " +
                          $"Shared clean: {memoryInfo.SharedCleanMemory}, " +
                          $"Working set: {memoryInfo.WorkingSet}, " +
                          $"Available memory: {memoryInfo.AvailableMemory}, " +
                          $"Calculated Available memory: {memoryInfo.AvailableMemoryForProcessing}, " +
                          $"Total Scratch Dirty memory: {memoryInfo.TotalScratchDirtyMemory}, " +
                          $"Total memory: {memoryInfo.TotalPhysicalMemory} {Environment.NewLine}" +
                          $"Error: {exception}"
            };
        }
        
        private class NotificationTime
        {
            public int IsInProgress;
            public DateTime Time;
            public string Key;
            public string Title;
        }
    }
}
