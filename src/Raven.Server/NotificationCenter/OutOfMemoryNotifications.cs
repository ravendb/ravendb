using System;
using System.Collections.Concurrent;
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
        private readonly ConcurrentDictionary<string, NotificationTime> _notificationTimes = new ConcurrentDictionary<string, NotificationTime>();
        private readonly ConcurrentDictionary<(StorageEnvironment, Type), NotificationTime> _notificationsMetadata = new ConcurrentDictionary<(StorageEnvironment, Type), NotificationTime>();
        private readonly NotificationCenter _notificationsCenter;

        public OutOfMemoryNotifications(NotificationCenter notificationsCenter)
        {
            _notificationsCenter = notificationsCenter;
        }

        public void Add(string title, string key, Exception exception)
        {
            if (_notificationTimes.TryGetValue(key, out var notificationTime))
            {
                if (DateTime.Now - notificationTime.Time < _updateFrequency)
                    return;
                
                if (Interlocked.CompareExchange(ref notificationTime.IsInProgress, 1, 0) == 1)
                    return;
                
                notificationTime.Time = DateTime.Now;
            }
            else
            {
                notificationTime = new NotificationTime{ Time = DateTime.Now, IsInProgress = 1 };
                if (_notificationTimes.TryAdd(key, notificationTime) == false)
                    return;
            }
            
            var message = $"Error message: {exception.Message}";
            var alert = AlertRaised.Create(
                null,
                title,
                message,
                AlertType.OutOfMemoryException,
                NotificationSeverity.Error,
                key,
                details: new MessageDetails
                {
                    Message = OutOfMemoryDetails(exception)
                });
            
            _notificationsCenter.Add(alert);
            
            Volatile.Write(ref notificationTime.IsInProgress, 0);
        }

        public void Add(StorageEnvironment environment, Exception exception)
        {
            if (_notificationsMetadata.TryGetValue((environment, exception.GetType()), out var notificationMetadata))
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
                if (_notificationsMetadata.TryAdd((environment, exception.GetType()), notificationMetadata) == false)
                    return;

                //We are in low of memory so we want to allocate the strings only once
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
                details: new MessageDetails
                {
                    Message = OutOfMemoryDetails(exception)
                });

            _notificationsCenter.Add(alert);

            Volatile.Write(ref notificationMetadata.IsInProgress, 0);
        }

        private static string OutOfMemoryDetails(Exception oome)
        {
            var memoryInfo = MemoryInformation.GetMemoryInformationUsingOneTimeSmapsReader();

            return $"Managed memory: {new Size(AbstractLowMemoryMonitor.GetManagedMemoryInBytes(), SizeUnit.Bytes)}, " +
                   $"Unmanaged allocations: {new Size(AbstractLowMemoryMonitor.GetUnmanagedAllocationsInBytes(), SizeUnit.Bytes)}, " +
                   $"Shared clean: {memoryInfo.SharedCleanMemory}, " +
                   $"Working set: {memoryInfo.WorkingSet}, " +
                   $"Available memory: {memoryInfo.AvailableMemory}, " +
                   $"Calculated Available memory: {memoryInfo.AvailableWithoutTotalCleanMemory}, " +
                   $"Total memory: {memoryInfo.TotalPhysicalMemory} {Environment.NewLine}" +
                   $"Error: {oome}";
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
