using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.NotificationCenter
{
    public class OutOfMemoryNotifications
    {
        private readonly TimeSpan _updateFrequency = TimeSpan.FromSeconds(15);
        private readonly ConcurrentDictionary<string, NotificationTime> _notificationTimes = new ConcurrentDictionary<string, NotificationTime>();
        private readonly NotificationCenter _notificationsCenter;

        public OutOfMemoryNotifications(NotificationCenter notificationsCenter)
        {
            _notificationsCenter = notificationsCenter;
        }

        public void Add(string title, string key, Exception oome)
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
            
            var message = $"Error message: {oome.Message}";
            var alert = AlertRaised.Create(
                null,
                title,
                message,
                AlertType.OutOfMemoryException,
                NotificationSeverity.Error,
                key,
                details: new MessageDetails
                {
                    Message = OutOfMemoryDetails(oome)
                });
            
            _notificationsCenter.Add(alert);
            
            Volatile.Write(ref notificationTime.IsInProgress, 0);
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
        }
    }
}
