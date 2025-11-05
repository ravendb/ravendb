using System;
using System.Threading;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Json;

namespace Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications;

public class DatabaseNotificationsSummaryNotificationSender : AbstractClusterDashboardNotificationSender
{
    private readonly DatabasesInfoRetriever _databasesInfoRetriever;
    private DatabaseNotificationsSummaryRequestConfig _databaseNotificationsSummaryRequestConfig;
    
    public DatabaseNotificationsSummaryNotificationSender(int widgetId, DatabasesInfoRetriever databasesInfoRetriever, ConnectedWatcher watcher, BlittableJsonReaderObject configuration, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
    {
        _databasesInfoRetriever = databasesInfoRetriever;
        _databaseNotificationsSummaryRequestConfig = JsonDeserializationServer.NotificationsSummaryRequestConfig(configuration);
    }

    protected override TimeSpan NotificationInterval { get; } = TimeSpan.FromSeconds(30);
    
    protected override AbstractClusterDashboardNotification CreateNotification()
    {
        var databasesNotificationsSummary = _databasesInfoRetriever.GetDatabaseNotificationsSummary();

        var notificationsSummaryPayload = new DatabaseNotificationsSummaryPayload();

        foreach (var item in databasesNotificationsSummary.Items)
        {
            var databaseNotificationsSummary = new DatabaseNotificationsSummary()
            {
                DatabaseName = item.DatabaseName
            };

            foreach (var notificationCountsKvp in item.NotificationCounts.ByType)
            {
                var notificationType = notificationCountsKvp.Key;

                var typeSettings = notificationType switch
                {
                    NotificationType.AlertRaised => _databaseNotificationsSummaryRequestConfig.Alerts,
                    NotificationType.PerformanceHint => _databaseNotificationsSummaryRequestConfig.PerformanceHints,
                    _ => null
                };
                
                if (typeSettings == null || typeSettings.IsEnabled == false)
                    continue;
                
                foreach (var notificationReasonCount in notificationCountsKvp.Value.ByReason)
                {
                    if (typeSettings.Reasons.Count > 0 && typeSettings.Reasons.Contains(notificationReasonCount.Key) == false)
                        continue;
                    
                    var notificationSummaryItem = new NotificationSummaryItem
                    {
                        Reason = notificationReasonCount.Key,
                        PrettifiedReason = NotificationSummaryItem.PrettifyReason(notificationReasonCount.Key),
                        Count = notificationReasonCount.Value
                    };

                    switch (notificationType)
                    {
                        case NotificationType.AlertRaised:
                            databaseNotificationsSummary.Alerts.Add(notificationSummaryItem);
                            databaseNotificationsSummary.AlertsCount += notificationSummaryItem.Count;
                            break;
                        case NotificationType.PerformanceHint:
                            databaseNotificationsSummary.PerformanceHints.Add(notificationSummaryItem);
                            databaseNotificationsSummary.PerformanceHintsCount += notificationSummaryItem.Count;
                            break;
                        default:
                            throw new NotSupportedException($"Unsupported {nameof(NotificationType)}: {notificationType}");
                    }
                }
            }
            
            notificationsSummaryPayload.NotificationsSummary.Add(databaseNotificationsSummary);
        }

        return notificationsSummaryPayload;
    }

    internal override void UpdateConfiguration(BlittableJsonReaderObject configuration)
    {
        _databaseNotificationsSummaryRequestConfig = JsonDeserializationServer.NotificationsSummaryRequestConfig(configuration);
        
        // Force new notification summary to be sent on configuration change rather than in configured interval
        EnqueueNotification();
    }
}
