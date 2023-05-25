using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;

namespace Raven.Server.NotificationCenter;

public class QueueSinkNotification
{
    private readonly NotificationCenter _notificationCenter;
    private readonly NotificationsStorage _notificationsStorage;

    public QueueSinkNotification(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage)
    {
        _notificationCenter = notificationCenter;
        _notificationsStorage = notificationsStorage;
    }
    public AlertRaised AddLoadErrors(string processTag, string processName, Queue<EtlErrorInfo> errors, string preMessage = null)
    {
        var alert = GetOrCreateAlert<EtlErrorsDetails>(processTag,
            processName,
            AlertType.Etl_LoadError,
            $"{preMessage}Loading transformed data to the destination has failed (last {EtlErrorsDetails.MaxNumberOfErrors} errors are shown)",
            out var details);

        return AddErrorAlert(errors, details, alert);
    }
    
    private AlertRaised AddErrorAlert(Queue<EtlErrorInfo> errors, EtlErrorsDetails details, AlertRaised alert)
    {
        details.Update(errors);

        _notificationCenter.Add(alert);

        return alert;
    }
    
    private AlertRaised GetOrCreateAlert<T>(string processTag, string processName, AlertType etlAlertType, string message, out T details) where T : INotificationDetails, new()
    {
        Debug.Assert(etlAlertType == AlertType.Etl_LoadError || etlAlertType == AlertType.Etl_TransformationError);

        var key = $"{processTag}/{processName}";

        var id = AlertRaised.GetKey(etlAlertType, key);

        using (_notificationsStorage.Read(id, out NotificationTableValue ntv))
        {
            //details = GetDetails<T>(ntv);
            details = default;
            
            return AlertRaised.Create(
                //_databaseName,
                "db-name",
                $"{processTag}: '{processName}'",
                message,
                etlAlertType,
                NotificationSeverity.Warning,
                key: key,
                details: details);
        }
    }
}
