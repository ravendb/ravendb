using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Conventions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public class QueueSinkNotifications
    {
        private readonly AbstractDatabaseNotificationCenter _notificationCenter;

        public QueueSinkNotifications(AbstractDatabaseNotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter;
        }

        public AlertRaised AddScriptErrors(string processTag, string processName, Queue<QueueSinkErrorInfo> errors, string preMessage = null)
        {
            var alert = GetOrCreateAlert<QueueSinkErrorsDetails>(processTag,
                processName,
                AlertType.QueueSink_ScriptError,
                $"{preMessage}Script has failed for the following messages (last {QueueSinkErrorsDetails.MaxNumberOfErrors} errors are shown)",
                out var details);

            return AddErrorAlert(errors, details, alert);
        }

        public AlertRaised AddConsumeErrors(string processTag, string processName, Queue<QueueSinkErrorInfo> errors, string preMessage = null)
        {
            var alert = GetOrCreateAlert<QueueSinkErrorsDetails>(processTag,
                processName,
                AlertType.QueueSink_ConsumeError,
                $"{preMessage}Consume messages has failed (last {QueueSinkErrorsDetails.MaxNumberOfErrors} errors are shown)",
                out var details);

            return AddErrorAlert(errors, details, alert);
        }

        private AlertRaised AddErrorAlert(Queue<QueueSinkErrorInfo> errors, QueueSinkErrorsDetails details, AlertRaised alert)
        {
            details.Update(errors);

            _notificationCenter.Add(alert);

            return alert;
        }

        private AlertRaised GetOrCreateAlert<T>(string processTag, string processName, AlertType alertType, string message, out T details) where T : INotificationDetails, new()
        {
            Debug.Assert(alertType == AlertType.QueueSink_ConsumeError || alertType == AlertType.QueueSink_ScriptError);

            var key = $"{processTag}/{processName}";

            var id = AlertRaised.GetKey(alertType, key);

            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                details = GetDetails<T>(ntv);

                return AlertRaised.Create(
                    _notificationCenter.Database,
                    $"{processTag}: '{processName}'",
                    message,
                    alertType,
                    NotificationSeverity.Warning,
                    key: key,
                    details: details);
            }
        }

        public T GetAlert<T>(string processTag, string processName, AlertType alertType) where T : INotificationDetails, new()
        {
            Debug.Assert(
                alertType is AlertType.QueueSink_ConsumeError or AlertType.QueueSink_ScriptError or AlertType.QueueSink_Error
                    or AlertType.QueueSink_ConsumerCreationError, $"Got type: {alertType}");

            var key = $"{processTag}/{processName}";

            var id = AlertRaised.GetKey(alertType, key);

            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                return GetDetails<T>(ntv);
            }
        }

        private T GetDetails<T>(NotificationTableValue ntv) where T : INotificationDetails, new()
        {
            if (ntv == null || ntv.Json.TryGet(nameof(AlertRaised.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                return new T();

            return DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<T>(detailsJson);
        }
    }
}
