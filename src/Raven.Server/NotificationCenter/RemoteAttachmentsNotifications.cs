using System.Diagnostics;
using Raven.Client.Documents.Conventions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public class RemoteAttachmentsNotifications
    {
        private readonly AbstractDatabaseNotificationCenter _notificationCenter;

        public RemoteAttachmentsNotifications(AbstractDatabaseNotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter;
        }

        private static readonly string AlertTitleError = "Remote attachment upload failed.";
        private static readonly string MisconfigurationAlertMessage = $"Failed to upload the attachments to remote storage due to misconfiguration. (last {RemoteAttachmentsErrorsDetails.MaxNumberOfErrors} errors are shown)";

        public AlertRaised AddConfigurationAlerts(RemoteAttachmentsErrorInfo error)
        {
            var alert = GetOrCreateAlert<RemoteAttachmentsErrorsDetails>(AlertTitleError,
                MisconfigurationAlertMessage,
                AlertReason.Attachments_RemoteAttachmentWithoutIdentifier,
                nameof(AlertReason.Attachments_RemoteAttachmentWithoutIdentifier),
                identifier: null,
                out var details);

            return AddErrorAlert(error, details, alert);
        }

        public AlertRaised AddUploadErrors(string msg, string identifier, RemoteAttachmentsErrorInfo error)
        {
            var alert = GetOrCreateAlert<RemoteAttachmentsErrorsDetails>($"{AlertTitleError} for '{identifier}'",
                $"{msg} (last {RemoteAttachmentsErrorsDetails.MaxNumberOfErrors} errors are shown)",
                AlertReason.Attachments_RemoteAttachmentErroredIdentifier,
                nameof(AlertReason.Attachments_RemoteAttachmentErroredIdentifier),
                identifier,
                out var details);

            return AddErrorAlert(error, details, alert);
        }

        private AlertRaised AddErrorAlert(RemoteAttachmentsErrorInfo error, RemoteAttachmentsErrorsDetails details, AlertRaised alert)
        {
            details.Add(error);

            _notificationCenter.Add(alert);

            return alert;
        }

        private AlertRaised GetOrCreateAlert<T>(string title, string message, AlertReason alertReason, string tag, string identifier, out T details) where T : INotificationDetails, new()
        {
            Debug.Assert(alertReason == AlertReason.Attachments_RemoteAttachmentWithoutIdentifier || alertReason == AlertReason.Attachments_RemoteAttachmentErroredIdentifier);

            string key = GetAlertId<T>(alertReason, tag, identifier, out string id);

            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                details = GetDetails<T>(ntv);

                return AlertRaised.Create(
                    _notificationCenter.Database,
                    title,
                    message,
                    alertReason,
                    NotificationSeverity.Warning,
                    key: key,
                    details: details);
            }
        }

        private static string GetAlertId<T>(AlertReason alertReason, string tag, string identifier, out string id) where T : INotificationDetails, new()
        {
            var key = string.IsNullOrEmpty(identifier) ? tag : $"{tag}/{identifier}";
            id = AlertRaised.GetKey(alertReason, key: key);
            return key;
        }

        public T GetAlert<T>(string tag, string identifier, AlertReason alertReason) where T : INotificationDetails, new()
        {
            Debug.Assert(
                alertReason is AlertReason.Attachments_RemoteAttachmentWithoutIdentifier or AlertReason.Attachments_RemoteAttachmentErroredIdentifier, $"Got type: {alertReason}");

            string key = GetAlertId<T>(alertReason, tag, identifier, out string id);


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
