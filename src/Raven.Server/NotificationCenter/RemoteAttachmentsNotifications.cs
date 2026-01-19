using System;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents.Conventions;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.NotificationCenter
{
    public class RemoteAttachmentsNotifications : IDisposable
    {
        private readonly AbstractDatabaseNotificationCenter _notificationCenter;
        private Timer _timer;
        private  AlertRaised _alert;
        private readonly Lock _locker = new();
        private volatile bool _needsSync;
        private readonly RavenLogger _logger;
        private RemoteAttachmentsErrorsDetails _details;

        public RemoteAttachmentsNotifications(AbstractDatabaseNotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter;
            _logger = RavenLogManager.Instance.GetLoggerForDatabase<RemoteAttachmentsNotifications>(notificationCenter.Database);
        }

        private static readonly string AlertTitleError = "Remote attachment upload failed.";

        public void AddUploadErrors(string msg, string identifier, RemoteAttachmentsErrorInfo error)
        {
            lock (_locker)
            {
                if (_alert == null)
                {
                    _alert = GetOrCreateAlert(AlertTitleError,
                        $"{msg} (last {RemoteAttachmentsErrorsDetails.MaxNumberOfErrors} errors are shown)",
                        AlertReason.Attachments_RemoteAttachmentErroredIdentifier,
                        nameof(AlertReason.Attachments_RemoteAttachmentErroredIdentifier),
                        identifier,
                        out _details);
                }

                _details.Add(error);

                _needsSync = true;

                if (_timer != null)
                    return;

                _timer = new Timer(PublishErrorAlerts, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
            }

        }

        internal void PublishErrorAlerts(object state)
        {
            try
            {
                if (_needsSync == false)
                    return;

                lock (_locker)
                {
                    _needsSync = false;

                    _alert.RefreshCreatedAt();
                    _notificationCenter.Add(_alert);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error in a request latency timer", e);
            }
        }

        private AlertRaised GetOrCreateAlert(string title, string message, AlertReason alertReason, string tag, string identifier, out RemoteAttachmentsErrorsDetails details)
        {
            Debug.Assert(alertReason == AlertReason.Attachments_RemoteAttachmentWithoutIdentifier || alertReason == AlertReason.Attachments_RemoteAttachmentErroredIdentifier);

            string key = GetAlertId<RemoteAttachmentsErrorsDetails>(alertReason, tag, identifier, out string id);

            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                details = GetDetails<RemoteAttachmentsErrorsDetails>(ntv);

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

        public void Dispose()
        {
            _timer?.Dispose();
            _timer = null;
        }
    }
}
