using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Conventions;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public class EtlNotifications
    {
        private readonly NotificationCenter _notificationCenter;
        private readonly NotificationsStorage _notificationsStorage;
        private readonly string _databaseName;

        public EtlNotifications(NotificationCenter notificationCenter, NotificationsStorage notificationsStorage, string databaseName)
        {
            _notificationCenter = notificationCenter;
            _notificationsStorage = notificationsStorage;
            _databaseName = databaseName;
        }

        public AlertRaised AddTransformationErrors(string processTag, string processName, Queue<EtlErrorInfo> errors, string preMessage = null)
        {
            var alert = GetOrCreateAlert<EtlErrorsDetails>(processTag,
                processName,
                AlertType.Etl_TransformationError,
                $"{preMessage}Transformation has failed for the following documents (last {EtlErrorsDetails.MaxNumberOfErrors} errors are shown)",
                out var details);

            return AddErrorAlert(errors, details, alert);
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

        public void AddSlowSqlWarnings(string processTag, string processName, Queue<SlowSqlStatementInfo> slowSqls)
        {
            var alert = GetOrCreatePerformanceHint<SlowSqlDetails>(processTag,
                processName,
                PerformanceHintType.SqlEtl_SlowSql,
                $"Slow SQL detected (last {SlowSqlDetails.MaxNumberOfStatements} statements are shown)",
                out var details);

            foreach (var slowSql in slowSqls)
            {
                details.Add(slowSql);
            }

            _notificationCenter.Add(alert);
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
                using (ntv)
                {
                    details = GetDetails<T>(ntv);

                    return AlertRaised.Create(
                        _databaseName,
                        $"{processTag}: '{processName}'",
                        message,
                        etlAlertType,
                        NotificationSeverity.Warning,
                        key: key,
                        details: details);
                }
            }
        }

        public T GetAlert<T>(string processTag, string processName, AlertType etlAlertType) where T : INotificationDetails, new()
        {
            Debug.Assert(etlAlertType == AlertType.Etl_LoadError || etlAlertType == AlertType.Etl_TransformationError);

            var key = $"{processTag}/{processName}";

            var id = AlertRaised.GetKey(etlAlertType, key);

            using (_notificationsStorage.Read(id, out NotificationTableValue ntv))
            {
                using (ntv)
                    return GetDetails<T>(ntv);
            }
        }

        private PerformanceHint GetOrCreatePerformanceHint<T>(string processTag, string processName, PerformanceHintType etlHintType, string message, out T details) where T : INotificationDetails, new()
        {
            Debug.Assert(etlHintType == PerformanceHintType.SqlEtl_SlowSql);

            var key = $"{processTag}/{processName}";

            var id = PerformanceHint.GetKey(etlHintType, key);

            using (_notificationsStorage.Read(id, out NotificationTableValue ntv))
            {
                using (ntv)
                {
                    details = GetDetails<T>(ntv);

                    return PerformanceHint.Create(
                        _databaseName,
                        $"{processTag}: '{processName}'",
                        message,
                        etlHintType,
                        NotificationSeverity.Warning,
                        source: key,
                        details: details);
                }
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
