using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Conventions;
using Raven.Server.Documents.ETL;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;

namespace Raven.Server.NotificationCenter
{
    public sealed class EtlNotifications
    {
        private readonly AbstractDatabaseNotificationCenter _notificationCenter;

        public EtlNotifications(AbstractDatabaseNotificationCenter notificationCenter)
        {
            _notificationCenter = notificationCenter;
        }

        public void AddSlowSqlWarnings(string processTag, string processName, Queue<SlowSqlStatementInfo> slowSqls)
        {
            var alert = GetOrCreatePerformanceHint<SlowSqlDetails>(processTag,
                processName,
                PerformanceHintReason.SqlEtl_SlowSql,
                $"Slow SQL detected (last {SlowSqlDetails.MaxNumberOfStatements} statements are shown)",
                out var details);

            foreach (var slowSql in slowSqls)
            {
                details.Add(slowSql);
            }

            _notificationCenter.Add(alert);
        }

        public void AddTaskHealthChangeNotification(string processTag, string processName, EtlProcessHealthStatus status)
        {
            var existingAlert = GetAlert<EtlTaskHealthChangeDetails>(processTag, processName, AlertReason.Etl_HealthStatusChange);

            var details = new EtlTaskHealthChangeDetails { HealthStatus = status };

            if (existingAlert != null)
            {
                var existingDetails = existingAlert.Details as EtlTaskHealthChangeDetails;
                details.PreviousHealthStatus = existingDetails?.HealthStatus;
                details.PreviousHealthStatusChangeAt = existingAlert.CreatedAt;
            }

            var key = $"{processTag}/{processName}";

            var message = status == EtlProcessHealthStatus.Healthy ? $"Task recovered to {nameof(EtlProcessHealthStatus.Healthy)} status." : $"Task health status was changed to {status}.";

            var alert = AlertRaised.Create(
                _notificationCenter.Database,
                $"{processTag}: '{processName}'",
                message,
                AlertReason.Etl_HealthStatusChange,
                NotificationSeverity.Warning,
                key: key,
                details: details);

            _notificationCenter.Add(alert);
        }
        
        public AlertRaised AddWarning(string processTag, string processName, string message, string documentId, string timeSeriesName)
        {
            var alert = GetOrCreateAlert<EtlWarningDetails>(
                processTag,
                processName,
                AlertReason.Etl_Warning,
                message,
                out var details);

            details.DocumentId = documentId;
            details.TimeSeriesName = timeSeriesName;

            _notificationCenter.Add(alert);
            return alert;
        }

        private AlertRaised GetOrCreateAlert<T>(string processTag, string processName, AlertReason etlAlertReason, string message, out T details) where T : INotificationDetails, new()
        {
            Debug.Assert(etlAlertReason == AlertReason.Etl_LoadError 
                         || etlAlertReason == AlertReason.Etl_TransformationError 
                         || etlAlertReason == AlertReason.Etl_HealthStatusChange
                         || etlAlertReason == AlertReason.Etl_Warning);

            var key = $"{processTag}/{processName}";

            var id = AlertRaised.GetKey(etlAlertReason, key);

            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                using (ntv)
                {
                    details = GetDetails<T>(ntv);

                    return AlertRaised.Create(
                        _notificationCenter.Database,
                        $"{processTag}: '{processName}'",
                        message,
                        etlAlertReason,
                        NotificationSeverity.Warning,
                        key: key,
                        details: details);
                }
            }
        }

        public AlertRaised GetAlert<T>(string processTag, string processName, AlertReason etlAlertReason)
            where T : INotificationDetails, new()
        {
            Debug.Assert(etlAlertReason == AlertReason.Etl_LoadError || etlAlertReason == AlertReason.Etl_TransformationError || etlAlertReason == AlertReason.Etl_HealthStatusChange);

            var key = $"{processTag}/{processName}";

            var id = AlertRaised.GetKey(etlAlertReason, key);

            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                if (ntv == null)
                    return null;

                var details = GetDetails<T>(ntv);

                return AlertRaised.FromJson(key, ntv.Json, details);
            }
        }

        private PerformanceHint GetOrCreatePerformanceHint<T>(string processTag, string processName, PerformanceHintReason etlHintReason, string message, out T details) where T : INotificationDetails, new()
        {
            Debug.Assert(etlHintReason == PerformanceHintReason.SqlEtl_SlowSql);

            var key = $"{processTag}/{processName}";

            var id = PerformanceHint.GetKey(etlHintReason, key);

            using (_notificationCenter.Storage.Read(id, out NotificationTableValue ntv))
            {
                using (ntv)
                {
                    details = GetDetails<T>(ntv);

                    return PerformanceHint.Create(
                        _notificationCenter.Database,
                        $"{processTag}: '{processName}'",
                        message,
                        etlHintReason,
                        NotificationSeverity.Warning,
                        source: key,
                        details: details);
                }
            }
        }

        private static T GetDetails<T>(NotificationTableValue ntv) where T : INotificationDetails, new()
        {
            if (ntv == null || ntv.Json.TryGet(nameof(AlertRaised.Details), out BlittableJsonReaderObject detailsJson) == false || detailsJson == null)
                return new T();

            return DocumentConventions.DefaultForServer.Serialization.DefaultConverter.FromBlittable<T>(detailsJson);
        }
    }
}
