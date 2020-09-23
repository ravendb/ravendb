using System;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL
{
    public class EtlProcessStatistics : IDynamicJson
    {
        private readonly string _processTag;
        private readonly string _processName;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;

        private readonly EnsureAlerts _alertsGuard;

        private bool _preventFromAddingAlertsToNotificationCenter;

        public EtlProcessStatistics(string processTag, string processName, NotificationCenter.NotificationCenter notificationCenter)
        {
            _processTag = processTag;
            _processName = processName;
            _notificationCenter = notificationCenter;
            TransformationErrorsInCurrentBatch = new EtlErrorsDetails();
            LastLoadErrorsInCurrentBatch = new EtlErrorsDetails();
            LastSlowSqlWarningsInCurrentBatch = new SlowSqlDetails();
            _alertsGuard = new EnsureAlerts(this);
        }

        public string LastChangeVector { get; set; }

        public long LastProcessedEtag { get; set; }

        public DateTime? LastTransformationErrorTime { get; private set; }

        public DateTime? LastLoadErrorTime { get; private set; }

        private int TransformationErrors { get; set; }

        private int TransformationSuccesses { get; set; }

        public int LoadErrors { get; set; }

        public int LoadSuccesses { get; private set; }

        public int LoadSuccessesInCurrentBatch { get; private set; }

        public AlertRaised LastAlert { get; set; }

        public EtlErrorsDetails TransformationErrorsInCurrentBatch { get; }

        public EtlErrorsDetails LastLoadErrorsInCurrentBatch { get; }

        public SlowSqlDetails LastSlowSqlWarningsInCurrentBatch { get; }

        public bool WasLatestLoadSuccessful { get; set; }

        public void TransformationSuccess()
        {
            TransformationSuccesses++;
        }

        public IDisposable NewBatch()
        {
            TransformationErrorsInCurrentBatch.Errors.Clear();
            LastLoadErrorsInCurrentBatch.Errors.Clear();
            LastSlowSqlWarningsInCurrentBatch.Statements.Clear();
            LoadSuccessesInCurrentBatch = 0;

            return _alertsGuard;
        }

        public void RecordTransformationError(Exception e, string documentId)
        {
            TransformationErrors++;

            LastTransformationErrorTime = SystemTime.UtcNow;

            TransformationErrorsInCurrentBatch.Add(new EtlErrorInfo
            {
                Date = SystemTime.UtcNow,
                DocumentId = documentId,
                Error = e.ToString()
            });

            if (TransformationErrors < 100)
                return;

            if (TransformationErrors <= TransformationSuccesses)
                return;

            var message = $"Transformation error ratio is too high (errors: {TransformationErrors}, successes: {TransformationSuccesses}). " +
                          "Could not tolerate transformation error ratio and stopped current ETL batch. ";

            CreateAlertIfAnyTransformationErrors(message);

            throw new InvalidOperationException($"{message}. Current stats: {this}");
        }

        public void RecordLoadError(string error, string documentId, int count = 1)
        {
            WasLatestLoadSuccessful = false;

            LoadErrors += count;

            LastLoadErrorTime = SystemTime.UtcNow;

            LastLoadErrorsInCurrentBatch.Add(new EtlErrorInfo
            {
                Date = SystemTime.UtcNow,
                DocumentId = documentId,
                Error = error
            });

            if (LoadErrors < 100)
                return;

            if (LoadErrors <= LoadSuccesses)
                return;

            var message = $"Load error ratio is too high (errors: {LoadErrors}, successes: {LoadSuccesses}). " +
                          "Could not tolerate load error ratio and stopped current ETL batch. ";

            CreateAlertIfAnyLoadErrors(message);

            throw new InvalidOperationException($"{message}. Current stats: {this}. Error: {error}");
        }

        public void RecordSlowSql(SlowSqlStatementInfo slowSql)
        {
            LastSlowSqlWarningsInCurrentBatch.Add(slowSql);
        }

        public void LoadSuccess(int items)
        {
            WasLatestLoadSuccessful = true;
            LoadSuccesses += items;
            LoadSuccessesInCurrentBatch += items;
        }

        private void CreateAlertIfAnyTransformationErrors(string preMessage = null)
        {
            if (TransformationErrorsInCurrentBatch.Errors.Count == 0 || _preventFromAddingAlertsToNotificationCenter)
                return;

            LastAlert = _notificationCenter.EtlNotifications.AddTransformationErrors(_processTag, _processName, TransformationErrorsInCurrentBatch.Errors, preMessage);

            TransformationErrorsInCurrentBatch.Errors.Clear();
        }

        private void CreateAlertIfAnyLoadErrors(string preMessage = null)
        {
            if (LastLoadErrorsInCurrentBatch.Errors.Count == 0 || _preventFromAddingAlertsToNotificationCenter)
                return;

            LastAlert = _notificationCenter.EtlNotifications.AddLoadErrors(_processTag, _processName, LastLoadErrorsInCurrentBatch.Errors, preMessage);

            LastLoadErrorsInCurrentBatch.Errors.Clear();
        }

        private void CreateAlertIfAnySlowSqls()
        {
            if (LastSlowSqlWarningsInCurrentBatch.Statements.Count == 0 || _preventFromAddingAlertsToNotificationCenter)
                return;

            _notificationCenter.EtlNotifications.AddSlowSqlWarnings(_processTag, _processName, LastSlowSqlWarningsInCurrentBatch.Statements);

            LastSlowSqlWarningsInCurrentBatch.Statements.Clear();
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(LastAlert)] = LastAlert?.ToJson(),
                [nameof(LastTransformationErrorTime)] = LastTransformationErrorTime,
                [nameof(LastLoadErrorTime)] = LastLoadErrorTime,
                [nameof(LastProcessedEtag)] = LastProcessedEtag,
                [nameof(TransformationSuccesses)] = TransformationSuccesses,
                [nameof(TransformationErrors)] = TransformationErrors,
                [nameof(LoadSuccesses)] = LoadSuccesses,
                [nameof(LoadErrors)] = LoadErrors
            };
            return json;
        }

        public override string ToString()
        {
            return $"{nameof(LastProcessedEtag)}: {LastProcessedEtag} " +
                   $"{nameof(LastTransformationErrorTime)}: {LastTransformationErrorTime} " +
                   $"{nameof(LastLoadErrorTime)}: {LastLoadErrorTime} " +
                   $"{nameof(TransformationSuccesses)}: {TransformationSuccesses} " +
                   $"{nameof(TransformationErrors)}: {TransformationErrors} " +
                   $"{nameof(LoadSuccesses)}: {LoadSuccesses} " +
                   $"{nameof(LoadErrors)}: {LoadErrors}";
        }

        public void Reset()
        {
            LastProcessedEtag = 0;
            LastTransformationErrorTime = null;
            LastLoadErrorTime = null;
            TransformationSuccesses = 0;
            TransformationErrors = 0;
            LoadSuccesses = 0;
            LoadSuccessesInCurrentBatch = 0;
            LoadErrors = 0;
            LastChangeVector = null;
            LastAlert = null;
            TransformationErrorsInCurrentBatch.Errors.Clear();
            LastLoadErrorsInCurrentBatch.Errors.Clear();
            LastSlowSqlWarningsInCurrentBatch.Statements.Clear();
        }

        private class EnsureAlerts : IDisposable
        {
            private readonly EtlProcessStatistics _parent;

            public EnsureAlerts(EtlProcessStatistics parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                _parent.CreateAlertIfAnyTransformationErrors();
                _parent.CreateAlertIfAnySlowSqls();
                _parent.CreateAlertIfAnyLoadErrors();
            }
        }

        public IDisposable PreventFromAddingAlertsToNotificationCenter()
        {
            _preventFromAddingAlertsToNotificationCenter = true;

            return new DisposableAction(() => _preventFromAddingAlertsToNotificationCenter = false);
        }
    }
}
