using System;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL
{
    public class EtlProcessStatistics
    {
        private readonly string _processTag;
        private readonly string _processName;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;
        private readonly EtlErrorsDetails _transformationErrorsInCurrentBatch;
        private readonly EtlErrorsDetails _loadErrorsInCurrentBatch;
        private readonly SlowSqlDetails _slowSqlsInCurrentBatch;

        private readonly EnsureAlerts _alertsGuard;

        public EtlProcessStatistics(string processTag, string processName, NotificationCenter.NotificationCenter notificationCenter)
        {
            _processTag = processTag;
            _processName = processName;
            _notificationCenter = notificationCenter;
            _transformationErrorsInCurrentBatch = new EtlErrorsDetails();
            _loadErrorsInCurrentBatch = new EtlErrorsDetails();
            _slowSqlsInCurrentBatch = new SlowSqlDetails();
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

        public AlertRaised LastAlert { get; set; }

        public bool WasLatestLoadSuccessful { get; set; }

        public void TransformationSuccess()
        {
            TransformationSuccesses++;
        }

        public IDisposable NewBatch()
        {
            _transformationErrorsInCurrentBatch.Errors.Clear();
            _loadErrorsInCurrentBatch.Errors.Clear();
            _slowSqlsInCurrentBatch.Statements.Clear();

            return _alertsGuard;
        }

        public void RecordTransformationError(Exception e, string documentId)
        {
            TransformationErrors++;

            LastTransformationErrorTime = SystemTime.UtcNow;

            _transformationErrorsInCurrentBatch.Add(new EtlErrorInfo
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

            _loadErrorsInCurrentBatch.Add(new EtlErrorInfo
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
            _slowSqlsInCurrentBatch.Add(slowSql);
        }

        public void LoadSuccess(int items)
        {
            WasLatestLoadSuccessful = true;
            LoadSuccesses += items;
        }

        private void CreateAlertIfAnyTransformationErrors(string preMessage = null)
        {
            if (_transformationErrorsInCurrentBatch.Errors.Count == 0)
                return;

            LastAlert = _notificationCenter.EtlNotifications.AddTransformationErrors(_processTag, _processName, _transformationErrorsInCurrentBatch.Errors, preMessage);

            _transformationErrorsInCurrentBatch.Errors.Clear();
        }

        private void CreateAlertIfAnyLoadErrors(string preMessage = null)
        {
            if (_loadErrorsInCurrentBatch.Errors.Count == 0)
                return;

            LastAlert = _notificationCenter.EtlNotifications.AddLoadErrors(_processTag, _processName, _loadErrorsInCurrentBatch.Errors, preMessage);

            _loadErrorsInCurrentBatch.Errors.Clear();
        }

        private void CreateAlertIfAnySlowSqls()
        {
            if (_slowSqlsInCurrentBatch.Statements.Count == 0)
                return;

            _notificationCenter.EtlNotifications.AddSlowSqlWarnings(_processTag, _processName, _slowSqlsInCurrentBatch.Statements);

            _slowSqlsInCurrentBatch.Statements.Clear();
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
            LoadErrors = 0;
            LastChangeVector = null;
            LastAlert = null;
            _transformationErrorsInCurrentBatch.Errors.Clear();
            _loadErrorsInCurrentBatch.Errors.Clear();
            _slowSqlsInCurrentBatch.Statements.Clear();
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
    }
}
