using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL
{
    public sealed class EtlProcessStatistics : IDynamicJson
    {
        private readonly string _processTag;
        private readonly string _processName;
        private readonly DatabaseNotificationCenter _notificationCenter;
        private readonly EtlConfiguration _etlConfiguration;

        private readonly OnDisposeActions _onDisposeActions;

        private bool _preventFromAddingAlertsToNotificationCenter;
        private readonly List<TaskItemError> _itemErrors;

        public EtlProcessStatistics()
        {
            // for deserialization
        }

        public EtlProcessStatistics(string processTag, string processName, DatabaseNotificationCenter notificationCenter, EtlConfiguration etlConfiguration)
        {
            _processTag = processTag;
            _processName = processName;
            _notificationCenter = notificationCenter;
            LastSlowSqlWarningsInCurrentBatch = new SlowSqlDetails();
            _onDisposeActions = new OnDisposeActions(this);
            _etlConfiguration = etlConfiguration;
            AverageErrorsRatio = new TimeAgnosticEwma();
            HealthStatus = EtlProcessHealthStatus.Healthy;
            _itemErrors = new List<TaskItemError>();
        }

        public string LastChangeVector { get; set; }

        public long LastProcessedEtag { get; set; }

        public DateTime? LastLoadErrorTime { get; private set; }

        public int TransformationErrors { get; private set; }

        public int TransformationSuccesses { get; private set; }

        public int LoadErrors { get; private set; }

        public int LoadSuccesses { get; private set; }

        public int LoadSuccessesInCurrentBatch { get; private set; }

        public SlowSqlDetails LastSlowSqlWarningsInCurrentBatch { get; }

        public bool WasLatestLoadSuccessful { get; set; }
        
        public TimeAgnosticEwma AverageErrorsRatio { get; }
        
        private long BatchErrors { get; set; }
        
        public EtlProcessHealthStatus HealthStatus { get; private set; }
        private bool SetHealthStatusToFailedOnScriptParseError { get; set; }
        public DateTime? NextBatchRetryTime { get; set; }
        public DateTime? LastSuccessfulBatchTime { get; set; }
        public TaskProcessError BatchStopReason { get; internal set; }

        public void TransformationSuccess()
        {
            TransformationSuccesses++;
        }

        public IDisposable NewBatch()
        {
            LastSlowSqlWarningsInCurrentBatch.Statements.Clear();
            
            return _onDisposeActions;
        }

        internal void OnBatchCompletion()
        {
            AverageErrorsRatio.UpdateOnBatchCompletion(BatchErrors, BatchErrors + LoadSuccessesInCurrentBatch);
            UpdateHealthStatusOnBatchCompletion();
            
            ResetBatchStatistics();
            _itemErrors.Clear();
            
            return;
            
            void ResetBatchStatistics()
            {
                LoadSuccessesInCurrentBatch = 0;
                BatchErrors = 0;
            }
        }

        private void UpdateHealthStatusOnBatchCompletion()
        {
            var previousStatus = HealthStatus;
            
            if (SetHealthStatusToFailedOnScriptParseError)
            {
                HealthStatus = EtlProcessHealthStatus.Failed;
            }

            else
            {
                var errorsEwma = AverageErrorsRatio.GetRate();
                
                HealthStatus = errorsEwma switch
                {
                    _ when errorsEwma > _etlConfiguration.ProcessHealthStatusFailedThreshold => EtlProcessHealthStatus.Failed,
                    _ when errorsEwma > _etlConfiguration.ProcessHealthStatusImpairedThreshold => EtlProcessHealthStatus.Impaired,
                    _ => EtlProcessHealthStatus.Healthy
                };
            }
            
            if (HealthStatus != previousStatus)
                _notificationCenter.EtlNotifications.AddTaskHealthChangeNotification(_processTag, _processName, HealthStatus);
        }

        internal void SetProcessHealthStatusToFailedOnScriptParseError()
        {
            SetHealthStatusToFailedOnScriptParseError = true;
        }

        public void RecordItemTransformationError(Exception e, string documentId)
        {
            var now = SystemTime.UtcNow;

            var itemError = new TaskItemError()
            {
                CreatedAt = now,
                TaskName = _processName,
                DocumentId = documentId,
                Step = TaskErrorStep.Transformation,
                Error = e.ToString()
            };

            _itemErrors.Add(itemError);

            TransformationErrors++;
            BatchErrors++;
        }

        public void RecordItemLoadError(string error, string documentId, int count = 1)
        {
            var now = SystemTime.UtcNow;

            var itemError = new TaskItemError()
            {
                CreatedAt = now,
                TaskName = _processName,
                DocumentId = documentId,
                Step = TaskErrorStep.Load,
                Error = error
            };
            
            _itemErrors.Add(itemError);
            WasLatestLoadSuccessful = false;

            LoadErrors += count; 
            BatchErrors += count;
            
            LastLoadErrorTime = now;
        }

        public void RecordProcessLoadError(int count)
        {
            WasLatestLoadSuccessful = false;
            LoadErrors += count;
            BatchErrors += count;
            LastLoadErrorTime = SystemTime.UtcNow;
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

        private void CreateAlertIfAnySlowSqls()
        {
            if (LastSlowSqlWarningsInCurrentBatch.Statements.Count == 0 || _preventFromAddingAlertsToNotificationCenter)
                return;

            _notificationCenter.EtlNotifications.AddSlowSqlWarnings(_processTag, _processName, LastSlowSqlWarningsInCurrentBatch.Statements);

            LastSlowSqlWarningsInCurrentBatch.Statements.Clear();
        }
        
        internal int InMemoryItemErrorsCount => _itemErrors.Count;

        internal List<TaskItemError> ReadInMemoryItemErrors()
        {
            return _itemErrors.ToList();
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(LastLoadErrorTime)] = LastLoadErrorTime,
                [nameof(LastProcessedEtag)] = LastProcessedEtag,
                [nameof(TransformationSuccesses)] = TransformationSuccesses,
                [nameof(TransformationErrors)] = TransformationErrors,
                [nameof(LoadSuccesses)] = LoadSuccesses,
                [nameof(LoadErrors)] = LoadErrors,
                [nameof(AverageErrorsRatio)] = AverageErrorsRatio.GetRate(),
                [nameof(HealthStatus)] = HealthStatus,
                [nameof(NextBatchRetryTime)] = NextBatchRetryTime,
                [nameof(LastSuccessfulBatchTime)] = LastSuccessfulBatchTime,
                [nameof(BatchStopReason)] = BatchStopReason?.ToJson(),
            };
            return json;
        }

        public override string ToString()
        {
            return $"{nameof(LastProcessedEtag)}: {LastProcessedEtag} " +
                   $"{nameof(LastLoadErrorTime)}: {LastLoadErrorTime} " +
                   $"{nameof(TransformationSuccesses)}: {TransformationSuccesses} " +
                   $"{nameof(TransformationErrors)}: {TransformationErrors} " +
                   $"{nameof(LoadSuccesses)}: {LoadSuccesses} " +
                   $"{nameof(LoadErrors)}: {LoadErrors}";
        }

        public void Reset()
        {
            LastProcessedEtag = 0;
            LastLoadErrorTime = null;
            TransformationSuccesses = 0;
            TransformationErrors = 0;
            LoadSuccesses = 0;
            LoadSuccessesInCurrentBatch = 0;
            LoadErrors = 0;
            BatchErrors = 0;
            LastChangeVector = null;
            LastSlowSqlWarningsInCurrentBatch.Statements.Clear();
            AverageErrorsRatio.Reset();
            HealthStatus = EtlProcessHealthStatus.Healthy;
            SetHealthStatusToFailedOnScriptParseError = false;
        }

        private sealed class OnDisposeActions : IDisposable
        {
            private readonly EtlProcessStatistics _parent;

            public OnDisposeActions(EtlProcessStatistics parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                _parent.CreateAlertIfAnySlowSqls();
            }
        }

        public IDisposable PreventFromAddingAlertsToNotificationCenter()
        {
            _preventFromAddingAlertsToNotificationCenter = true;

            return new DisposableAction(() => _preventFromAddingAlertsToNotificationCenter = false);
        }
    }
}
