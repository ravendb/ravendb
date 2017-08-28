using System;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL
{
    public class EtlProcessStatistics
    {
        private readonly string _processType;
        private readonly string _name;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;

        public EtlProcessStatistics(string processType, string name, NotificationCenter.NotificationCenter notificationCenter)
        {
            _processType = processType;
            _name = name;
            _notificationCenter = notificationCenter;
        }

        public string LastChangeVector { get; set; }

        public long LastProcessedEtag { get; set; }

        public DateTime? LastErrorTime { get; private set; }

        private int TransformationErrors { get; set; }

        private int TransformationSuccesses { get; set; }

        private int LoadErrors { get; set; }

        public int LoadSuccesses { get; private set; }

        public AlertRaised LastAlert { get; set; }

        public void TransformationSuccess()
        {
            TransformationSuccesses++;
        }

        public void RecordTransformationError(Exception e)
        {
            TransformationErrors++;

            LastErrorTime = SystemTime.UtcNow;

            LastAlert = AlertRaised.Create(_processType,
                $"[{_name}] Transformation script failed",
                AlertType.Etl_TransformationError,
                NotificationSeverity.Warning,
                key: _name,
                details: new ExceptionDetails(e));

            if (TransformationErrors < 100)
                return;

            if (TransformationErrors <= TransformationSuccesses)
                return;

            var message = $"[{_name}] Transformation errors ratio too high. " +
                          "Could not tolerate transformation script error ratio and stopped current ETL cycle";

            LastAlert = AlertRaised.Create(_processType,
                message,
                AlertType.Etl_TransformationError,
                NotificationSeverity.Error,
                key: _name,
                details: new ExceptionDetails(e));

            _notificationCenter.Add(LastAlert);

            throw new InvalidOperationException($"{message}. Current stats: {this}");
        }

        public void RecordLoadError(Exception e, int count = 1)
        {
            LoadErrors += count;

            LastErrorTime = SystemTime.UtcNow;

            LastAlert = AlertRaised.Create(_processType,
                $"[{_name}] Write error: {e.Message}",
                AlertType.Etl_LoadError,
                NotificationSeverity.Error,
                key: _name, details: new ExceptionDetails(e));

            if (LoadErrors < 100)
                return;

            if (LoadErrors <= LoadSuccesses)
                return;

            var message = $"[{_name}] Write error hit ratio too high. Could not tolerate write error ratio and stopped current ETL cycle";

            LastAlert = AlertRaised.Create(_processType,
                message,
                AlertType.Etl_WriteErrorRatio,
                NotificationSeverity.Error,
                key: _name,
                details: new ExceptionDetails(e));

            _notificationCenter.Add(LastAlert);

            throw new InvalidOperationException($"{message}. Current stats: {this}", e);
        }

        public void LoadSuccess(int items)
        {
            LoadSuccesses += items;
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(LastAlert)] = LastAlert?.ToJson(),
                [nameof(LastErrorTime)] = LastErrorTime,
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
                   $"{nameof(LastErrorTime)}: {LastErrorTime} " +
                   $"{nameof(TransformationSuccesses)}: {TransformationSuccesses} " +
                   $"{nameof(TransformationErrors)}: {TransformationErrors} " +
                   $"{nameof(LoadSuccesses)}: {LoadSuccesses} " +
                   $"{nameof(LoadErrors)}: {LoadErrors}";
        }
    }
}