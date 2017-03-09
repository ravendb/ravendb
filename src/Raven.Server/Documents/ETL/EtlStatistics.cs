using System;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL
{
    public class EtlStatistics
    {
        private readonly string _processType;
        private readonly string _name;
        private readonly NotificationCenter.NotificationCenter _notificationCenter;

        public EtlStatistics(string processType, string name, NotificationCenter.NotificationCenter notificationCenter)
        {
            _processType = processType;
            _name = name;
            _notificationCenter = notificationCenter;
        }

        public long LastProcessedEtag { get; set; }

        public DateTime? LastErrorTime { get; private set; }
        public DateTime? SuspendUntil { get; private set; }

        private int ScriptErrorCount { get; set; }
        private int TransformationSuccessCount { get; set; }
        private int WriteErrorCount { get; set; }
        public int SuccessCount { get; private set; }

        public AlertRaised LastAlert { get; set; }

        public void TransformationSuccess()
        {
            TransformationSuccessCount++;
        }

        public void MarkTransformationScriptAsInvalid(string script)
        {
            ScriptErrorCount = int.MaxValue;
            LastErrorTime = SystemTime.UtcNow;
            SuspendUntil = DateTime.MaxValue;

            LastAlert = AlertRaised.Create(_processType,
                $"[{_name}] Could not parse script",
                AlertType.SqlReplication_ScriptError,
                NotificationSeverity.Error,
                key: _name,
                details: new MessageDetails
                {
                    Message = $"Script:{Environment.NewLine}{script}"
                });

            _notificationCenter.Add(LastAlert);
        }

        public void RecordScriptError(Exception e)
        {
            ScriptErrorCount++;

            LastErrorTime = SystemTime.UtcNow;

            LastAlert = AlertRaised.Create(_processType,
                $"[{_name}] Transformation script failed",
                AlertType.SqlReplication_Error,
                NotificationSeverity.Error,
                key: _name,
                details: new ExceptionDetails(e));

            if (ScriptErrorCount < 100)
                return;

            if (ScriptErrorCount <= TransformationSuccessCount)
                return;

            LastAlert = AlertRaised.Create(_processType,
                $"[{_name}] Script error hit ratio too high. Could not tolerate script error ratio and stopped current replication cycle",
                AlertType.SqlReplication_ScriptErrorRatio,
                NotificationSeverity.Error,
                key: _name,
                details: new ExceptionDetails(e));

            _notificationCenter.Add(LastAlert);

            throw new InvalidOperationException("Could not tolerate script error ratio and stopped current transformation cycle for " + _name + Environment.NewLine + this);
        }

        public void RecordWriteError(Exception e, int count = 1, DateTime? suspendUntil = null)
        {
            WriteErrorCount += count;

            LastErrorTime = SystemTime.UtcNow;

            LastAlert = AlertRaised.Create(_processType,
                $"[{_name}] Write error: {e.Message}",
                AlertType.SqlReplication_Error,
                NotificationSeverity.Error,
                key: _name, details: new ExceptionDetails(e));

            if (WriteErrorCount < 100)
                return;

            if (WriteErrorCount <= SuccessCount)
                return;

            if (suspendUntil.HasValue)
            {
                SuspendUntil = suspendUntil.Value;
                return;
            }

            LastAlert = AlertRaised.Create(_processType,
                $"[{_name}] Write error hit ratio too high. Could not tolerate write error ratio and stopped current replication cycle",
                AlertType.SqlReplication_WriteErrorRatio,
                NotificationSeverity.Error,
                key: _name,
                details: new ExceptionDetails(e));

            _notificationCenter.Add(LastAlert);

            throw new InvalidOperationException("Could not tolerate write error ratio and stopped current replication cycle for " + _name + Environment.NewLine + this, e);
        }

        public void Success(int countOfItems)
        {
            LastErrorTime = null;
            SuspendUntil = null;
            SuccessCount += countOfItems;
        }

        public void CompleteSuccess(int countOfItems)
        {
            Success(countOfItems);
            WriteErrorCount /= 2;
            ScriptErrorCount /= 2;
        }

        public DynamicJsonValue ToBlittable()
        {
            var json = new DynamicJsonValue
            {
                [nameof(LastAlert)] = LastAlert?.ToJson(),
                [nameof(LastErrorTime)] = LastErrorTime,
                [nameof(LastProcessedEtag)] = LastProcessedEtag,
                [nameof(SuccessCount)] = SuccessCount,
                [nameof(SuspendUntil)] = SuspendUntil,
            };
            return json;
        }
    }
}