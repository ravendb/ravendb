using System;
using Raven.Abstractions;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationStatistics
    {
        private readonly string _name;
        private readonly bool _reportToDatabaseAlerts;

        public SqlReplicationStatistics(string name, bool reportToDatabaseAlerts = true)
        {
            _name = name;
            _reportToDatabaseAlerts = reportToDatabaseAlerts;
        }

        public DateTime? LastErrorTime { get; private set; }
        public DateTime? SuspendUntil { get; private set; }

        private int ScriptErrorCount { get; set; }
        private int ScriptSuccessCount { get; set; }
        private int WriteErrorCount { get; set; }
        public int SuccessCount { get; private set; }

        public Alert LastAlert { get; set; }

        public long LastReplicatedEtag { get; set; }
        public long LastTombstonesEtag { get; set; }

        public void Success(int countOfItems)
        {
            LastErrorTime = null;
            SuspendUntil = null;
            SuccessCount += countOfItems;
        }

        public void RecordWriteError(Exception e, DocumentDatabase database, int count = 1, DateTime? suspendUntil = null)
        {
            WriteErrorCount += count;

            LastErrorTime = SystemTime.UtcNow;

            LastAlert = new Alert
            {
                Type = AlertType.SqlReplicationError,
                Severity = AlertSeverity.Error,
                CreatedAt = SystemTime.UtcNow,
                Message = "SQL replication error",
                Key = _name,
                Content = new ExceptionAlertContent
                {
                    Message = "Last SQL replication operation for " + _name + " was failed",
                    Exception = e.ToString()
                }
            };

            if (WriteErrorCount < 100)
                return;

            if (WriteErrorCount <= SuccessCount)
                return;

            if (suspendUntil.HasValue)
            {
                SuspendUntil = suspendUntil.Value;
                return;
            }

            LastAlert = new Alert
            {
                Type = AlertType.SqlReplicationWriteErrorRatio, 
                Severity = AlertSeverity.Error,
                CreatedAt = SystemTime.UtcNow,
                Message = "Sql Replication write error hit ratio too high",
                Key = _name,
                Content = new ExceptionAlertContent
                {
                    Message = "Could not tolerate write error ratio and stopped current replication cycle for " + _name + Environment.NewLine + this,
                    Exception = e.ToString()
                }
            };

            if (_reportToDatabaseAlerts)
            {
                database.Alerts.AddAlert(LastAlert);
            }

            throw new InvalidOperationException("Could not tolerate write error ratio and stopped current replication cycle for " + _name + Environment.NewLine + this, e);
        }

        public override string ToString()
        {
            return $"LastErrorTime: {LastErrorTime}, SuspendUntil: {SuspendUntil}, ScriptErrorCount: {ScriptErrorCount}, WriteErrorCount: {WriteErrorCount}, SuccessCount: {SuccessCount}";
        }

        public void CompleteSuccess(int countOfItems)
        {
            Success(countOfItems);
            WriteErrorCount /= 2;
            ScriptErrorCount /= 2;
        }

        public void MarkScriptAsInvalid(DocumentDatabase database, string script)
        {
            ScriptErrorCount = int.MaxValue;
            LastErrorTime = SystemTime.UtcNow;
            SuspendUntil = DateTime.MaxValue;
            LastAlert = new Alert
            {
                Type = AlertType.SqlReplicationScriptError,
                Severity = AlertSeverity.Error,
                CreatedAt = SystemTime.UtcNow,
                Key = _name,
                Message = "Could not parse Script",
                Content = new ExceptionAlertContent
                {
                    Message = string.Format("Could not parse script for {0} " + Environment.NewLine + "Script: {1}", _name, script),
                }
            };
            if (_reportToDatabaseAlerts)
            {
                database.Alerts.AddAlert(LastAlert);
            }
        }

        public void RecordScriptError(DocumentDatabase database, Exception e)
        {
            ScriptErrorCount++;

            LastErrorTime = SystemTime.UtcNow;

            LastAlert = new Alert
            {
                Type = AlertType.SqlReplicationError,
                Severity = AlertSeverity.Error,
                CreatedAt = SystemTime.UtcNow,
                Message = "SQL replication error",
                Key = _name,
                Content = new ExceptionAlertContent
                {
                    Message = "Replication script for " + _name + " was failed",
                    Exception = e.ToString()
                }
            };

            if (ScriptErrorCount < 100)
                return;

            if (ScriptErrorCount <= ScriptSuccessCount)
                return;

            LastAlert = new Alert
            {
                Type = AlertType.SqlReplicationScriptErrorRatio,
                Severity = AlertSeverity.Error,
                CreatedAt = SystemTime.UtcNow,
                Message = "Sql Replication script error hit ratio too high",
                Key = _name,
                Content = new ExceptionAlertContent
                {
                    Message = "Could not tolerate script error ratio and stopped current replication cycle for " + _name + Environment.NewLine + this,
                }
            };
            if (_reportToDatabaseAlerts)
            {
                database.Alerts.AddAlert(LastAlert);
            }

            throw new InvalidOperationException("Could not tolerate script error ratio and stopped current replication cycle for " + _name + Environment.NewLine + this);
        }

        public void ScriptSuccess()
        {
            ScriptSuccessCount++;
        }

        public DynamicJsonValue ToBlittable()
        {
            var json = new DynamicJsonValue
            {
                ["LastAlert"] = LastAlert?.ToJson(),
                ["LastErrorTime"] = LastErrorTime,
                ["LastReplicatedEtag"] = LastReplicatedEtag,
                ["LastTombstonesEtag"] = LastTombstonesEtag,
                ["SuccessCount"] = SuccessCount,
                ["SuspendUntil"] = SuspendUntil,
            };
            return json;
        }
    }
}