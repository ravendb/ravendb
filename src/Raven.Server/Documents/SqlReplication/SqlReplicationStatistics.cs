using System;
using Raven.Client;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
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

        public AlertRaised LastAlert { get; set; }

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

            LastAlert = AlertRaised.Create(SqlReplication.AlertTitle, 
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

            LastAlert = AlertRaised.Create(SqlReplication.AlertTitle,
                $"[{_name}] Write error hit ratio too high. Could not tolerate write error ratio and stopped current replication cycle",
                AlertType.SqlReplication_WriteErrorRatio,
                NotificationSeverity.Error,
                key: _name, 
                details: new ExceptionDetails(e));

            if (_reportToDatabaseAlerts)
            {
                database.NotificationCenter.Add(LastAlert);
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

            LastAlert = AlertRaised.Create(SqlReplication.AlertTitle,
                $"[{_name}] Could not parse script",
                AlertType.SqlReplication_ScriptError,
                NotificationSeverity.Error,
                key: _name, 
                details: new MessageDetails
                {
                    Message = $"Script:{Environment.NewLine}{script}"
                });

            if (_reportToDatabaseAlerts)
            {
                database.NotificationCenter.Add(LastAlert);
            }
        }

        public void RecordScriptError(DocumentDatabase database, Exception e)
        {
            ScriptErrorCount++;

            LastErrorTime = SystemTime.UtcNow;

            LastAlert = AlertRaised.Create(SqlReplication.AlertTitle,
                $"[{_name}] Replication script failed",
                AlertType.SqlReplication_Error, 
                NotificationSeverity.Error,
                key: _name, 
                details: new ExceptionDetails(e));

            if (ScriptErrorCount < 100)
                return;

            if (ScriptErrorCount <= ScriptSuccessCount)
                return;

            LastAlert = AlertRaised.Create(SqlReplication.AlertTitle,
                $"[{_name}] Script error hit ratio too high. Could not tolerate script error ratio and stopped current replication cycle",
                AlertType.SqlReplication_ScriptErrorRatio,
                NotificationSeverity.Error,
                key: _name, 
                details: new ExceptionDetails(e));

            if (_reportToDatabaseAlerts)
            {
                database.NotificationCenter.Add(LastAlert);
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