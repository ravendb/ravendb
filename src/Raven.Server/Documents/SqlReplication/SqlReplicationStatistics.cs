using System;
using Raven.Abstractions;

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
                IsError = true,
                CreatedAt = SystemTime.UtcNow,
                Message = "Last SQL replication operation for " + _name + " was failed",
                Title = "SQL replication error",
                Exception = e.ToString(),
                UniqueKey = "Sql Replication Error: " + _name
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
                IsError = true,
                CreatedAt = SystemTime.UtcNow,
                Message = "Could not tolerate write error ratio and stopped current replication cycle for " + _name + Environment.NewLine + this,
                Title = "Sql Replication write error hit ratio too high",
                Exception = e.ToString(),
                UniqueKey = "Sql Replication Write Error Ratio: " + _name
            };

            if (_reportToDatabaseAlerts)
            {
                database.AddAlert(LastAlert);
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
                IsError = true,
                CreatedAt = SystemTime.UtcNow,
                Message = string.Format("Could not parse script for {0} " + Environment.NewLine + "Script: {1}", _name, script),
                Title = "Could not parse Script",
                UniqueKey = "Script Parse Error: " + _name
            };
            if (_reportToDatabaseAlerts)
            {
                database.AddAlert(LastAlert);
            }
        }

        public void RecordScriptError(DocumentDatabase database, Exception e)
        {
            ScriptErrorCount++;

            LastErrorTime = SystemTime.UtcNow;

            LastAlert = new Alert
            {
                IsError = true,
                CreatedAt = SystemTime.UtcNow,
                Message = "Replication script for " + _name + " was failed",
                Title = "SQL replication error",
                Exception = e.ToString(),
                UniqueKey = "Sql Replication Error: " + _name
            };

            if (ScriptErrorCount < 100)
                return;

            if (ScriptErrorCount <= ScriptSuccessCount)
                return;

            LastAlert = new Alert
            {
                IsError = true,
                CreatedAt = SystemTime.UtcNow,
                Message = "Could not tolerate script error ratio and stopped current replication cycle for " + _name + Environment.NewLine + this,
                Title = "Sql Replication script error hit ratio too high",
                UniqueKey = "Sql Replication Script Error Ratio: " + _name
            };
            if (_reportToDatabaseAlerts)
            {
                database.AddAlert(LastAlert);
            }

            throw new InvalidOperationException("Could not tolerate script error ratio and stopped current replication cycle for " + _name + Environment.NewLine + this);
        }

        public void ScriptSuccess()
        {
            ScriptSuccessCount++;
        }
    }
}