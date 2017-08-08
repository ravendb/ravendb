using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Maintenance
{
    [Flags]
    public enum DatabaseStatus
    {
        Loaded = 1,
        Loading = 2,
        Faulted = 4,
        Unloaded = 8,
        Shutdown = 16
    }

    public class DatabaseStatusReport : IDynamicJson
    {
        public string Name;
        public string NodeName;

        public string LastChangeVector;

        public Dictionary<string, ObservedIndexStatus> LastIndexStats = new Dictionary<string, ObservedIndexStatus>();

        public class ObservedIndexStatus
        {
            public bool IsSideBySide;
            public long LastIndexedEtag;
            public bool IsStale;
        }

        public long LastEtag;
        public long LastTombstoneEtag;
        public long NumberOfConflicts;

        public DatabaseStatus Status;
        public string Error;

        public DynamicJsonValue ToJson()
        {
            var dynamicJsonValue = new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(NodeName)] = NodeName,
                [nameof(Status)] = Status,
                [nameof(LastEtag)] = LastEtag,
                [nameof(LastTombstoneEtag)] = LastTombstoneEtag,
                [nameof(NumberOfConflicts)] = NumberOfConflicts,
                [nameof(LastChangeVector)] = LastChangeVector,
                [nameof(Error)] = Error
            };
            var indexStats = new DynamicJsonValue();

            foreach (var stat in LastIndexStats)
            {
                indexStats[stat.Key] = new DynamicJsonValue
                {
                    [nameof(stat.Value.LastIndexedEtag)] = stat.Value.LastIndexedEtag,
                    [nameof(stat.Value.IsSideBySide)] = stat.Value.IsSideBySide,
                    [nameof(stat.Value.IsStale)] = stat.Value.IsStale
                };
            }

            dynamicJsonValue[nameof(LastIndexStats)] = indexStats;
            return dynamicJsonValue;
        }
    }

    public class ClusterNodeStatusReport : IDynamicJson
    {
        // public string ClusterTag { get; set; }

        public enum ReportStatus
        {
            WaitingForResponse,
            Timeout,
            Error,
            Ok
        }

        public readonly Dictionary<string, DatabaseStatusReport> Report;

        public readonly Dictionary<string, DateTime> LastGoodDatabaseStatus;

        public readonly ReportStatus Status;

        public readonly Exception Error;

        public readonly DateTime UpdateDateTime;
        private readonly ClusterNodeStatusReport _lastSuccessfulReport;

        public DateTime LastSuccessfulUpdateDateTime => _lastSuccessfulReport?.UpdateDateTime ?? DateTime.MinValue;

        public ClusterNodeStatusReport(
            Dictionary<string, DatabaseStatusReport> report, 
            ReportStatus reportStatus, 
            Exception error, 
            DateTime updateDateTime, 
            ClusterNodeStatusReport lastSuccessfulReport)
        {
            Report = report;
            Status = reportStatus;
            Error = error;
            UpdateDateTime = updateDateTime;
            _lastSuccessfulReport = lastSuccessfulReport;
            LastGoodDatabaseStatus = new Dictionary<string, DateTime>();
            foreach (var dbReport in report)
            {
                var dbName = dbReport.Key;
                var dbStatus = dbReport.Value.Status;

                if (reportStatus != ReportStatus.Ok || dbStatus != DatabaseStatus.Loaded)
                { 
                    SetLastDbGoodTime(lastSuccessfulReport, dbName);
                }
                else
                {
                    LastGoodDatabaseStatus[dbName] = updateDateTime;
                }
            }
        }

        private void SetLastDbGoodTime(ClusterNodeStatusReport lastSuccessfulReport, string dbName)
        {
            DateTime lastGood = DateTime.MinValue;
            lastSuccessfulReport?.LastGoodDatabaseStatus.TryGetValue(dbName, out lastGood);
            LastGoodDatabaseStatus[dbName] = lastGood;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Report)] = DynamicJsonValue.Convert(Report),
                [nameof(LastGoodDatabaseStatus)] = DynamicJsonValue.Convert(LastGoodDatabaseStatus),
                [nameof(Status)] = Status,
                [nameof(Error)] = Error,
                [nameof(UpdateDateTime)] = UpdateDateTime,
                [nameof(LastSuccessfulUpdateDateTime)] = LastSuccessfulUpdateDateTime
            };
        }
    }
}