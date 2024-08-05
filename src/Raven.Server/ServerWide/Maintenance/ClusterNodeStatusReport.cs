using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Maintenance
{
    [Flags]
    public enum DatabaseStatus
    {
        None = 0,
        Loaded = 1,
        Loading = 2,
        Faulted = 4,
        Unloaded = 8,
        Shutdown = 16,
        NoChange = 32
    }

    public class MaintenanceReport : IDynamicJson
    {
        public ServerReport ServerReport;
        public Dictionary<string, DatabaseStatusReport> DatabasesReport;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ServerReport)] = ServerReport,
                [nameof(DatabasesReport)] = DynamicJsonValue.Convert(DatabasesReport)
            };
        }
    }

    public class ServerReport : IDynamicJson
    {
        public bool? OutOfCpuCredits;

        public bool? EarlyOutOfMemory;

        public bool? HighDirtyMemory;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(OutOfCpuCredits)] = OutOfCpuCredits,
                [nameof(EarlyOutOfMemory)] = EarlyOutOfMemory,
                [nameof(HighDirtyMemory)] = HighDirtyMemory
            };
        }
    }

    public class DatabaseStatusReport : IDynamicJson
    {
        public string Name;
        public string NodeName;

        public string DatabaseChangeVector;

        public Dictionary<string, ObservedIndexStatus> LastIndexStats = new Dictionary<string, ObservedIndexStatus>();
        public Dictionary<string, long> LastSentEtag = new Dictionary<string, long>();

        public long LastCompareExchangeIndex { get; set; }

        public class ObservedIndexStatus
        {
            public bool IsSideBySide;
            public long LastIndexedEtag;
            public long? LastIndexedCompareExchangeReferenceEtag;
            public long? LastIndexedCompareExchangeReferenceTombstoneEtag;
            public TimeSpan? LastQueried;
            public bool IsStale;
            public IndexState State;
            public long? LastTransactionId; // this is local, so we don't serialize it
        }

        public long LastEtag;
        public long LastTombstoneEtag;
        public long NumberOfConflicts;
        public long NumberOfDocuments;
        public long LastCompletedClusterTransaction;

        public DatabaseStatus Status;
        public string Error;
        public TimeSpan? UpTime;

        public long LastTransactionId; // this is local, so we don't serialize it
        public long EnvironmentsHash; // this is local, so we don't serialize it

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
                [nameof(NumberOfDocuments)] = NumberOfDocuments,
                [nameof(DatabaseChangeVector)] = DatabaseChangeVector,
                [nameof(LastCompletedClusterTransaction)] = LastCompletedClusterTransaction,
                [nameof(LastSentEtag)] = DynamicJsonValue.Convert(LastSentEtag),
                [nameof(Error)] = Error,
                [nameof(UpTime)] = UpTime,
                [nameof(LastCompareExchangeIndex)] = LastCompareExchangeIndex
            };
            var indexStats = new DynamicJsonValue();
            foreach (var stat in LastIndexStats)
            {
                indexStats[stat.Key] = new DynamicJsonValue
                {
                    [nameof(stat.Value.LastIndexedEtag)] = stat.Value.LastIndexedEtag,
                    [nameof(stat.Value.LastIndexedCompareExchangeReferenceEtag)] = stat.Value.LastIndexedCompareExchangeReferenceEtag,
                    [nameof(stat.Value.LastIndexedCompareExchangeReferenceTombstoneEtag)] = stat.Value.LastIndexedCompareExchangeReferenceTombstoneEtag,
                    [nameof(stat.Value.LastQueried)] = stat.Value.LastQueried,
                    [nameof(stat.Value.IsSideBySide)] = stat.Value.IsSideBySide,
                    [nameof(stat.Value.IsStale)] = stat.Value.IsStale,
                    [nameof(stat.Value.State)] = stat.Value.State
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
            Ok,
            OutOfCredits,
            EarlyOutOfMemory,
            HighDirtyMemory
        }

        public readonly Dictionary<string, DatabaseStatusReport> Report;

        public readonly ServerReport ServerReport;

        public readonly Dictionary<string, DateTime> LastGoodDatabaseStatus;

        public readonly ReportStatus Status;

        public readonly Exception Error;

        public readonly DateTime UpdateDateTime;

        public readonly DateTime LastSuccessfulUpdateDateTime;

        public ClusterNodeStatusReport(
            ServerReport serverReport,
            Dictionary<string, DatabaseStatusReport> report,
            ReportStatus reportStatus, 
            Exception error, 
            DateTime updateDateTime, 
            ClusterNodeStatusReport lastSuccessfulReport)
        {
            ServerReport = serverReport;
            Report = report;
            Status = reportStatus;
            Error = error;
            UpdateDateTime = updateDateTime;

            if (ServerReport.OutOfCpuCredits ?? ServerReport.EarlyOutOfMemory ?? ServerReport.HighDirtyMemory ?? false)
            {
                // we don't want to give any grace time if the node is out of credits, early out of memory or high dirty memory
                LastSuccessfulUpdateDateTime = DateTime.MinValue;
            }
            else
            {
                LastSuccessfulUpdateDateTime = lastSuccessfulReport?.UpdateDateTime ?? DateTime.MinValue;
            }

            LastGoodDatabaseStatus = new Dictionary<string, DateTime>();
            foreach (var dbReport in report)
            {
                var dbName = dbReport.Key;
                var dbStatus = dbReport.Value.Status;

                if (reportStatus == ReportStatus.Ok && 
                    (dbStatus == DatabaseStatus.Loaded || dbStatus == DatabaseStatus.NoChange))
                {
                    LastGoodDatabaseStatus[dbName] = updateDateTime;
                }
                else
                {
                    SetLastDbGoodTime(lastSuccessfulReport, dbName);
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
                [nameof(Error)] = Error?.ToString(),
                [nameof(UpdateDateTime)] = UpdateDateTime,
                [nameof(LastSuccessfulUpdateDateTime)] = LastSuccessfulUpdateDateTime
            };
        }
    }
}
