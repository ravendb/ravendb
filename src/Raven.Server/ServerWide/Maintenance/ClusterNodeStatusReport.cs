using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.ServerWide.Maintenance.Sharding;
using Sparrow;
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

    public sealed class MaintenanceReport : IDynamicJson
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

    public sealed class ServerReport : IDynamicJson
    {
        public bool? OutOfCpuCredits;

        public bool? EarlyOutOfMemory;

        public bool? HighDirtyMemory;
        
        public long? LastCommittedIndex;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(OutOfCpuCredits)] = OutOfCpuCredits,
                [nameof(EarlyOutOfMemory)] = EarlyOutOfMemory,
                [nameof(HighDirtyMemory)] = HighDirtyMemory,
                [nameof(LastCommittedIndex)] = LastCommittedIndex
            };
        }
    }

    public sealed class DatabaseStatusReport : IDynamicJson
    {
        public string Name;
        public string NodeName;

        public string DatabaseChangeVector;

        public Dictionary<string, ObservedIndexStatus> LastIndexStats = new();
        public Dictionary<string, long> LastSentEtag = new();
        public Dictionary<int, BucketReport> ReportPerBucket = new();
        public Dictionary<long, PeriodicBackupStatusReport> BackupStatuses;

        public long LastCompareExchangeIndex { get; set; }
        public long LastClusterWideTransactionRaftIndex { get; set; }

        public DatabaseStatusReport()
        {
        }

        /// <summary>
        /// This is a Shallow Copy Ctor
        /// </summary>
        public DatabaseStatusReport([NotNull] DatabaseStatusReport other)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));

            Name = other.Name;
            NodeName = other.NodeName;
            DatabaseChangeVector = other.DatabaseChangeVector;

            // shallow
            LastIndexStats = other.LastIndexStats;
            LastSentEtag = other.LastSentEtag;
            ReportPerBucket = other.ReportPerBucket;

            LastCompareExchangeIndex = other.LastCompareExchangeIndex;
            LastClusterWideTransactionRaftIndex = other.LastClusterWideTransactionRaftIndex;
            LastEtag = other.LastEtag;
            LastTombstoneEtag = other.LastTombstoneEtag;
            NumberOfConflicts = other.NumberOfConflicts;
            NumberOfDocuments = other.NumberOfDocuments;
            LastCompletedClusterTransaction = other.LastCompletedClusterTransaction;
            Status = other.Status;
            Error = other.Error;
            UpTime = other.UpTime;
            LastTransactionId = other.LastTransactionId;
            EnvironmentsHash = other.EnvironmentsHash;
        }

        public sealed class ObservedIndexStatus
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

        internal static long GetPeriodicBackupStatusesHash(Dictionary<long, PeriodicBackupStatusReport> periodicBackupStatusReports)
        {
            long hash = 0;

            if (periodicBackupStatusReports == null)
                return hash;

            foreach ((long taskId, PeriodicBackupStatusReport backupStatusReport) in periodicBackupStatusReports)
            {
                hash = Hashing.Combine(hash, taskId);
                hash = Hashing.Combine(hash, backupStatusReport?.LastRaftIndexEtag ?? 0);
            }

            return hash;
        }

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
                [nameof(ReportPerBucket)] = DynamicJsonValue.Convert(ReportPerBucket),
                [nameof(Error)] = Error,
                [nameof(UpTime)] = UpTime,
                [nameof(LastCompareExchangeIndex)] = LastCompareExchangeIndex,
                [nameof(LastClusterWideTransactionRaftIndex)] = LastClusterWideTransactionRaftIndex
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

            if (BackupStatuses != null)
            {
                var backupStatuses = new DynamicJsonValue();

                foreach ((long taskId, PeriodicBackupStatusReport backupStatusReport) in BackupStatuses)
                    backupStatuses[taskId.ToString()] = backupStatusReport?.ToJson();

                dynamicJsonValue[nameof(BackupStatuses)] = backupStatuses;
            }
            else
            {
                dynamicJsonValue[nameof(BackupStatuses)] = null;
            }

            return dynamicJsonValue;
        }
    }

    public sealed class ClusterNodeStatusReport : IDynamicJson
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
            Dictionary<string, DatabaseStatusReport> databaseStatusReports,
            ReportStatus reportStatus, 
            Exception error, 
            DateTime updateDateTime, 
            ClusterNodeStatusReport lastSuccessfulReport)
        {
            ServerReport = serverReport;
            Report = databaseStatusReports;
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
            foreach ((string dbName, DatabaseStatusReport databaseStatusReport) in databaseStatusReports)
            {
                var dbStatus = databaseStatusReport.Status;

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
