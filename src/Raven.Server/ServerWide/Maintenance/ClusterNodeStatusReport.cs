using System;
using System.Collections.Generic;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Maintenance
{
    public enum DatabaseStatus
    {
        Loaded,
        Loading,
        Faulted,
        Unloaded,
        Shutdown,
    }

    public class DatabaseStatusReport : IDynamicJson
    {
        public string Name;
        public string NodeName;

        public ChangeVectorEntry[] LastDocumentChangeVector;

        public readonly Dictionary<string, ObservedIndexStatus> LastIndexStats = new Dictionary<string, ObservedIndexStatus>();

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
                [nameof(LastDocumentChangeVector)] = LastDocumentChangeVector?.ToJson(),
                [nameof(Error)] = Error,
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

        public readonly Dictionary<string, DatabaseStatusReport> LastReport;

        public readonly ReportStatus LastReportStatus;

        public readonly Exception LastError;

        public readonly DateTime LastUpdateDateTime;

        public readonly DateTime LastSuccessfulUpdateDateTime;

        public ClusterNodeStatusReport(Dictionary<string, DatabaseStatusReport> lastReport, ReportStatus lastReportStatus, Exception lastError, DateTime lastUpdateDateTime, DateTime lastSuccessfulUpdateDateTime)
        {
            LastReport = lastReport;
            LastReportStatus = lastReportStatus;
            LastError = lastError;
            LastUpdateDateTime = lastUpdateDateTime;
            LastSuccessfulUpdateDateTime = lastSuccessfulUpdateDateTime;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastReport)] = DynamicJsonValue.Convert(LastReport),
                [nameof(LastReportStatus)] = LastReportStatus,
                [nameof(LastError)] = LastError,
                [nameof(LastUpdateDateTime)] = LastUpdateDateTime,
                [nameof(LastSuccessfulUpdateDateTime)] = LastSuccessfulUpdateDateTime,
            };
        }
    }
}