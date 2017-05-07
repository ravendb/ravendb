using System;
using System.Collections.Generic;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Maintance
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

        // <index name,etag diff>
        public Dictionary<string,long> LastIndexedDocumentEtag = new Dictionary<string, long>();

        public long LastEtag;
        public long LastTombstoneEtag;
        public long NumberOfConflicts;

        public DatabaseStatus Status;
        public string FailureToLoad;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(NodeName)] = NodeName,
                [nameof(Status)] = Status,
                [nameof(LastEtag)] = LastEtag,
                [nameof(LastTombstoneEtag)] = LastTombstoneEtag,
                [nameof(NumberOfConflicts)] = NumberOfConflicts,
                [nameof(LastDocumentChangeVector)] = LastDocumentChangeVector?.ToJson(),
                [nameof(LastIndexedDocumentEtag)] = DynamicJsonValue.Convert(LastIndexedDocumentEtag),
                [nameof(FailureToLoad)] = FailureToLoad,              
            };
        }
    }

    public class ClusterNodeStatusReport
    {
        // public string ClusterTag { get; set; }

        public enum ReportStatus
        {
            WaitingForResponse,
            Timeout,
            Error,
            Ok
        }
    
        public readonly Dictionary<string,DatabaseStatusReport> LastReport;

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
    }
}