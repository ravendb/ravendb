using System;
using System.Collections.Generic;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json.Parsing;

namespace Raven.Server.Rachis
{
    public enum DatabaseStatus
    {
        Loaded,
        Loading,
        Faulted,
        Unloaded,
    }
    
    public class DatabaseStatusReport : IDynamicJson
    {
        public string Name;
        public string NodeName;

        public ChangeVectorEntry[] LastDocumentChangeVector;

        public ChangeVectorEntry[] LastAttachmentChangeVector;

        // <collection,etag>
        public Dictionary<string,long> LastIndexedDocumentEtag = new Dictionary<string, long>();

        public long LastEtag;

        public DatabaseStatus Status;
        public string FailureToLoad;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastDocumentChangeVector)] = LastDocumentChangeVector?.ToJson(),
                [nameof(LastAttachmentChangeVector)] = LastAttachmentChangeVector?.ToJson(),
                [nameof(LastIndexedDocumentEtag)] = DynamicJsonValue.Convert(LastIndexedDocumentEtag),
                [nameof(LastEtag)] = LastEtag,
                [nameof(Status)] = Status,
                [nameof(Name)] = Name,
                [nameof(NodeName)] = NodeName,              
            };
        }
    }

/*    public class NodeReport : IDynamicJson
    {
        // <db name,report>
        public Dictionary<string, DatabaseStatusReport> ReportPerDatabase = new Dictionary<string, DatabaseStatusReport>();

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                //    [nameof(ClusterTag)] = ClusterTag,
                [nameof(ReportPerDatabase)] = DynamicJsonValue.Convert(ReportPerDatabase),
            };
        }
    }*/

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