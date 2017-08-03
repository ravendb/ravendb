using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.Operations
{
    public class DatabasesInfo
    {
        public List<DatabaseInfo> Databases { get; set; }
    }

    public class BackupInfo : IDynamicJson
    {
        public DateTime? LastBackup { get; set; }

        public int IntervalUntilNextBackupInSec { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastBackup)] = LastBackup,
                [nameof(IntervalUntilNextBackupInSec)] = IntervalUntilNextBackupInSec
            };
        }
    }

    public class DatabaseInfo : IDynamicJson
    {
        public string Name { get; set; }
        public bool Disabled { get; set; }
        public Size TotalSize { get; set; }

        public bool IsAdmin { get; set; }
        public TimeSpan? UpTime { get; set; }
        public BackupInfo BackupInfo { get; set; }

        public long? Alerts { get; set; }
        public bool RejectClients { get; set; }
        public string LoadError { get; set; }
        public long? IndexingErrors { get; set; }

        public long? DocumentsCount { get; set; }
        public bool HasRevisionsConfiguration { get; set; }
        public bool HasExpirationConfiguration { get; set; }
        public int? IndexesCount { get; set; }
        public IndexRunningStatus IndexingStatus { get; set; }

        public NodesTopology NodesTopology { get; set; }
        public int ReplicationFactor { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Disabled)] = Disabled,
                [nameof(TotalSize)] = new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = TotalSize.HumaneSize,
                    [nameof(Size.SizeInBytes)] = TotalSize.SizeInBytes
                },

                [nameof(IsAdmin)] = IsAdmin,
                [nameof(UpTime)] = UpTime?.ToString(),
                [nameof(BackupInfo)] = BackupInfo?.ToJson(),

                [nameof(Alerts)] = Alerts,
                [nameof(RejectClients)] = false,
                [nameof(IndexingErrors)] = IndexingErrors,

                [nameof(DocumentsCount)] = DocumentsCount,
                [nameof(HasRevisionsConfiguration)] = HasRevisionsConfiguration,
                [nameof(HasExpirationConfiguration)] = HasExpirationConfiguration,
                [nameof(IndexesCount)] = IndexesCount,
                [nameof(IndexingStatus)] = IndexingStatus.ToString(),

                [nameof(NodesTopology)] = NodesTopology?.ToJson(),
                [nameof(ReplicationFactor)] = ReplicationFactor
            };
        }
    }

    public class NodesTopology : IDynamicJson
    {
        public List<NodeId> Members { get; set; }
        public List<NodeId> Promotables { get; set; }
        public List<NodeId> Rehabs { get; set; }
        public Dictionary<string, DbGroupNodeStatus> Status { get; set; }

        public NodesTopology()
        {
            Members = new List<NodeId>();
            Promotables = new List<NodeId>();
            Rehabs = new List<NodeId>();
            Status = new Dictionary<string, DbGroupNodeStatus>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Members)] = new DynamicJsonArray(Members.Select(x => x.ToJson())),
                [nameof(Promotables)] = new DynamicJsonArray(Promotables.Select(x => x.ToJson())),
                [nameof(Rehabs)] = new DynamicJsonArray(Rehabs.Select(x => x.ToJson())),
                [nameof(Status)] = DynamicJsonValue.Convert(Status)
            };
        }
    }

    public enum DatabasePromotionStatus
    {
        WaitingForFirstPromotion,
        NotRespondingMovedToRehab,
        IndexNotUpToDate,
        ChangeVectorNotMerged,
        WaitingForResponse,
        Ok
    }

    public class DbGroupNodeStatus : IDynamicJson
    {
        public DatabasePromotionStatus LastStatus;
        public string LastError;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastStatus)] = LastStatus,
                [nameof(LastError)] = LastError
            };
        }
    }

    public class NodeId : IDynamicJson
    {
        public string NodeTag;
        public string NodeUrl;
        public string ResponsibleNode;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(NodeUrl)] = NodeUrl,
                [nameof(ResponsibleNode)] = ResponsibleNode,
            };
        }
    }
}