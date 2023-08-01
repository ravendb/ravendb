using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations
{
    public sealed class DatabasesInfo
    {
        public List<DatabaseInfo> Databases { get; set; }
    }

    public sealed class BackupInfo : IDynamicJson
    {
        public DateTime? LastBackup { get; set; }

        public double IntervalUntilNextBackupInSec { get; set; }

        public BackupTaskType BackupTaskType { get; set; }

        public List<string> Destinations { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LastBackup)] = LastBackup,
                [nameof(IntervalUntilNextBackupInSec)] = IntervalUntilNextBackupInSec,
                [nameof(BackupTaskType)] = BackupTaskType.ToString(),
                [nameof(Destinations)] = Destinations == null ? null : new DynamicJsonArray(Destinations)
            };
        }
    }

    public class DatabaseInfo : DatabaseState
    {
        public bool Disabled { get; set; }
        public DatabaseLockMode LockMode;
        public bool IsAdmin { get; set; }
        public bool IsEncrypted { get; set; }
        public bool RejectClients { get; set; }
        public bool HasRevisionsConfiguration { get; set; }
        public bool HasExpirationConfiguration { get; set; }
        public bool HasRefreshConfiguration { get; set; }
        public int? IndexesCount { get; set; }
        public NodesTopology NodesTopology { get; set; }
        public int ReplicationFactor { get; set; }
        public bool DynamicNodesDistribution { get; set; }
        public Dictionary<string, DeletionInProgressStatus> DeletionInProgress { get; set; }

        public StudioConfiguration.StudioEnvironment Environment { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();

            djv[nameof(Name)] = Name;
            djv[nameof(Disabled)] = Disabled;
            djv[nameof(LockMode)] = LockMode;
            djv[nameof(IsAdmin)] = IsAdmin;
            djv[nameof(IsEncrypted)] = IsEncrypted;
            djv[nameof(RejectClients)] = false;
            djv[nameof(HasRevisionsConfiguration)] = HasRevisionsConfiguration;
            djv[nameof(HasExpirationConfiguration)] = HasExpirationConfiguration;
            djv[nameof(HasRefreshConfiguration)] = HasRefreshConfiguration;
            djv[nameof(IndexesCount)] = IndexesCount;
            djv[nameof(NodesTopology)] = NodesTopology?.ToJson();
            djv[nameof(ReplicationFactor)] = ReplicationFactor;
            djv[nameof(DynamicNodesDistribution)] = DynamicNodesDistribution;
            djv[nameof(DeletionInProgress)] = DynamicJsonValue.Convert(DeletionInProgress);
            djv[nameof(Environment)] = Environment;

            return djv;
        }
    }

    public class DatabaseState : IDynamicJson
    {
        public string Name { get; set; }
        public Size TotalSize { get; set; }
        public Size TempBuffersSize { get; set; }
        public TimeSpan? UpTime { get; set; }
        public BackupInfo BackupInfo { get; set; }
        public long? DocumentsCount { get; set; }
        public long? Alerts { get; set; }
        public long? PerformanceHints { get; set; }
        public string LoadError { get; set; }
        public long? IndexingErrors { get; set; }
        public IndexRunningStatus IndexingStatus { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(TotalSize)] = TotalSize != null ? new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = TotalSize.HumaneSize,
                    [nameof(Size.SizeInBytes)] = TotalSize.SizeInBytes
                } : null,
                [nameof(TempBuffersSize)] = TempBuffersSize != null ? new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = TempBuffersSize.HumaneSize,
                    [nameof(Size.SizeInBytes)] = TempBuffersSize.SizeInBytes
                } : null,
                [nameof(UpTime)] = UpTime?.ToString(),
                [nameof(BackupInfo)] = BackupInfo?.ToJson(),

                [nameof(Alerts)] = Alerts,
                [nameof(PerformanceHints)] = PerformanceHints,
                [nameof(IndexingErrors)] = IndexingErrors,

                [nameof(DocumentsCount)] = DocumentsCount,
                [nameof(IndexingStatus)] = IndexingStatus.ToString()
            };
        }
    }

    public sealed class MountPointUsage
    {
        public string Name { get; set; }

        public string Type { get; set; }

        public DiskSpaceResult DiskSpaceResult { get; set; }

        public IoStatsResult IoStatsResult { get; set; }

        public long UsedSpace { get; set; }

        public long UsedSpaceByTempBuffers { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Type)] = Type,
                [nameof(UsedSpace)] = UsedSpace,
                [nameof(UsedSpaceByTempBuffers)] = UsedSpaceByTempBuffers,
                [nameof(DiskSpaceResult)] = DiskSpaceResult.ToJson(),
                [nameof(IoStatsResult)] = IoStatsResult?.ToJson()
            };
        }
    }

    public sealed class DiskSpaceResult
    {
        public string DriveName { get; set; }

        public string VolumeLabel { get; set; }

        public long TotalFreeSpaceInBytes { get; set; }

        public long TotalSizeInBytes { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DriveName)] = DriveName,
                [nameof(VolumeLabel)] = VolumeLabel,
                [nameof(TotalFreeSpaceInBytes)] = TotalFreeSpaceInBytes,
                [nameof(TotalSizeInBytes)] = TotalSizeInBytes,
            };
        }
    }

    public sealed class IoStatsResult
    {
        public double IoReadOperations { get; set; }

        public double IoWriteOperations { get; set; }

        public long ReadThroughputInKb { get; set; }

        public long WriteThroughputInKb { get; set; }

        public long? QueueLength { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(IoReadOperations)] = IoReadOperations,
                [nameof(IoWriteOperations)] = IoWriteOperations,
                [nameof(ReadThroughputInKb)] = ReadThroughputInKb,
                [nameof(WriteThroughputInKb)] = WriteThroughputInKb,
                [nameof(QueueLength)] = QueueLength,
            };
        }
    }

    public sealed class NodesTopology : IDynamicJson
    {
        public List<NodeId> Members { get; set; }
        public List<NodeId> Promotables { get; set; }
        public List<NodeId> Rehabs { get; set; }
        public Dictionary<string, DatabaseGroupNodeStatus> Status { get; set; }
        public List<string> PriorityOrder { get; set; }
        public bool DynamicNodesDistribution { get; set; }

        public NodesTopology()
        {
            Members = new List<NodeId>();
            Promotables = new List<NodeId>();
            Rehabs = new List<NodeId>();
            Status = new Dictionary<string, DatabaseGroupNodeStatus>();
            PriorityOrder = new List<string>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Members)] = new DynamicJsonArray(Members.Select(x => x.ToJson())),
                [nameof(Promotables)] = new DynamicJsonArray(Promotables.Select(x => x.ToJson())),
                [nameof(Rehabs)] = new DynamicJsonArray(Rehabs.Select(x => x.ToJson())),
                [nameof(Status)] = DynamicJsonValue.Convert(Status),
                [nameof(PriorityOrder)] = PriorityOrder != null ? new DynamicJsonArray(PriorityOrder) : null,
                [nameof(DynamicNodesDistribution)] = DynamicNodesDistribution
            };
        }
    }

    public sealed class DatabaseGroupNodeStatus : IDynamicJson
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

    public sealed class NodeId : IDynamicJson
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
                [nameof(ResponsibleNode)] = ResponsibleNode
            };
        }
    }
}
