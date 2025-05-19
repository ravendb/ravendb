using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Dashboard;
using Raven.Server.Dashboard.Cluster.Notifications;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageAnalysisSummary : IDynamicJson
{
    public DebugPackageAnalysisSummary()
    {
        // deserialization
    }
    
    public DebugPackageAnalysisSummary(DebugPackageReport packageAnalysisReport)
    {
        PackageId = packageAnalysisReport.PackageId;
        SummaryPerNode = new Dictionary<string, DebugPackageNodeAnalysisSummary>(packageAnalysisReport.Reports.Length);
        
        foreach (var report in packageAnalysisReport.Reports)
        {
            var nodeTag = report.NodeTag;
            var summary = GetNodeSummary(report);
            
            SummaryPerNode.Add(nodeTag, summary);
        }

        ClusterWideIssues = packageAnalysisReport.ClusterWideIssues;
    }
    public string PackageId { get; set; }
    public Dictionary<string, DebugPackageNodeAnalysisSummary> SummaryPerNode { get; set; }
    public DebugPackageAnalysisIssues ClusterWideIssues { get; set; }

    private DebugPackageNodeAnalysisSummary GetNodeSummary(DebugPackageNodeReport nodeReport)
    {
        var databasesOverview = new DatabaseOverviewPayload()
        {
            Items = new List<DatabaseInfoItem>()
        };

        var databaseStorageUsage = new DatabaseStorageUsagePayload()
        {
            Items = new List<DatabaseDiskUsage>()
        };
        
        var databasesTasks = new OngoingTasksPayload()
        {
            Items = new List<DatabaseOngoingTasksInfoItem>()
        };
        
        var databaseIndexingSpeed = new IndexingSpeedPayload()
        {
            IndexingSpeedPerDatabase = new List<IndexingSpeedItem>()
        };
        
        foreach (var dbReport in nodeReport.Databases)
        {
            var databaseTopology = dbReport.DatabaseInfo.DatabaseRecord.Topology;

            var irrelevant = databaseTopology == null ||
                             databaseTopology.AllNodes.Contains(nodeReport.NodeTag) == false;
            
            var dbInfo = new DatabaseInfoItem
            {
                Database = dbReport.DatabaseName,
                DocumentsCount = dbReport.DatabaseInfo.Stats?.CountOfDocuments ?? -1,
                IndexesCount = dbReport.IndexesInfo?.Stats.Length ?? -1,
                ErroredIndexesCount = dbReport.IndexesInfo?.Stats.Count(x => x.State == IndexState.Error) ?? -1,
                IndexingErrorsCount = dbReport.IndexesInfo?.Errors.Sum(x => x.Errors.Length) ?? -1,
                Disabled = dbReport.DatabaseInfo.DatabaseRecord.Disabled,
                AlertsCount = -1,
                PerformanceHintsCount = -1,
                ReplicationFactor = dbReport.DatabaseInfo.DatabaseRecord.Topology?.ReplicationFactor ?? -1,
                Online = true,
                Irrelevant = irrelevant,
                OngoingTasksCount = dbReport.TasksInfo?.TaskCounts?.Total ?? -1,
                BackupInfo = dbReport.TasksInfo?.LastBackupInfo,
            };
            
            databasesOverview.Items.Add(dbInfo);

            if (dbReport.DatabaseInfo.Stats != null)
            {
                var dbDiskUsage = new DatabaseDiskUsage
                {
                    Database = dbReport.DatabaseName,
                    Size = dbReport.DatabaseInfo.Stats.SizeOnDisk.SizeInBytes,
                    TempBuffersSize = dbReport.DatabaseInfo.Stats.TempBuffersSizeOnDisk.SizeInBytes,
                };
            
                databaseStorageUsage.Items.Add(dbDiskUsage);
            }
            
            if (dbReport.TasksInfo is { TaskCounts: not null })
                databasesTasks.Items.Add(dbReport.TasksInfo.TaskCounts);

            if (dbReport.IndexesInfo is { Stats: not null })
            {
                var indexingSpeed = new IndexingSpeedItem
                {
                    Database = dbReport.DatabaseName,
                    IndexedPerSecond = dbReport.IndexesInfo.Stats
                        .Where(x => x.Type.IsMap() || x.Type.IsAuto())
                        .Sum(x => x.MappedPerSecondRate),
                    MappedPerSecond = dbReport.IndexesInfo.Stats
                        .Where(x => x.Type.IsMapReduce() || x.Type.IsAutoMapReduce())
                        .Sum(x => x.MappedPerSecondRate),
                    ReducedPerSecond = dbReport.IndexesInfo.Stats.Sum(x => x.ReducedPerSecondRate),
                };
                
                databaseIndexingSpeed.IndexingSpeedPerDatabase.Add(indexingSpeed);
            }
        }

        return new DebugPackageNodeAnalysisSummary
        {
            ClusterNodeInfo = new ClusterOverviewPayload
            {
                NodeState = nodeReport.ClusterNode.NodeStateInfo?.CurrentState ?? RachisState.Passive,
                NodeTag = nodeReport.NodeTag,
                NodeType = nodeReport.ClusterNode.NodeStateInfo?.Topology?.ServerRole.ToString(),
                NodeUrl = nodeReport.Server.PublicServerUrl ?? nodeReport.Server.ServerUrl ?? "unknown-node-url",
                OsName = nodeReport.Machine.OsInfo.FullName,
                OsType = nodeReport.Machine.OsInfo.Type,
                ServerVersion = nodeReport.Server.BasicServerInfo.Version,
                StartTime = nodeReport.Server.BasicServerInfo.StartUpTime ?? DateTime.MinValue,
                UpTime = nodeReport.Server.BasicServerInfo.UpTime
            },
            CpuUsageInfo = nodeReport.Server.CpuUsageInfo,
            MemoryUsageInfo = nodeReport.Server.MemoryInfo,
            GcInfo = nodeReport.Server.MemoryInfo.Managed.LastGcInfo,
            DatabasesOverview = databasesOverview,
            DatabaseStorageUsage = databaseStorageUsage,
            DatabasesOngoingTasks = databasesTasks,
            DatabaseIndexingSpeed = databaseIndexingSpeed,
            DetectedIssues = nodeReport.DetectedIssues,
            AnalyzeErrors = nodeReport.AnalyzeErrors,
        };
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(PackageId)] = PackageId,
            [nameof(SummaryPerNode)] = SummaryPerNode != null 
                ? DynamicJsonValue.Convert(SummaryPerNode)
                : null,
            [nameof(ClusterWideIssues)] = ClusterWideIssues?.ToJson()
        };
    }
}
