using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Text.Json;
using System.Text.Json.Serialization;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Dashboard;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Maintenance;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_14548 : RavenTestBase
{
    private static readonly JsonSerializerOptions DeserializeOptions = new JsonSerializerOptions { IncludeFields = true, Converters = { new JsonStringEnumConverter() } };
    
    public RavenDB_14548(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Debug)]
    public void CanAnalyzeServerDebugPackage()
    {
        using (var debugPackageStream = typeof(RavenDB_14548).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_14548.debug-package - Node [A].zip"))
        {
            var analyzer = new DebugPackageAnalyzer(debugPackageStream);

            var packageReport = analyzer.Analyze();

            var report = packageReport.ForNode("A");

            Assert.Equal(2, report.Machine.NumberOfCores);
            Assert.Equal(2, report.Machine.UtilizedCores);
            Assert.NotNull(report.Machine.InstalledMemoryInGb);
            Assert.NotNull(report.Machine.UsableMemoryInGb);
            Assert.Equal(report.Machine.InstalledMemoryInGb.Value, 1.88, 0.1);
            Assert.Equal(report.Machine.UsableMemoryInGb.Value, 1.88, 0.1);
            Assert.NotNull(report.Machine.OsInfo);
            Assert.Equal(OSType.Linux, report.Machine.OsInfo.Type);
            Assert.Contains("Ubuntu", report.Machine.OsInfo.FullName);

            Assert.Contains("live-test", report.Server.PublicServerUrl);
            
            Assert.NotNull(report.Server.BasicServerInfo.UpTime);
            Assert.NotNull(report.Server.BasicServerInfo.StartUpTime);
            Assert.Equal("A", report.Server.BasicServerInfo.NodeTag);
            Assert.Equal("7.0.2-nightly-20250415-0350", report.Server.BasicServerInfo.Version);
            Assert.NotNull(report.Server.BasicServerInfo.ServerId);
            Assert.NotNull(report.Server.CpuUsageInfo.AverageCpuUsage);
            Assert.Equal(report.Server.CpuUsageInfo.AverageCpuUsage.Value, 4.2, 0.1);

            Assert.NotEmpty(report.Server.ServerSettings);
            
            Assert.Equal(71, report.Server.NetworkInfo.TotalActiveTcpConnections);
            var networkInfoTcpConnections = report.Server.NetworkInfo.TcpConnections;

            var establishedConnections = networkInfoTcpConnections.First(x => x.TcpState == nameof(TcpState.Established));
            Assert.Equal(14, establishedConnections.NumberOfConnectionsInState);
            Assert.NotEmpty(establishedConnections.TopConnectionsInState);

            var timeWaitConnections = networkInfoTcpConnections.First(x => x.TcpState == nameof(TcpState.TimeWait));
            Assert.Equal(57, timeWaitConnections.NumberOfConnectionsInState);
            Assert.NotEmpty(establishedConnections.TopConnectionsInState);

            var nodeSummary = packageReport.GetSummary().SummaryPerNode["A"];

            Assert.NotNull(nodeSummary.ClusterNodeInfo.UpTime);
            Assert.Equal("A", nodeSummary.ClusterNodeInfo.NodeTag);
            Assert.Equal(RachisState.Leader, nodeSummary.ClusterNodeInfo.NodeState);
            Assert.NotNull(nodeSummary.ClusterNodeInfo.StartTime);
            Assert.NotNull(nodeSummary.ClusterNodeInfo.OsType);
            Assert.NotNull(nodeSummary.ClusterNodeInfo.ServerVersion);
            
            var databasesOverview = nodeSummary.DatabasesOverview;

            Assert.Equal(3, databasesOverview.Items.Count);

            var databaseNames = databasesOverview.Items.Select(x => x.Database).ToList();
            
            Assert.Contains("aaa", databaseNames);
            Assert.Contains("DemoUser-8d208a62-2252-4bdb-84ec-acb5daee25c2", databaseNames);
            Assert.Contains("DemoUser-d25cbd4f-6b1c-4828-a868-25cf81bd783a", databaseNames);

            foreach (DatabaseInfoItem dbInfo in databasesOverview.Items)
            {
                Assert.True(dbInfo.DocumentsCount > 0);
                Assert.True(dbInfo.IndexesCount >= 0);
                Assert.Equal(0, dbInfo.OngoingTasksCount);
                Assert.Null(dbInfo.BackupInfo);
                Assert.False(dbInfo.Disabled);
                Assert.True(dbInfo.Online);
            }
            
            var memoryInfo = report.Server.MemoryInfo;

            Assert.Equal("293.9 MBytes", memoryInfo.AvailableMemory);
            Assert.Equal("828.97 MBytes", memoryInfo.AvailableMemoryForProcessing);
            Assert.Equal("1.886 GBytes", memoryInfo.PhysicalMemory);
            Assert.Equal("556.02 MBytes", memoryInfo.WorkingSet);
            Assert.Equal("262.48 MBytes", memoryInfo.Managed.ManagedAllocations);
            Assert.Equal("7.46 MBytes", memoryInfo.Unmanaged.UnmanagedAllocations);

            Assert.NotNull(memoryInfo.Managed.LastGcInfo);

            Assert.NotNull(report.Server.NetworkInfo);
            Assert.NotNull(report.Server.NetworkInfo.TcpConnections);
            Assert.Equal(2, report.Server.NetworkInfo.TcpConnections.Count(x =>
                x.TcpState == nameof(TcpState.Established) || x.TcpState == nameof(TcpState.TimeWait)));
            Assert.NotNull(report.Server.NetworkInfo.TcpConnections);
            Assert.Equal(71, report.Server.NetworkInfo.TotalActiveTcpConnections);

            Assert.NotNull(report.Server.ThreadsInfo);
            Assert.NotNull(report.Server.ThreadsInfo.Threads);
            Assert.NotNull(report.Server.ThreadsInfo.StackTracesEntry);
            Assert.True(report.Server.ThreadsInfo.Threads.CpuUsage > 0);
            Assert.True(report.Server.ThreadsInfo.Threads.ProcessCpuUsage > 0);
            Assert.Equal(2, report.Server.ThreadsInfo.Threads.ActiveCores);
            Assert.NotNull(report.Server.ThreadsInfo.Threads.List);

            Assert.NotEmpty(report.Server.CpuUsageInfo.TopCurrentCpuUsageThreads);
            Assert.NotEmpty(report.Server.CpuUsageInfo.TopOverallCpuUsageThreads);

            Assert.NotNull(report.ClusterNode);
            
            Assert.NotNull(report.ClusterNode.DefaultElectionTimeoutInMs);
            Assert.NotNull(report.ClusterNode.ElectionTimeoutInMs);
            
            Assert.Equal(300, report.ClusterNode.DefaultElectionTimeoutInMs);
            Assert.Equal(300, report.ClusterNode.ElectionTimeoutInMs);
            
            Assert.NotNull(report.ClusterNode.NodeStateInfo.CurrentTerm);
            Assert.NotNull(report.ClusterNode.NodeStateInfo.Topology.NodeTag);
            Assert.Equal("A", report.ClusterNode.NodeStateInfo.Topology.NodeTag);
            Assert.NotNull(report.ClusterNode.NodeStateInfo.CurrentState);
            Assert.Equal(RachisState.Leader, report.ClusterNode.NodeStateInfo.CurrentState);
            Assert.Equal(1, report.ClusterNode.NodeStateInfo.CurrentTerm);
            Assert.NotEmpty(report.ClusterNode.NodeStateInfo.Topology.Topology.Members);
            Assert.Contains(report.ClusterNode.NodeStateInfo.Topology.Topology.Members, m => m.Key == "A");
            Assert.NotNull(report.ClusterNode.NodeStateInfo.Topology.Topology.Watchers);
            Assert.NotNull(report.ClusterNode.NodeStateInfo.Topology.Topology.Promotables);
            Assert.Equal("A", report.ClusterNode.NodeStateInfo.Topology.Topology.LastNodeId);
            Assert.NotEmpty(report.ClusterNode.NodeStateInfo.Topology.Topology.TopologyId);

            Assert.NotNull(report.ClusterNode.NodeLogInfo);
            Assert.NotNull(report.ClusterNode.NodeLogInfo.LogEntry);
            Assert.NotNull(report.ClusterNode.NodeLogInfo.LogSummary);
            Assert.NotNull(report.ClusterNode.NodeLogInfo.DebugInfoCollectedAt);

            Assert.NotNull(report.ClusterNode.NodeLogInfo.ConnectionToPeers);

            Assert.NotNull(report.ClusterNode.ObserverInfo.ObserverDecisions);
            Assert.NotNull(report.ClusterNode.ObserverInfo.ObserverDecisionsEntry);
            Assert.False(report.ClusterNode.ObserverInfo.ObserverDecisions.Suspended);
            Assert.NotNull(report.ClusterNode.ObserverInfo.ObserverDecisions.ObserverLog);
            Assert.NotNull(report.ClusterNode.ObserverInfo.ObserverDecisions.LeaderNode);
            Assert.True(report.ClusterNode.ObserverInfo.ObserverDecisions.Iteration > 0);
            Assert.True(report.ClusterNode.ObserverInfo.ObserverDecisions.Term > 0);
            Assert.True(report.ClusterNode.ObserverInfo.ObserverDecisions.ObserverLog.Count >= 0);

            Assert.Equal(3, report.Databases.Length);

            foreach (var db in report.Databases)
            {
                Assert.NotNull(db.DatabaseInfo);
                Assert.NotNull(db.DatabaseName);
                Assert.NotNull(db.DatabaseInfo.Stats.DatabaseId);
                Assert.True(db.DatabaseInfo.Stats.CountOfDocuments > 0);
                Assert.True(db.DatabaseInfo.Stats.CountOfIndexes >= 0);
                Assert.True(db.DatabaseInfo.Stats.CountOfAttachments >= 0);
                Assert.True(db.DatabaseInfo.Stats.CountOfRevisionDocuments >= 0);
                Assert.True(db.DatabaseInfo.Stats.CountOfCounterEntries >= 0);
                Assert.True(db.DatabaseInfo.Stats.CountOfTimeSeriesSegments >= 0);

                Assert.NotNull(db.IndexesInfo);
                Assert.NotNull(db.IndexesInfo.Definitions);
                
                foreach (var index in db.IndexesInfo.Stats)
                {
                    Assert.NotNull(index.Priority);
                    Assert.NotNull(index.State);
                    Assert.NotNull(index.Type);
                    Assert.NotNull(index.Status);

                    if (index.LastIndexingTime.HasValue)
                    {
                        Assert.True(index.LastIndexingTime.Value > DateTime.MinValue);
                    }

                    Assert.NotNull(index.LockMode);
                    Assert.True(index.EntriesCount >= 0);
                    Assert.True(index.ErrorsCount >= 0);
                }

                foreach (IndexDefinition definition in db.IndexesInfo.Definitions)
                {
                    Assert.NotNull(definition.Name);
                    Assert.NotNull(definition.Maps);
                    Assert.NotEmpty(definition.Maps);
                    Assert.True(definition.Fields?.Count >= 0);
                    Assert.NotNull(definition.Type);
                    Assert.True(definition.LockMode >= 0);
                    Assert.True(definition.Priority >= 0);
                    Assert.NotNull(definition.Configuration);
                }

                foreach (var metadataInfo in db.IndexesInfo.Metadata)
                {
                    Assert.NotNull(metadataInfo.Name);
                    Assert.True(metadataInfo.State >= 0);
                    Assert.True(metadataInfo.Priority >= 0);
                    Assert.NotNull(metadataInfo.Type);
                    Assert.NotNull(metadataInfo.LockMode);
                }
            }

            Assert.Equal(0, report.AnalyzeErrors.Errors.Count);
            Assert.Equal(5, report.DetectedIssues.ServerIssues.Count);
            Assert.Equal(0, report.DetectedIssues.ClusterIssues.Count);
        }
    }

    [RavenFact(RavenTestCategory.Core)]
    public void CanAnalyzeClusterWideDebugPackageAndGetSummary()
    {
        using (var store = GetDocumentStore())
        {
            var executor = store.GetRequestExecutor();

            using var _ = executor.ContextPool.AllocateOperationContext(out var ctx);

            var summaryResult = UploadClusterWideDebugPackage(store, ctx);

            try
            {
                Assert.NotNull(summaryResult);
                Assert.NotNull(summaryResult.PackageId);
                Assert.NotEmpty(summaryResult.PackageId);
                Assert.NotNull(summaryResult.SummaryPerNode);
                Assert.NotEmpty(summaryResult.SummaryPerNode);
                Assert.True(summaryResult.SummaryPerNode.ContainsKey("A"));

                // node C was down - intentionally
                Assert.NotNull(summaryResult.SummaryPerNode["A"]);
                Assert.NotNull(summaryResult.SummaryPerNode["B"]);

                foreach (var item in summaryResult.SummaryPerNode.Where(x => x.Key != "C"))
                {
                    var nodeSummary = item.Value;
                    var nodeTag = item.Key;

                    Assert.NotNull(nodeSummary.ClusterNodeInfo);
                    Assert.Equal(nodeTag, nodeSummary.ClusterNodeInfo.NodeTag);
                    Assert.Equal("6.2.5", nodeSummary.ClusterNodeInfo.ServerVersion);
                    Assert.NotNull(nodeSummary.ClusterNodeInfo.UpTime);
                    Assert.NotNull(nodeSummary.ClusterNodeInfo.StartTime);
                    Assert.NotNull(nodeSummary.ClusterNodeInfo.UpTime);

                    Assert.NotNull(nodeSummary.DatabasesOverview);

                    var databaseNames = nodeSummary.DatabasesOverview.Items.Select(x => x.Database).ToList();
                    if (nodeTag == "A")
                    {
                        Assert.Equal(2, nodeSummary.DatabasesOverview.Items.Count);
                        Assert.Contains("Northwind", databaseNames);
                        Assert.Contains("EastRain", databaseNames);
                    }
                    else
                    {
                        Debug.Assert(nodeTag == "B", "Unexpected node tag");

                        Assert.Equal(2, nodeSummary.DatabasesOverview.Items.Count);
                        Assert.Contains("Northwind", databaseNames);
                        Assert.Contains("WestCloud", databaseNames);
                    }

                    Assert.NotNull(nodeSummary.ClusterNodeInfo);
                    Assert.Equal(2, nodeSummary.CpuUsageInfo.NumberOfCores);
                    Assert.Equal(2, nodeSummary.CpuUsageInfo.UtilizedCores);
                    Assert.NotNull(nodeSummary.MemoryUsageInfo.PhysicalMemory);
                    Assert.NotNull(nodeSummary.MemoryUsageInfo.AvailableMemory);
                    Assert.NotNull(nodeSummary.ClusterNodeInfo.OsName);
                    Assert.Equal(OSType.Linux, nodeSummary.ClusterNodeInfo.OsType);
                    Assert.Contains("Ubuntu", nodeSummary.ClusterNodeInfo.OsName);

                    Assert.NotEmpty(nodeSummary.ClusterNodeInfo.NodeUrl);
                    
                    Assert.NotNull(nodeSummary.MemoryUsageInfo);
                    Assert.NotNull(nodeSummary.MemoryUsageInfo.AvailableMemory);
                    Assert.NotNull(nodeSummary.MemoryUsageInfo.AvailableMemoryForProcessing);
                    Assert.NotNull(nodeSummary.MemoryUsageInfo.PhysicalMemory);
                    Assert.NotNull(nodeSummary.MemoryUsageInfo.WorkingSet);
                    Assert.NotNull(nodeSummary.MemoryUsageInfo.Managed);
                    Assert.NotNull(nodeSummary.MemoryUsageInfo.Unmanaged);

                    Assert.NotNull(nodeSummary.DetectedIssues);

                    if (nodeTag == "A")
                    {
                        Assert.NotNull(nodeSummary.DetectedIssues.ServerIssues);

                        Assert.Equal(7, nodeSummary.DetectedIssues.ServerIssues.Count);
                        Assert.Equal(2, nodeSummary.DetectedIssues.ClusterIssues.Count);
                        
                        Assert.Equal(6, nodeSummary.DetectedIssues.DatabaseIssues["Northwind"].Count);
                        Assert.Equal(2, nodeSummary.DetectedIssues.DatabaseIssues["EastRain"].Count);
                    }
                    else
                    {
                        Debug.Assert(nodeTag == "B", "Unexpected node tag");

                        Assert.NotNull(nodeSummary.DetectedIssues.ServerIssues);

                        Assert.Equal(7, nodeSummary.DetectedIssues.ServerIssues.Count);
                        Assert.Equal(0, nodeSummary.DetectedIssues.ClusterIssues.Count);
                        
                        Assert.Equal(5, nodeSummary.DetectedIssues.DatabaseIssues["Northwind"].Count);
                    }

                    Assert.NotNull(nodeSummary.AnalyzeErrors);
                    Assert.Equal(0, nodeSummary.AnalyzeErrors.Errors.Count);

                    foreach (var dbInfo in nodeSummary.DatabasesOverview.Items)
                    {
                        Assert.True(dbInfo.DocumentsCount > 0);
                        Assert.True(dbInfo.IndexesCount > 0);
                        Assert.True(dbInfo.OngoingTasksCount > 0);
                        Assert.True(dbInfo.AlertsCount == -1);
                        Assert.True(dbInfo.PerformanceHintsCount == -1);
                        Assert.True(dbInfo.ErroredIndexesCount >= 0);
                        Assert.True(dbInfo.IndexingErrorsCount >= 0);
                        Assert.True(dbInfo.ReplicationFactor >= 1);
                        Assert.NotNull(dbInfo.Database);
                        Assert.NotEmpty(dbInfo.Database);
                        Assert.False(dbInfo.Disabled);
                        Assert.True(dbInfo.Online);
                        Assert.NotNull(dbInfo.BackupInfo);
                    }
                }

                Assert.NotNull(summaryResult.ClusterWideIssues);
                
                Assert.Equal(1, summaryResult.ClusterWideIssues.ClusterIssues.Count);

                Assert.Contains("Custom Election Timeout", summaryResult.ClusterWideIssues.ClusterIssues[0].Title);
                
                Assert.Equal(0, summaryResult.ClusterWideIssues.DatabaseIssues.Count);
                Assert.Equal(0, summaryResult.ClusterWideIssues.ServerIssues.Count);
            }
            finally
            {
                if (summaryResult != null)
                {
                    var removePackageAnalysis = new RemoveDebugPackageAnalysisCommand(summaryResult.PackageId);
                    store.GetRequestExecutor().Execute(removePackageAnalysis, ctx);
                }
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Debug)]
    [InlineData("single-node", "A", "aaa")]
    [InlineData("single-node", "A", "DemoUser-d25cbd4f-6b1c-4828-a868-25cf81bd783a")]
    [InlineData("cluster-wide", "A", "Northwind")]
    [InlineData("cluster-wide", "B", "WestCloud")]
    [InlineData("cluster-wide", "A", "EastRain")]
    public void DebugPackageAnalysisReadEndpoints(string packageType, string node, string database)
    {
        using (var store = GetDocumentStore())
        {
            var executor = store.GetRequestExecutor();

            using var _ = executor.ContextPool.AllocateOperationContext(out var ctx);

            DebugPackageAnalysisSummary summaryResult;
            
            switch (packageType)
            {
                case "single-node":
                    summaryResult = UploadSingleNodeDebugPackage(store, ctx);
                    break;
                case "cluster-wide":
                    summaryResult = UploadClusterWideDebugPackage(store, ctx);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(packageType), packageType, null);
            }
            
            try
            {
                foreach (var item in new (string Endpoint, Type ReturnedType)[]
                         {
                             ($"summary?packageId={summaryResult.PackageId}", typeof(DebugPackageAnalysisSummary)),
                             ($"summary/node?packageId={summaryResult.PackageId}&nodeTag={node}", typeof(DebugPackageNodeAnalysisSummary)),
                             
                             ($"network?packageId={summaryResult.PackageId}&nodeTag={node}", typeof(NetworkAnalysisInfo)),
                             ($"memory?packageId={summaryResult.PackageId}&nodeTag={node}", typeof(MemoryAnalysisInfo)),
                             ($"threads/runaway?packageId={summaryResult.PackageId}&nodeTag={node}", typeof(ThreadsInfo)),
                             ($"threads/stack-trace?packageId={summaryResult.PackageId}&nodeTag={node}", null),
                             
                             ($"cluster/topology?packageId={summaryResult.PackageId}&nodeTag={node}", typeof(ClusterTopology)),
                             ($"cluster/log?packageId={summaryResult.PackageId}&nodeTag={node}", typeof(LogSummary)),
                             ($"cluster/observer/decisions?packageId={summaryResult.PackageId}&nodeTag={node}", typeof(ClusterObserverDecisions)),
                             
                             //($"databases/overview?packageId={summaryResult.PackageId}&nodeTag={node}&name={database}", typeof(DatabaseOverviewAnalysisInfo)),
                             ($"databases/stats?packageId={summaryResult.PackageId}&nodeTag={node}&name={database}", typeof(DatabaseStatistics)),
                             ($"databases/record?packageId={summaryResult.PackageId}&nodeTag={node}&name={database}", typeof(DatabaseRecord)),
                             ($"databases/indexes?packageId={summaryResult.PackageId}&nodeTag={node}&name={database}", typeof(GetIndexesResponse)),
                             ($"databases/indexes/stats?packageId={summaryResult.PackageId}&nodeTag={node}&name={database}", typeof(GetIndexStatisticsResponse)),
                             ($"databases/indexes/performance?packageId={summaryResult.PackageId}&nodeTag={node}&name={database}", typeof(GetIndexPerformanceStatsResponse)),
                             ($"databases/indexes/errors?packageId={summaryResult.PackageId}&nodeTag={node}&name={database}", typeof(GetIndexErrorsResponse)),
                             
                             ($"databases/configuration/settings?packageId={summaryResult.PackageId}&nodeTag={node}&name={database}", null),
                         })
                {
                    var getInfoCmd = new GetDebugPackageAnalysisInfoCommand<dynamic>(item.Endpoint)
                    {
                        DeserializeType = item.ReturnedType
                    };
                    store.GetRequestExecutor().Execute(getInfoCmd, ctx);
                   
                    Assert.NotNull(getInfoCmd.Result);
                    
                    if (item.ReturnedType != null)
                        Assert.NotNull(getInfoCmd.DeserializedObject);
                }
            }
            finally
            {
                if (summaryResult != null)
                {
                    var removePackageAnalysis = new RemoveDebugPackageAnalysisCommand(summaryResult.PackageId);
                    store.GetRequestExecutor().Execute(removePackageAnalysis, ctx);
                }
            }
        }
    }
    
    private sealed class GetIndexPerformanceStatsResponse : ResultsResponse<IndexPerformanceStats>
    {
    }
    
    private sealed class GetIndexErrorsResponse : ResultsResponse<IndexErrors>
    {
    }

    [RavenFact(RavenTestCategory.Debug)]
    public void CanGetNetworkInfoForNode()
    {
        using (var store = GetDocumentStore())
        {
            var executor = store.GetRequestExecutor();

            using var _ = executor.ContextPool.AllocateOperationContext(out var ctx);

            var summaryResult = UploadClusterWideDebugPackage(store, ctx);

            try
            {
                var networkInfoDebugPackageCmd = new GetNetworkAnalysisInfoCommand(summaryResult.PackageId, "A");
                store.GetRequestExecutor().Execute(networkInfoDebugPackageCmd, ctx);
                var networkInfo = networkInfoDebugPackageCmd.Result;

                Assert.Equal(16, networkInfo.TotalActiveTcpConnections);

                var establishedConnections = networkInfo.TcpConnections.First(x => x.TcpState == nameof(TcpState.Established));
                var timeWaitConnections = networkInfo.TcpConnections.First(x => x.TcpState == nameof(TcpState.TimeWait));

                Assert.Equal(14, establishedConnections.NumberOfConnectionsInState);
                Assert.Equal(2, timeWaitConnections.NumberOfConnectionsInState);

                Assert.Equal(5, establishedConnections.TopConnectionsInState.Count);
                Assert.Equal(2, timeWaitConnections.TopConnectionsInState.Count);

                foreach (var topConnections in new[] { establishedConnections.TopConnectionsInState, timeWaitConnections.TopConnectionsInState })
                {
                    foreach (int ip in topConnections.Values)
                    {
                        Assert.NotNull(ip);
                    }
                }
            }
            finally
            {
                if (summaryResult != null)
                {
                    var removePackageAnalysis = new RemoveDebugPackageAnalysisCommand(summaryResult.PackageId);
                    store.GetRequestExecutor().Execute(removePackageAnalysis, ctx);
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.Debug)]
    public void CanGetMemoryInfoForNode()
    {
        using (var store = GetDocumentStore())
        {
            var executor = store.GetRequestExecutor();

            using var _ = executor.ContextPool.AllocateOperationContext(out var ctx);

            var summaryResult = UploadClusterWideDebugPackage(store, ctx);

            try
            {
                var memoryInfoDebugPackageCmd = new GetMemoryAnalysisInfoCommand(summaryResult.PackageId, "A");
                store.GetRequestExecutor().Execute(memoryInfoDebugPackageCmd, ctx);
                var memoryInfo = memoryInfoDebugPackageCmd.Result;

                Assert.NotNull(memoryInfo);
                Assert.NotNull(memoryInfo.PhysicalMemory);
                Assert.NotNull(memoryInfo.WorkingSet);
                Assert.NotNull(memoryInfo.AvailableMemory);
                Assert.NotNull(memoryInfo.AvailableMemoryForProcessing);

                Assert.NotNull(memoryInfo.Managed);
                Assert.NotNull(memoryInfo.Managed.ManagedAllocations);

                Assert.NotNull(memoryInfo.Unmanaged);
                Assert.NotNull(memoryInfo.Unmanaged.UnmanagedAllocations);

                Assert.NotEmpty(memoryInfo.PhysicalMemory);
                Assert.NotEmpty(memoryInfo.WorkingSet);
                Assert.NotEmpty(memoryInfo.AvailableMemory);
                Assert.NotEmpty(memoryInfo.AvailableMemoryForProcessing);
                Assert.NotEmpty(memoryInfo.Managed.ManagedAllocations);
                Assert.NotEmpty(memoryInfo.Unmanaged.UnmanagedAllocations);

                Assert.Contains("GBytes", memoryInfo.PhysicalMemory);
                Assert.NotNull(memoryInfo.WorkingSet);
                Assert.NotNull(memoryInfo.AvailableMemory);
                Assert.NotNull(memoryInfo.AvailableMemoryForProcessing);
                Assert.NotNull(memoryInfo.Managed.ManagedAllocations);
                Assert.NotNull(memoryInfo.Unmanaged.UnmanagedAllocations);
            }
            finally
            {
                if (summaryResult != null)
                {
                    var removePackageAnalysis = new RemoveDebugPackageAnalysisCommand(summaryResult.PackageId);
                    store.GetRequestExecutor().Execute(removePackageAnalysis, ctx);
                }
            }
        }
    }
    
    private class UploadDebugPackageCommand(Stream debugPackageStream) : RavenCommand<DebugPackageAnalysisSummary>
    {
        public override bool IsReadRequest { get; } = false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/debug/info-package/analyzer/upload";

            return new HttpRequestMessage 
            { 
                Method = HttpMethod.Post, 
                Content = new StreamContent(debugPackageStream)
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = JsonSerializer.Deserialize<DebugPackageAnalysisSummary>(response.ToString(), DeserializeOptions);
        }
    }

    private class RemoveDebugPackageAnalysisCommand(string debugPackageId) : RavenCommand<DebugPackageAnalysisSummary>
    {
        public override bool IsReadRequest { get; } = false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/debug/info-package/analyzer/remove?packageId={debugPackageId}";

            return new HttpRequestMessage { Method = HttpMethod.Delete };
        }
    }

    private class GetDebugPackageAnalysisInfoCommand<T>(string endpointInfoSuffixWithParameters) : RavenCommand<T> where T : class
    {
        public override bool IsReadRequest => true;
        public Type DeserializeType { get; set; }
        public object DeserializedObject { get; set; }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/debug/info-package/analyzer/{endpointInfoSuffixWithParameters.TrimStart('/')}";

            return new HttpRequestMessage { Method = HttpMethod.Get };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (DeserializeType != null)
            {
                // for testing the deserialization of a specific type
                DeserializedObject = JsonSerializer.Deserialize(response.ToString(), DeserializeType, DeserializeOptions);
            }
            
            Result = JsonSerializer.Deserialize<T>(response.ToString(), DeserializeOptions);
        }
    }
    
    private class GetNetworkAnalysisInfoCommand(string packageId, string nodeTag)
        : GetDebugPackageAnalysisInfoCommand<NetworkAnalysisInfo>($"network?packageId={packageId}&nodeTag={nodeTag}");

    private class GetMemoryAnalysisInfoCommand (string packageId, string nodeTag)
        : GetDebugPackageAnalysisInfoCommand<MemoryAnalysisInfo>($"memory?packageId={packageId}&nodeTag={nodeTag}");

    private static DebugPackageAnalysisSummary UploadClusterWideDebugPackage(DocumentStore store, JsonOperationContext ctx)
    {
        return UploadDebugPackage("SlowTests.Data.RavenDB_14548.debug-package - Cluster Wide.zip", store, ctx);
    }
    
    private static DebugPackageAnalysisSummary UploadSingleNodeDebugPackage(DocumentStore store, JsonOperationContext ctx)
    {
        return UploadDebugPackage("SlowTests.Data.RavenDB_14548.debug-package - Node [A].zip", store, ctx);
    }
    
    private static DebugPackageAnalysisSummary UploadDebugPackage(string packageResourceName, DocumentStore store, JsonOperationContext ctx)
    {
        using var debugPackageStream = typeof(RavenDB_14548).Assembly.GetManifestResourceStream(packageResourceName);
        var analyzeDebugPackageCmd = new UploadDebugPackageCommand(debugPackageStream);
        store.GetRequestExecutor().Execute(analyzeDebugPackageCmd, ctx);
        return analyzeDebugPackageCmd.Result;
    }
}
