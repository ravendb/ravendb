using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Database;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Cluster;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage;

public class DebugPackageAnalyzer(Stream packageZipStream)
{
    public DebugPackageReport Analyze()
    {
        var packageZip = new ZipArchive(packageZipStream, ZipArchiveMode.Read, leaveOpen: true);

        bool isClusterPackage = packageZip.Entries.Any(x => x.FullName.EndsWith(".zip"));

        var packagesContent = isClusterPackage ? packageZip.Entries.Where(x => x.FullName.EndsWith(".zip")).Select(x => x.Open()).ToArray() : [packageZipStream];

        if (isClusterPackage)
        {
            var errors = packageZip.Entries.Where(x => x.FullName.EndsWith(".error"));
            // TODO arek
        }
        
        var nodeReports = new List<DebugPackageNodeReport>();

        foreach (var stream in packagesContent)
        {
            var errors = new DebugPackageAnalyzeErrors();
            var issues = new DebugPackageAnalysisIssues();

            using (var reader = new DebugPackageReader(new ZipArchive(stream, ZipArchiveMode.Read), errors))
            {
                var packageContent = reader.ReadPackageEntries();

                var machineAnalyzer = new MachineInfoAnalyzer(errors, issues);
                var basicServerInfoAnalyzer = new BasicServerInfoAnalyzer(errors, issues);
                var cpuUsageAnalyzer = new CpuUsageInfoAnalyzer(machineAnalyzer, basicServerInfoAnalyzer, errors, issues);
                var networkAnalyzer = new NetworkInfoAnalyzer(errors, issues);
                var memoryAnalyzer = new MemoryInfoAnalyzer(errors, issues);
                var gcAnalyzer = new GcInfoAnalyzer(memoryAnalyzer, errors, issues);
                var threadsAnalyzer = new ThreadsInfoAnalyzer(cpuUsageAnalyzer, errors, issues);

                var clusterStateAnalyzer = new ClusterStateInfoAnalyzer(errors, issues);
                var clusterLogAnalyzer = new ClusterLogInfoAnalyzer(errors, issues);
                var clusterObserverAnalyzer = new ClusterObserverInfoAnalyzer(errors, issues);

                var serverWideAnalyzers = new AbstractDebugPackageAnalyzer[]
                {
                    // the order matters here e.g., we want to run the machine analysis first, so we can check the machine details in other analyzers
                    machineAnalyzer, 
                    basicServerInfoAnalyzer, 
                    cpuUsageAnalyzer, 
                    networkAnalyzer, 
                    memoryAnalyzer, 
                    gcAnalyzer, 
                    threadsAnalyzer,
                    
                    clusterStateAnalyzer, 
                    clusterLogAnalyzer,
                    clusterObserverAnalyzer
                };

                foreach (var analyzer in serverWideAnalyzers)
                {
                    try
                    {
                        analyzer.Analyze(packageContent.ServerEntries);
                    }
                    catch (Exception e)
                    {
                        errors.AddAnalyzerError(analyzer.Name, "Exception when running server wide analyzer", AnalyzeErrorSeverity.Error, e);
                    }
                }
                
                var serverAnalysisInfo = new ServerAnalysisInfo
                {
                    BasicServerInfo = basicServerInfoAnalyzer.BasicServerInfo,
                    NetworkInfo = networkAnalyzer.NetworkInfo,
                    CpuUsageInfo = cpuUsageAnalyzer.CpuUsageInfo,
                    MemoryInfo = memoryAnalyzer.MemoryInfo,
                    ThreadsInfo = threadsAnalyzer.ThreadsInfo,
                };
                
                var clusterAnalysisInfo = new ClusterAnalysisInfo
                {
                    NodeStateInfo = clusterStateAnalyzer.ClusterNodeStateInfo, 
                    NodeLogInfo = clusterLogAnalyzer.ClusterNodeLogInfo,
                    ObserverInfo = clusterObserverAnalyzer.ObserverAnalysisInfo,
                };
                
                var databaseReports = new List<DebugPackageDatabaseReport>();
                
                foreach (var databaseName in packageContent.DatabaseNames)
                {
                    var generalInfoAnalyzer = new GeneralDatabaseInfoAnalyzer(databaseName, errors, issues);
                    var configurationAnalyzer = new ConfigurationInfoAnalyzer(serverAnalysisInfo, clusterAnalysisInfo, databaseName, errors, issues);
                    var indexesAnalyzer = new IndexesInfoAnalyzer(databaseName, errors, issues);
                    var tombstonesAnalyzer = new TombstonesInfoAnalyzer(databaseName, errors, issues);
                    var tasksAnalyzer = new TasksInfoAnalyzer(databaseName, errors, issues);
                    
                    var databaseAnalyzers = new AbstractDebugPackageDatabaseAnalyzer[]
                    {
                        generalInfoAnalyzer,
                        indexesAnalyzer,
                        tombstonesAnalyzer,
                        tasksAnalyzer,
                        configurationAnalyzer
                    };

                    var databaseEntries = packageContent.ForDatabase(databaseName);
   
                    foreach (var analyzer in databaseAnalyzers)
                    {
                        if (generalInfoAnalyzer.Analyzed && generalInfoAnalyzer.DatabaseInfo.DatabaseRecord is { Disabled: true })
                            continue;
                            
                        try
                        {
                            analyzer.Analyze(databaseEntries);
                        }
                        catch (Exception e)
                        {
                            errors.AddAnalyzerError($"{analyzer.Name} for '{databaseName}' database", "Exception when running database analyzer", 
                                AnalyzeErrorSeverity.Error, e);
                        }
                    }
                    
                    var databaseReport = new DebugPackageDatabaseReport(databaseName)
                    {
                        DatabaseInfo = generalInfoAnalyzer.DatabaseInfo,
                        Settings = configurationAnalyzer.DatabaseSettings,
                        IndexesInfo = indexesAnalyzer.IndexesInfo,
                        TasksInfo = tasksAnalyzer.TasksInfo,
                    };
                    
                    databaseReports.Add(databaseReport);
                }

                var nodeTag = basicServerInfoAnalyzer.BasicServerInfo?.NodeTag ?? clusterStateAnalyzer.ClusterNodeStateInfo?.Topology?.NodeTag ?? "A";
                
                var report = new DebugPackageNodeReport(nodeTag)
                {
                    Machine = machineAnalyzer.MachineInfo,
                    Server = serverAnalysisInfo,
                    ClusterNode = clusterAnalysisInfo,
                    Databases = databaseReports.ToArray(),
                    DetectedIssues = issues,
                    AnalyzeErrors = errors,
                };

                nodeReports.Add(report);
            }
        }

        var clusterWideIssues = DetectClusterIssues(nodeReports);
        var databaseGroupsIssues = DetectDatabaseGroupsIssues(nodeReports);

        return new DebugPackageReport(nodeReports.ToArray(), new DebugPackageAnalysisIssues
        {
            ClusterIssues = clusterWideIssues,
            DatabaseIssues = databaseGroupsIssues
        });
    }

    private List<DetectedIssue> DetectClusterIssues(List<DebugPackageNodeReport> reports)
    {
        var clusterWideIssues = new List<DetectedIssue>();

        foreach (var report in reports)
        {
            var nodeTag = report.NodeTag;
            var nodeAnalysis = report.ClusterNode;

            if (nodeAnalysis != null)
                continue;

            // TODO arek - check
        }

        return clusterWideIssues;
    }
    private Dictionary<string, List<DetectedIssue>> DetectDatabaseGroupsIssues(List<DebugPackageNodeReport> reports)
    {
        var databaseGroupIssues = new Dictionary<string, List<DetectedIssue>>();
        
        var dbGroups = GetDatabaseGroups(reports);

        foreach (var (databaseName, nodes) in dbGroups)
        {
            if (databaseGroupIssues.TryGetValue(databaseName, out var dbGroupIssues) == false)
            {
                dbGroupIssues = new List<DetectedIssue>();
                databaseGroupIssues.Add(databaseName, dbGroupIssues);
            }
            
            var indexesReports = reports.Where(x => nodes.Contains(x.NodeTag))
                .ToDictionary(x => x.NodeTag, x => x.ForDatabase(databaseName).IndexesInfo);
            
            IndexesInfoAnalyzer.DetectIssuesInDatabaseGroup(indexesReports, ref dbGroupIssues);
        }
        
        return databaseGroupIssues.Where(x => x.Value.Count > 0).ToDictionary();
    }

    private static Dictionary<string, List<string>> GetDatabaseGroups(List<DebugPackageNodeReport> reports)
    {
        var databasesOnNodes = new Dictionary<string, List<string>>();

        foreach (var report in reports)
        {
            foreach (var dbReport in report.Databases)
            {
                if (databasesOnNodes.TryGetValue(dbReport.DatabaseName, out var nodes) == false)
                {
                    nodes = [report.NodeTag];
                    
                    databasesOnNodes.Add(dbReport.DatabaseName, nodes);
                }
                else
                {
                    nodes.Add(report.NodeTag);
                }
            }
        }

        return databasesOnNodes;
    }
}
