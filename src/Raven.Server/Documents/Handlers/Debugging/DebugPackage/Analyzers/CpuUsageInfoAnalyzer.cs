using System;
using System.Diagnostics;
using System.Text.Json;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class CpuUsageInfoAnalyzer : AbstractDebugPackageAnalyzer
{
    private readonly MachineInfoAnalyzer _machineAnalyzer;
    private readonly BasicServerInfoAnalyzer _basicServerInfoAnalyzer;

    public CpuUsageInfoAnalyzer(MachineInfoAnalyzer machineAnalyzer, BasicServerInfoAnalyzer basicServerInfoAnalyzer,  DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : base(errors, issues)
    {
        _machineAnalyzer = machineAnalyzer;
        _basicServerInfoAnalyzer = basicServerInfoAnalyzer;
    }
    
    public CpuUsageAnalysisInfo CpuUsageInfo { get; private set; }

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        Debug.Assert(_machineAnalyzer.Analyzed, $"{nameof(MachineInfoAnalyzer)} should be executed before using in {nameof(CpuUsageInfoAnalyzer)}");
        Debug.Assert(_basicServerInfoAnalyzer.Analyzed, $"{nameof(BasicServerInfoAnalyzer)} should be executed before using in {nameof(CpuUsageInfoAnalyzer)}");
        
        if (serverEntries.TryGetEntry<ProcStatsHandler>(x => x.CpuStats(), out var cpuStatsEntry) &&
            cpuStatsEntry.TryGetJson("CpuStats", out var cpuStatsJsonArray))
        {
            if (cpuStatsJsonArray.ValueKind == JsonValueKind.Array && cpuStatsJsonArray.GetArrayLength() == 1)
            {
                var cpuStats = cpuStatsJsonArray[0].Deserialize<ProcStatsHandler.Stats>();

                CpuUsageInfo = new CpuUsageAnalysisInfo
                {
                    TotalProcessorTime = cpuStats.TotalProcessorTime,
                    PrivilegedProcessorTime = cpuStats.PrivilegedProcessorTime,
                    UserProcessorTime = cpuStats.UserProcessorTime,
                    NumberOfCores = _machineAnalyzer.MachineInfo.NumberOfCores,
                    UtilizedCores = _machineAnalyzer.MachineInfo.UtilizedCores
                };

                if (_basicServerInfoAnalyzer.BasicServerInfo.UpTime != null && _machineAnalyzer.MachineInfo?.NumberOfCores != null)
                {
                    int processorCount = _machineAnalyzer.MachineInfo.NumberOfCores.Value;

                    var totalAvailableProcessorTime = new TimeSpan(_basicServerInfoAnalyzer.BasicServerInfo.UpTime.Value.Ticks * processorCount);

                    CpuUsageInfo.AverageCpuUsage = Math.Round(CpuUsageInfo.TotalProcessorTime.TotalMilliseconds / totalAvailableProcessorTime.TotalMilliseconds * 100, 1);
                }
            }
            else
            {
                AddWarning("Invalid CPU stats JSON");
                return false;
            }
        }
        else
        {
            AddWarning("Could not retrieve CPU stats");
            return false;
        }

        return true;
    }
    
    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        double kernelTimePercentage = CpuUsageInfo.PrivilegedProcessorTime.TotalMilliseconds / CpuUsageInfo.TotalProcessorTime.TotalMilliseconds * 100;

        if (kernelTimePercentage > 30)
        {
            issues.ServerIssues.Add(
                new DetectedIssue("High CPU kernel time detected",
                    $"RavenDB process is spending {kernelTimePercentage:F0}% of time in kernel",
                    kernelTimePercentage >= 50 ? IssueSeverity.Error : IssueSeverity.Warning, IssueCategory.Server));
        }

        CpuUsageInfo.KernelTimePercentage = kernelTimePercentage;
    }
}
