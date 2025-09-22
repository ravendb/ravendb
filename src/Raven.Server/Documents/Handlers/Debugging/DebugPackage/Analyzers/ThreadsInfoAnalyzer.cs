using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Server.Dashboard;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class ThreadsInfoAnalyzer : AbstractDebugPackageAnalyzer
{
    private readonly CpuUsageInfoAnalyzer _cpuUsageAnalyzer;

    public ThreadsInfoAnalyzer(CpuUsageInfoAnalyzer cpuUsageAnalyzer, DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : base(errors, issues)
    {
        _cpuUsageAnalyzer = cpuUsageAnalyzer;
    }

    public ThreadsAnalysisInfo ThreadsInfo { get; private set; }

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        Debug.Assert(_cpuUsageAnalyzer.Analyzed, $"{nameof(CpuUsageInfoAnalyzer)} should be executed before using it in {nameof(ThreadsInfoAnalyzer)}");

        if (serverEntries.TryGetValue<ThreadsHandler, ThreadsInfo>(x => x.RunawayThreads(), "Runaway Threads", out var threadsInfo) == false)
        {
            AddWarning("Could not retrieve threads info");
            return false;
        }

        ThreadsInfo = new ThreadsAnalysisInfo { Threads = threadsInfo };

        _cpuUsageAnalyzer.CpuUsageInfo.CurrentCpuUsage = threadsInfo.ProcessCpuUsage;
        _cpuUsageAnalyzer.CpuUsageInfo.CurrentMachineCpuUsage = threadsInfo.CpuUsage;

        if (serverEntries.TryGetEntry<ThreadsHandler>(x => x.StackTrace(), out DebugPackageEntries.Entry stackTracesEntry))
        {
            ThreadsInfo.StackTracesEntry = stackTracesEntry;
        }
        
        var threadsByOverallCpuTime = threadsInfo.List
            .Where(x => x.TotalProcessorTime > TimeSpan.FromMinutes(1))
            .OrderByDescending(x => x.TotalProcessorTime)
            .Take(5)
            .ToList();

        var threadsByCurrentCpuUsage = threadsInfo.List
            .Where(x => x.CpuUsage > 1)
            .OrderByDescending(x => x.CpuUsage)
            .Take(5)
            .ToList();

        _cpuUsageAnalyzer.CpuUsageInfo.TopCurrentCpuUsageThreads = threadsByCurrentCpuUsage.Select(x => GetThreadDescription(x) + $" - {x.CpuUsage:F0}%").ToList();
        
        _cpuUsageAnalyzer.CpuUsageInfo.TopOverallCpuUsageThreads = threadsByOverallCpuTime.Select(x =>
        {
            double threadPercentOfTotalCpu = x.TotalProcessorTime.TotalMilliseconds / _cpuUsageAnalyzer.CpuUsageInfo.TotalProcessorTime.TotalMilliseconds * 100;

            return GetThreadDescription(x) + $" - {threadPercentOfTotalCpu:F0}%";
        }).ToList();

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        var threadsInfo = ThreadsInfo.Threads;

        if (threadsInfo.ThreadsCount > 500)
        {
            var severity = threadsInfo.ThreadsCount > 1000 ? IssueSeverity.Warning : IssueSeverity.Info;
            issues.ServerIssues.Add(
                new DetectedIssue("High number of threads",
                    $"RavenDB process has {threadsInfo.ThreadsCount} threads",
                    severity, IssueCategory.Server));
        }

        foreach (var thread in threadsInfo.List)
        {
            double threadPercentOfTotalCpu = thread.TotalProcessorTime.TotalMilliseconds / _cpuUsageAnalyzer.CpuUsageInfo.TotalProcessorTime.TotalMilliseconds * 100;

            if (threadPercentOfTotalCpu > 10)
            {
                var severity = threadPercentOfTotalCpu > 20 ? IssueSeverity.Info : IssueSeverity.Warning;
                var description = GetThreadDescription(thread) + $" has consumed {threadPercentOfTotalCpu:F0}% of the total CPU time used by RavenDB process";

                issues.ServerIssues.Add(new DetectedIssue("High CPU usage detected by single thread", description, severity, IssueCategory.Server));
            }

            if (threadPercentOfTotalCpu > 1 && thread.TotalProcessorTime > TimeSpan.FromMinutes(1)) // do it only for non-trivial threads
            {
                double threadKernelTimePercentage = thread.PrivilegedProcessorTime.TotalMilliseconds / thread.TotalProcessorTime.TotalMilliseconds * 100;

                if (threadKernelTimePercentage > 40)
                {
                    var description = GetThreadDescription(thread) + $" has spent {threadKernelTimePercentage:F0}% of its execution time ({thread.TotalProcessorTime:g}) " +
                                      "running code inside OS kernel";

                    var severity = threadKernelTimePercentage >= 50 ? IssueSeverity.Error : IssueSeverity.Warning;

                    issues.ServerIssues.Add(new DetectedIssue("High kernel mode CPU usage detected on thread", description, severity, IssueCategory.Server));
                }
            }
        }
    }

    private string GetThreadDescription(ThreadInfo thread)
    {
        var description = $"Thread '{thread.Name}' (TID #{thread.Id}";

        if (thread.ManagedThreadId != null)
            description += $", managed TID #{thread.ManagedThreadId}";

        description += ")";

        return description;
    }
}
