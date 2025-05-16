using System;
using System.Diagnostics;
using System.Linq;
using Raven.Server.Dashboard.Cluster.Notifications;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class GcInfoAnalyzer : AbstractDebugPackageAnalyzer
{
    private readonly MemoryInfoAnalyzer _memoryAnalyzer;
    private GcInfoPerGcKind _latestGcPerGcKind;

    public GcInfoAnalyzer(MemoryInfoAnalyzer memoryInfoAnalyzer, DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : base(errors, issues)
    {   
        _memoryAnalyzer = memoryInfoAnalyzer;
    }

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        Debug.Assert(_memoryAnalyzer.Analyzed, $"{nameof(MemoryInfoAnalyzer)} should be executed before using it in {nameof(GcInfoAnalyzer)}");
        
        if (serverEntries.TryGetValue<MemoryDebugHandler, GcInfoPerGcKind>(x => x.GcInfo(), out _latestGcPerGcKind))
        {
            var gcRunInfo = _latestGcPerGcKind.Any;

            _memoryAnalyzer.MemoryInfo.Managed.LastGcInfo = new GcInfoPayload.GcMemoryInfo
            {
                Compacted = gcRunInfo.Compacted,
                Concurrent = gcRunInfo.Concurrent,
                Index = gcRunInfo.Index,
                Generation = gcRunInfo.Generation,
                PauseTimePercentage = gcRunInfo.PauseTimePercentage,
                TotalHeapSizeAfterBytes = gcRunInfo.HeapSizeBytes,
                PauseDurationsInMs = gcRunInfo.PauseDurations.Select(x => x.TotalMilliseconds).ToList(),
                Gen0HeapSize = GenHeapSize(gcRunInfo, 0),
                Gen1HeapSize = GenHeapSize(gcRunInfo, 1),
                Gen2HeapSize = GenHeapSize(gcRunInfo, 2),
                LargeObjectHeapSize = GenHeapSize(gcRunInfo, 3),
                PinnedObjectHeapSize = GenHeapSize(gcRunInfo, 4),
            };
        }
        else
        {
            AddWarning("Failed to get GC info");
            return false;
        }

        return true;

        GcInfoPayload.GenerationInfoSize GenHeapSize(GcRunInfo gcRunInfo, int index)
        {
            return new GcInfoPayload.GenerationInfoSize
            {
                FragmentationBeforeBytes = gcRunInfo.GenerationInfo[index].FragmentationBeforeBytes,
                FragmentationAfterBytes = gcRunInfo.GenerationInfo[index].FragmentationAfterBytes,
                SizeBeforeBytes = gcRunInfo.GenerationInfo[index].SizeBeforeBytes,
                SizeAfterBytes = gcRunInfo.GenerationInfo[index].SizeAfterBytes 
            };
        }
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        var longestPause = TimeSpan.MinValue;

        bool highManagedHeapIssueWasDetected = false;
        bool highGcPauseTimeIssueWasDetected = false;

        foreach (var gcRun in new[] { _latestGcPerGcKind.Ephemeral, _latestGcPerGcKind.Background, _latestGcPerGcKind.FullBlocking })
        {
            string gcKind = "";
                
            if (gcRun == _latestGcPerGcKind.Background)
                gcKind = "Background";
            else if (gcRun == _latestGcPerGcKind.Ephemeral)
                gcKind = "Ephemeral";
            else if (gcRun == _latestGcPerGcKind.FullBlocking)
                gcKind = "Full blocking";
            
            if (gcRun.PauseDurations is { Count: > 0 })
            {
                var pause = gcRun.PauseDurations[0] > gcRun.PauseDurations[1] ? gcRun.PauseDurations[0] : gcRun.PauseDurations[1];

                if (pause > TimeSpan.FromSeconds(1))
                {
                    issues.ServerIssues.Add(
                        new DetectedIssue("Long GC pause detected",
                            $"GC pause duration was {pause:g} " +
                            $"({GetGcRunInfo(gcKind, gcRun)})",
                            IssueSeverity.Warning, IssueCategory.Server));
                }

                if (pause > longestPause)
                    longestPause = pause;
            }

            if (gcRun.PauseTimePercentage > 5 && highGcPauseTimeIssueWasDetected == false)
            {
                var issueSeverity = gcRun.PauseTimePercentage >= 10 ? IssueSeverity.Error : IssueSeverity.Warning;

                issues.ServerIssues.Add(
                    new DetectedIssue("High GC pause time percentage",
                        $"GC pause time percentage was {gcRun.PauseTimePercentage:F1}% " +
                        $"({GetGcRunInfo(gcKind, gcRun)})",
                        issueSeverity, IssueCategory.Server));

                highGcPauseTimeIssueWasDetected = true;
            }

            double managedHeapFragmentationPercentage = (double)gcRun.FragmentedBytes / gcRun.HeapSizeBytes * 100;

            if (managedHeapFragmentationPercentage > 40 &&
                highManagedHeapIssueWasDetected == false) // don't repeat the same alert multiple times
            {
                var severity = managedHeapFragmentationPercentage > 50 ? IssueSeverity.Error : IssueSeverity.Warning;

                
                
                issues.ServerIssues.Add(
                    new DetectedIssue("High managed heap fragmentation",
                        $"Managed heap fragmentation was {managedHeapFragmentationPercentage:F1}% when the last {GetGcRunInfo(gcKind, gcRun)} has occurred",
                        severity, IssueCategory.Server));
                
                highManagedHeapIssueWasDetected = true;
            }
        }
    }

    private string GetGcRunInfo(string gcKind, GcRunInfo info)
    {
        var message = $"{gcKind} GC run #{info.Index} (Gen {info.Generation}";
        
        if (info.Compacted)
            message += ", compacted";
        
        if (info.Concurrent)
            message += ", concurrent";

        message += ")";
        
        return message;
    }
}
