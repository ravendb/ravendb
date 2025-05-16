using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class CpuUsageAnalysisInfo : IDynamicJson
{
    public long ProcessorAffinity { get; set; }
    public TimeSpan PrivilegedProcessorTime { get; set; }
    public TimeSpan TotalProcessorTime { get; set; }
    public TimeSpan UserProcessorTime { get; set; }
    public double? CurrentMachineCpuUsage { get; set; }
    public double? CurrentCpuUsage { get; set; }
    public double? AverageCpuUsage { get; set; }
    public double? KernelTimePercentage { get; set; }
    public List<string> TopCurrentCpuUsageThreads { get; set; }
    public List<string> TopOverallCpuUsageThreads { get; set; }
    public int? UtilizedCores { get; set; }
    public int? NumberOfCores { get; set; }
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(UtilizedCores)] = UtilizedCores,
            [nameof(NumberOfCores)] = NumberOfCores,
            [nameof(CurrentCpuUsage)] = CurrentCpuUsage,
            [nameof(AverageCpuUsage)] = AverageCpuUsage,
            [nameof(CurrentMachineCpuUsage)] = CurrentMachineCpuUsage,
            [nameof(ProcessorAffinity)] = ProcessorAffinity,
            [nameof(PrivilegedProcessorTime)] = PrivilegedProcessorTime,
            [nameof(TotalProcessorTime)] = TotalProcessorTime,
            [nameof(UserProcessorTime)] = UserProcessorTime,
            [nameof(KernelTimePercentage)] = KernelTimePercentage,
            [nameof(TopCurrentCpuUsageThreads)] = TopCurrentCpuUsageThreads != null ? new DynamicJsonArray(TopCurrentCpuUsageThreads) : null,
            [nameof(TopOverallCpuUsageThreads)] = TopOverallCpuUsageThreads != null ? new DynamicJsonArray(TopOverallCpuUsageThreads) : null,
        };
    }
}
