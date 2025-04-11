using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class CpuUsageAnalysisInfo
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
}
