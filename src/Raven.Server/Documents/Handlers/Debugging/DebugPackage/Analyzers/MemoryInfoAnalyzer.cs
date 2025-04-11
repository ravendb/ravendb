using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Errors;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Issues;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Memory;
using Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results.Threads;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers;

public class MemoryInfoAnalyzer : AbstractDebugPackageAnalyzer
{
    public MemoryInfoAnalyzer(DebugPackageAnalyzeErrors errors, DebugPackageAnalysisIssues issues) : base(errors, issues)
    {
    }

    public MemoryAnalysisInfo MemoryInfo { get; } = new();

    protected override bool RetrieveAnalyzerInfo(DebugPackageEntries serverEntries)
    {
        if (serverEntries.TryGetValue<MemoryDebugHandler, MemoryDebugHandler.MemoryInfo>(x => x.MemoryStats(),
                nameof(Sparrow.Server.LowMemory.MemoryInformation), out MemoryDebugHandler.MemoryInfo memoryInfo) == false)
        {
            AddError("Failed to retrieve memory information");

            return false;
        }

        MemoryInfo.PhysicalMemory = memoryInfo.PhysicalMemory;
        MemoryInfo.WorkingSet = memoryInfo.WorkingSet;
        MemoryInfo.AvailableMemory = memoryInfo.AvailableMemory;
        MemoryInfo.AvailableMemoryForProcessing = memoryInfo.AvailableMemoryForProcessing;

        MemoryInfo.Managed.ManagedAllocations = memoryInfo.ManagedAllocations;
        MemoryInfo.Managed.LuceneManagedAllocationsForTermCache = memoryInfo.LuceneManagedAllocationsForTermCache;

        MemoryInfo.Unmanaged.UnmanagedAllocations = memoryInfo.UnmanagedAllocations;
        MemoryInfo.Unmanaged.LuceneUnmanagedAllocationsForTermCache = memoryInfo.LuceneUnmanagedAllocationsForTermCache;
        MemoryInfo.Unmanaged.LuceneUnmanagedAllocationsForSorting = memoryInfo.LuceneUnmanagedAllocationsForSorting;

        MemoryInfo.Unmanaged.EncryptionBuffersInUse = memoryInfo.EncryptionBuffersInUse;
        MemoryInfo.Unmanaged.EncryptionBuffersPool = memoryInfo.EncryptionBuffersPool;
        MemoryInfo.Unmanaged.EncryptionLockedMemory = memoryInfo.EncryptionLockedMemory;

        MemoryInfo.MemoryMapped = memoryInfo.MemoryMapped;
        MemoryInfo.DirtyMemory = memoryInfo.DirtyMemory;

        MemoryInfo.IsHighDirty = memoryInfo.IsHighDirty;

        if (serverEntries.TryGetValue<MemoryDebugHandler, List<MemoryDebugHandler.ThreadAllocations>>(x => x.MemoryStats(),
                "Threads", out var threadAllocations))
        {
            MemoryInfo.Unmanaged.ThreadAllocations = new List<ThreadAllocations>(threadAllocations.Count);

            foreach (var item in threadAllocations)
            {
                var allocation = new ThreadAllocations
                {
                    ThreadName = item.Name,
                    Allocations = item.Allocations,
                    HumaneAllocations = item.HumaneAllocations
                };
                
                if (item.Ids != null)
                {
                    allocation.ThreadIds = item.Ids.Select(x => new ThreadId { Id = x.Id!.Value, ManagedThreadId = x.ManagedThreadId!.Value }).ToList();
                }
                else
                {
                    allocation.ThreadIds =
                    [
                        new() { Id = item.Id!.Value, ManagedThreadId = item.ManagedThreadId!.Value }
                    ];
                }
            }
        }

        return true;
    }

    protected override void DetectIssues(DebugPackageAnalysisIssues issues)
    {
        var unmanagedMemory = Size.Parse(MemoryInfo.Unmanaged.UnmanagedAllocations);
        var managedMemory = Size.Parse(MemoryInfo.Managed.ManagedAllocations);
        var availableMemoryForProcessing = Size.Parse(MemoryInfo.AvailableMemoryForProcessing);
        var physicalMemory = Size.Parse(MemoryInfo.PhysicalMemory);

        if (unmanagedMemory.SizeInBytes > physicalMemory.SizeInBytes / 2)
        {
            issues.ServerIssues.Add(
                new DetectedIssue("High unmanaged memory utilization",
                    $"Unmanaged memory usage ({unmanagedMemory.HumaneSize}) is more than 50% of installed memory ({physicalMemory.HumaneSize})",
                    IssueSeverity.Warning, IssueCategory.Server));
        }

        if (managedMemory.SizeInBytes > physicalMemory.SizeInBytes / 2)
        {
            issues.ServerIssues.Add(
                new DetectedIssue("High managed memory utilization",
                    $"Managed memory usage ({managedMemory.HumaneSize}) is more than 50% of installed memory ({physicalMemory.HumaneSize})",
                    IssueSeverity.Warning, IssueCategory.Server));
        }

        var lowMemoryLimit = Sparrow.Size.Min(
            new Sparrow.Size(2, Sparrow.SizeUnit.Gigabytes),
            new Sparrow.Size(physicalMemory.SizeInBytes, Sparrow.SizeUnit.Bytes) / 10);

        if (availableMemoryForProcessing.SizeInBytes <= lowMemoryLimit.GetValue(Sparrow.SizeUnit.Bytes))
        {
            issues.ServerIssues.Add(
                new DetectedIssue("Low available memory for processing",
                    $"Available memory for processing is low - {availableMemoryForProcessing.HumaneSize} " +
                    $"(installed memory: {physicalMemory.HumaneSize}, available memory: {MemoryInfo.AvailableMemory}, working set: {MemoryInfo.WorkingSet})",
                    IssueSeverity.Warning, IssueCategory.Server));
        }

        var encryptionBuffersInUse = Size.Parse(MemoryInfo.Unmanaged.EncryptionBuffersInUse);
        var encryptionBuffersPool = Size.Parse(MemoryInfo.Unmanaged.EncryptionBuffersPool);

        if (encryptionBuffersPool.SizeInBytes > 0 || encryptionBuffersInUse.SizeInBytes > 0)
        {
            issues.ServerIssues.Add(
                new DetectedIssue("Encryption feature in use",
                    $"Database encryption is enabled - Buffers in use: {encryptionBuffersInUse.HumaneSize}, Pool size: {encryptionBuffersPool.HumaneSize}, " +
                    $"Locked memory: {MemoryInfo.Unmanaged.EncryptionLockedMemory}",
                    IssueSeverity.Info, IssueCategory.Server));
        }
    }
}
