using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.Monitoring.Snmp.Objects.Server;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.OpenTelemetry;

public class ServerMetrics : MetricsBase
{
    private static readonly Lazy<Meter> ServerStatisticsMeter = new(() => new(Constants.ServerWideMeterName, "1.0.0", [new(Constants.Tags.NodeTag, NodeTag)]));

    private readonly SemaphoreSlim _locker = new(1, 1);
    private readonly RavenServer _server;
    private readonly KeyValuePair<string,object> _nodeTag;
    private ServerStore ServerStore => _server.ServerStore;

    public ServerMetrics(RavenServer server, string nodeTag) : base(server.Configuration.Monitoring.OpenTelemetry)
    {
        _nodeTag = Constants.Tags.CreateNodeTagLabel(nodeTag);
        _server = server;
        RegisterServerMemory();
        RegisterTotalDatabase();
        RegisterServerStorage();
    }

    private void RegisterServerMemory()
    {
        CreateObservableGaugeWithTags<int, ProcessCpu>(
            name: Constants.ServerWide.ProcessCpu,
            observeValue: new ProcessCpu(_server.MetricCacher, _server.CpuUsageCalculator, _nodeTag),
            description: "Process CPU usage in %",
            family: null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<int, MachineCpu>(
            name: Constants.ServerWide.MachineCpu,
            observeValue: new MachineCpu(_server.MetricCacher, _server.CpuUsageCalculator, _nodeTag),
            description: "Machine CPU usage in %",
            family: null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<byte, ServerLowMemoryFlag>(
            name: Constants.ServerWide.MachineCpu,
            observeValue: new ServerLowMemoryFlag(_nodeTag),
            description: "Server low memory flag value",
            family: null,
            meter: ServerStatisticsMeter);
        
        CreateObservableGaugeWithTags<long, ServerTotalMemory>(
            name: Constants.ServerWide.ServerTotalMemory,
            observeValue: new ServerTotalMemory(_server.MetricCacher, _nodeTag),
            "Server allocated memory in MB",
            null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<long, ServerTotalSwapSize>(
            name: Constants.ServerWide.ServerTotalSwapSize,
            observeValue: new ServerTotalSwapSize(_server.MetricCacher, _nodeTag),
            description: "Server total swap size in MB",
            family: null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<long, ServerTotalSwapUsage>(
            name: Constants.ServerWide.ServerTotalSwapUsage,
            observeValue: new ServerTotalSwapUsage(_server.MetricCacher, _nodeTag),
            description: "Server total swap usage in MB",
            family: null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<long, ServerDirtyMemory>(
            name: Constants.ServerWide.ServerDirtyMemory,
            observeValue: new ServerDirtyMemory(_nodeTag),
            description: "Dirty Memory that is used by the scratch buffers in MB",
            family: null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<long, ServerWorkingSetSwapUsage>(
            name: Constants.ServerWide.ServerWorkingSetSwapUsage,
            observeValue: new ServerWorkingSetSwapUsage(_server.MetricCacher, _nodeTag),
            description: "Dirty Memory that is used by the scratch buffers in MB",
            family: null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<long, ServerManagedMemory>(
            name: Constants.ServerWide.ServerManagedMemory,
            observeValue: new ServerManagedMemory(_nodeTag),
            description: "Server managed memory size in MB",
            family: null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<long, ServerUnmanagedMemory>(
            name: Constants.ServerWide.ServerUnmanagedMemory,
            observeValue: new ServerUnmanagedMemory(_nodeTag),
            description: "Server unmanaged memory size in MB",
            family: null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<long, ServerEncryptionBuffersMemoryInUse>(
            name: Constants.ServerWide.ServerEncryptionBuffersMemoryInUse,
            observeValue: new ServerEncryptionBuffersMemoryInUse(_nodeTag),
            description: "Server encryption buffers memory being in use in MB",
            family: null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<long, ServerEncryptionBuffersMemoryInPool>(
            name: Constants.ServerWide.ServerEncryptionBuffersMemoryInPool,
            observeValue: new ServerEncryptionBuffersMemoryInPool(_nodeTag),
            description: "Server encryption buffers memory being in pool in MB",
            family: null,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<long, ServerAvailableMemoryForProcessing>(
            name: Constants.ServerWide.ServerAvailableMemoryForProcessing,
            observeValue: new ServerAvailableMemoryForProcessing(_server.MetricCacher, _nodeTag),
            description: "Server encryption buffers memory being in pool in MB",
            family: null,
            meter: ServerStatisticsMeter);
    }

    private void RegisterTotalDatabase()
    {
        CreateObservableGaugeWithTags<int, TotalDatabaseCountOfStaleIndexes>(
            name: Constants.ServerWide.TotalDatabaseCountOfStaleIndexes,
            observeValue: new TotalDatabaseCountOfStaleIndexes(ServerStore, _nodeTag),
            description: "Number of stale indexes in all loaded databases",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounterWithTags<int, TotalDatabaseNumberOfIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfIndexes,
            observeValue: new TotalDatabaseNumberOfIndexes(ServerStore, _nodeTag),
            description: "Number of indexes in all loaded databases",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounterWithTags<int, TotalDatabaseNumberOfErrorIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfErrorIndexes,
            observeValue: new TotalDatabaseNumberOfErrorIndexes(ServerStore, _nodeTag),
            description: "Number of error indexes in all loaded databases",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounterWithTags<int, TotalDatabaseNumberOfFaultyIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfFaultyIndexes,
            observeValue: new TotalDatabaseNumberOfFaultyIndexes(ServerStore, _nodeTag),
            description: "Number of faulty indexes in all loaded databases",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<int, TotalDatabaseWritesPerSecond>(
            name: Constants.ServerWide.TotalDatabaseWritesPerSecond,
            observeValue: new TotalDatabaseWritesPerSecond(ServerStore, _nodeTag),
            description: "Number of writes (documents, attachments, counters) in all loaded databases",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGaugeWithTags<int, TotalDatabaseDataWrittenPerSecond>(
            name: Constants.ServerWide.TotalDatabaseDataWrittenPerSecond,
            observeValue: new TotalDatabaseDataWrittenPerSecond(ServerStore, _nodeTag),
            description: "Number of bytes written (documents, attachments, counters) in all loaded databases",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);
    }

    private void RegisterServerStorage()
    {
        CreateObservableGaugeWithTags<long, ServerStorageUsedSize>(
            name: Constants.ServerWide.ServerStorageUsedSize,
            observeValue: new ServerStorageUsedSize(ServerStore, _nodeTag),
            description: "Server storage used size in MB",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);        
        
        CreateObservableGaugeWithTags<long, ServerStorageTotalSize>(
            name: Constants.ServerWide.ServerStorageTotalSize,
            observeValue: new ServerStorageTotalSize(ServerStore, _nodeTag),
            description: "Server storage total size in MB",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);
        
        CreateObservableGaugeWithTags<long, ServerStorageDiskRemainingSpace>(
            name: Constants.ServerWide.ServerStorageDiskRemainingSpace,
            observeValue: new ServerStorageDiskRemainingSpace(ServerStore, _nodeTag),
            description: "Remaining server storage disk space in MB",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);
        
        CreateObservableGaugeWithTags<int, ServerStorageDiskRemainingSpacePercentage>(
            name: Constants.ServerWide.ServerStorageDiskRemainingSpacePercentage,
            observeValue: new ServerStorageDiskRemainingSpacePercentage(ServerStore, _nodeTag),
            description: "Remaining server storage disk space in %",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);
        
        CreateObservableGaugeWithTags<int, ServerStorageDiskIosReadOperations>(
            name: Constants.ServerWide.ServerStorageDiskIosReadOperations,
            observeValue: new ServerStorageDiskIosReadOperations(ServerStore, _nodeTag),
            description: "IO read operations per second",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);
        
        CreateObservableGaugeWithTags<int, ServerStorageDiskIosWriteOperations>(
            name: Constants.ServerWide.ServerStorageDiskIosWriteOperations,
            observeValue: new ServerStorageDiskIosWriteOperations(ServerStore, _nodeTag),
            description: "IO write operations per second",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);
        
        CreateObservableGaugeWithTags<long, ServerStorageDiskReadThroughput>(
            name: Constants.ServerWide.ServerStorageDiskReadThroughput,
            observeValue: new ServerStorageDiskReadThroughput(ServerStore, _nodeTag),
            description: "Read throughput in kilobytes per second",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);
        
        CreateObservableGaugeWithTags<long, ServerStorageDiskWriteThroughput>(
            name: Constants.ServerWide.ServerStorageDiskWriteThroughput,
            observeValue: new ServerStorageDiskWriteThroughput(ServerStore, _nodeTag),
            description: "Write throughput in kilobytes per second",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);
        
        CreateObservableGaugeWithTags<long, ServerStorageDiskQueueLength>(
            name: Constants.ServerWide.ServerStorageDiskQueueLength,
            observeValue: new ServerStorageDiskQueueLength(ServerStore, _nodeTag),
            description: "Queue length",
            family: Configuration.ServerInstruments,
            meter: ServerStatisticsMeter);
    }
}
