using System;
using System.Diagnostics.Metrics;
using System.Linq;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.Monitoring.Snmp.Objects.Server;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.OpenTelemetry;

public class ServerMetrics : MetricsBase
{
    private static readonly Lazy<Meter> ServerStatisticsMeter = new(() => new(Constants.ServerWideMeterName, "1.0.0"));
    private readonly RavenServer _server;
    private ServerStore ServerStore => _server.ServerStore;

    public ServerMetrics(RavenServer server) : base(server.Configuration.Monitoring.OpenTelemetry)
    {
        _server = server;
        RegisterRequestInstruments();
        RegisterCpuCreditsInstruments();
        RegisterServerHardwareInstruments();
        RegisterTotalDatabaseInstruments();
        RegisterStorageInstruments();
        RegisterGc(GCKind.Any);
        RegisterGc(GCKind.Background);
        RegisterGc(GCKind.Ephemeral);
        RegisterGc(GCKind.FullBlocking);
    }

    private void RegisterRequestInstruments()
    {
        if (Configuration.Requests == false)
            return;

        var metrics = _server.ServerStore.Server.Metrics;
        CreateObservableUpDownCounter<int, ServerConcurrentRequests>(
            name: Constants.ServerWide.ServerConcurrentRequests,
            observeValueFactory: () => new ServerConcurrentRequests(metrics),
            family: configuration => configuration.RequestsInstruments,
            meter: ServerStatisticsMeter
        );
        
        CreateObservableUpDownCounter<int, ServerTotalRequests>(
            name: Constants.ServerWide.ServerTotalRequests,
            observeValueFactory: () => new ServerTotalRequests(metrics),
            family: configuration => configuration.RequestsInstruments,
            meter: ServerStatisticsMeter
        );

        const string ServerRequestsPerSecondDescription = "Number of requests per second.";
        CreateObservableGaugeWithTags<int, ServerRequestsPerSecond>(
            name: Constants.ServerWide.ServerRequestsPerSecond,
            observeValueFactory: () => new ServerRequestsPerSecond(metrics, ServerRequestsPerSecond.RequestRateType.OneMinute),
            family: configuration => configuration.RequestsInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: ServerRequestsPerSecondDescription
        );

        CreateObservableGaugeWithTags<int, ServerRequestsPerSecond>(
            name: Constants.ServerWide.ServerRequestsPerSecond,
            observeValueFactory: () => new ServerRequestsPerSecond(metrics, ServerRequestsPerSecond.RequestRateType.FiveSeconds),
            family: configuration => configuration.RequestsInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: ServerRequestsPerSecondDescription
        );

        CreateObservableGauge<int, ServerRequestAverageDuration>(
            name: Constants.ServerWide.ServerRequestAverageDuration,
            observeValueFactory: () => new ServerRequestAverageDuration(metrics),
            family: configuration => configuration.RequestsInstruments,
            meter: ServerStatisticsMeter
        );

        CreateObservableGauge<long, TcpActiveConnections>(
            name: Constants.ServerWide.TcpActiveConnections,
            observeValueFactory: () => new TcpActiveConnections(),
            family: configuration => configuration.RequestsInstruments,
            meter: ServerStatisticsMeter
        );
    }

    private void RegisterCpuCreditsInstruments()
    {
        if (Configuration.CPUCredits == false)
            return;

        CreateObservableUpDownCounter<int, CpuCreditsBase>(
            name: Constants.ServerWide.CpuCreditsBase,
            observeValueFactory: () => new CpuCreditsBase(ServerStore.Server.CpuCreditsBalance),
            family: configuration => configuration.CPUCreditsInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, CpuCreditsMax>(
            name: Constants.ServerWide.CpuCreditsMax,
            observeValueFactory: () => new CpuCreditsMax(ServerStore.Server.CpuCreditsBalance),
            family: configuration => configuration.CPUCreditsInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, CpuCreditsRemaining>(
            name: Constants.ServerWide.CpuCreditsRemaining,
            observeValueFactory: () => new CpuCreditsRemaining(ServerStore.Server.CpuCreditsBalance),
            family: configuration => configuration.CPUCreditsInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<double, CpuCreditsCurrentConsumption>(
            name: Constants.ServerWide.CpuCreditsCurrentConsumption,
            observeValueFactory: () => new CpuCreditsCurrentConsumption(ServerStore.Server.CpuCreditsBalance),
            family: configuration => configuration.CPUCreditsInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<byte, CpuCreditsBackgroundTasksAlertRaised>(
            name: Constants.ServerWide.CpuCreditsBackgroundTasksAlertRaised,
            observeValueFactory: () => new CpuCreditsBackgroundTasksAlertRaised(ServerStore.Server.CpuCreditsBalance),
            family: configuration => configuration.CPUCreditsInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<byte, CpuCreditsFailoverAlertRaised>(
            name: Constants.ServerWide.CpuCreditsFailoverAlertRaised,
            observeValueFactory: () => new CpuCreditsFailoverAlertRaised(ServerStore.Server.CpuCreditsBalance),
            family: configuration => configuration.CPUCreditsInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<byte, CpuCreditsAlertRaised>(
            name: Constants.ServerWide.CpuCreditsAlertRaised,
            observeValueFactory: () => new CpuCreditsAlertRaised(ServerStore.Server.CpuCreditsBalance),
            family: configuration => configuration.CPUCreditsInstruments,
            meter: ServerStatisticsMeter);
    }

    private void RegisterServerHardwareInstruments()
    {
        if (Configuration.Hardware == false)
            return;

        CreateObservableGauge<int, ProcessCpu>(
            name: Constants.ServerWide.ProcessCpu,
            observeValueFactory: () => new ProcessCpu(_server.MetricCacher, _server.CpuUsageCalculator),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, MachineCpu>(
            name: Constants.ServerWide.MachineCpu,
            observeValueFactory: () => new MachineCpu(_server.MetricCacher, _server.CpuUsageCalculator),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, IoWait>(
            name: Constants.ServerWide.IoWait,
            observeValueFactory: () => new IoWait(_server.MetricCacher, _server.CpuUsageCalculator),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<byte, ServerLowMemoryFlag>(
            name: Constants.ServerWide.ServerLowMemoryFlag,
            observeValueFactory: () => new ServerLowMemoryFlag(),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerTotalMemory>(
            name: Constants.ServerWide.ServerTotalMemory,
            observeValueFactory: () => new ServerTotalMemory(_server.MetricCacher),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerTotalSwapSize>(
            name: Constants.ServerWide.ServerTotalSwapSize,
            observeValueFactory: () => new ServerTotalSwapSize(_server.MetricCacher),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerTotalSwapUsage>(
            name: Constants.ServerWide.ServerTotalSwapUsage,
            observeValueFactory: () => new ServerTotalSwapUsage(_server.MetricCacher),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerDirtyMemory>(
            name: Constants.ServerWide.ServerDirtyMemory,
            observeValueFactory: () => new ServerDirtyMemory(),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerWorkingSetSwapUsage>(
            name: Constants.ServerWide.ServerWorkingSetSwapUsage,
            observeValueFactory: () => new ServerWorkingSetSwapUsage(_server.MetricCacher),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerManagedMemory>(
            name: Constants.ServerWide.ServerManagedMemory,
            observeValueFactory: () => new ServerManagedMemory(),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerUnmanagedMemory>(
            name: Constants.ServerWide.ServerUnmanagedMemory,
            observeValueFactory: () => new ServerUnmanagedMemory(),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerEncryptionBuffersMemoryInUse>(
            name: Constants.ServerWide.ServerEncryptionBuffersMemoryInUse,
            observeValueFactory: () => new ServerEncryptionBuffersMemoryInUse(),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerEncryptionBuffersMemoryInPool>(
            name: Constants.ServerWide.ServerEncryptionBuffersMemoryInPool,
            observeValueFactory: () => new ServerEncryptionBuffersMemoryInPool(),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerAvailableMemoryForProcessing>(
            name: Constants.ServerWide.ServerAvailableMemoryForProcessing,
            observeValueFactory: () => new ServerAvailableMemoryForProcessing(_server.MetricCacher),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, MachineProcessorCount>(
            name: Constants.ServerWide.MachineProcessorCount,
            observeValueFactory: () => new MachineProcessorCount(),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, MachineAssignedProcessorCount>(
            name: Constants.ServerWide.MachineAssignedProcessorCount,
            observeValueFactory: () => new MachineAssignedProcessorCount(),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, ThreadPoolAvailableWorkerThreads>(
            name: Constants.ServerWide.ThreadPoolAvailableWorkerThreads,
            observeValueFactory: () => new ThreadPoolAvailableWorkerThreads(),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, ThreadPoolAvailableCompletionPortThreads>(
            name: Constants.ServerWide.ThreadPoolAvailableCompletionPortThreads,
            observeValueFactory: () => new ThreadPoolAvailableCompletionPortThreads(),
            family: configuration => configuration.HardwareInstruments,
            meter: ServerStatisticsMeter);
    }

    private void RegisterTotalDatabaseInstruments()
    {
        if (Configuration.TotalDatabases == false)
            return;

        CreateObservableUpDownCounter<int, DatabaseLoadedCount>(
            name: Constants.ServerWide.DatabaseLoadedCount,
            observeValueFactory: () => new DatabaseLoadedCount(_server.ServerStore.DatabasesLandlord),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, DatabaseTotalCount>(
            name: Constants.ServerWide.DatabaseTotalCount,
            observeValueFactory: () => new DatabaseTotalCount(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, DatabaseDisabledCount>(
            name: Constants.ServerWide.DatabaseDisabledCount,
            observeValueFactory: () => new DatabaseDisabledCount(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, DatabaseEncryptedCount>(
            name: Constants.ServerWide.DatabaseEncryptedCount,
            observeValueFactory: () => new DatabaseEncryptedCount(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, DatabaseFaultedCount>(
            name: Constants.ServerWide.DatabaseFaultedCount,
            observeValueFactory: () => new DatabaseFaultedCount(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, DatabaseNodeCount>(
            name: Constants.ServerWide.DatabaseNodeCount,
            observeValueFactory: () => new DatabaseNodeCount(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, TotalDatabaseNumberOfIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfIndexes,
            observeValueFactory: () => new TotalDatabaseNumberOfIndexes(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, TotalDatabaseCountOfStaleIndexes>(
            name: Constants.ServerWide.TotalDatabaseCountOfStaleIndexes,
            observeValueFactory: () => new TotalDatabaseCountOfStaleIndexes(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, TotalDatabaseNumberOfErrorIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfErrorIndexes,
            observeValueFactory: () => new TotalDatabaseNumberOfErrorIndexes(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, TotalDatabaseNumberOfFaultyIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfFaultyIndexes,
            observeValueFactory: () => new TotalDatabaseNumberOfFaultyIndexes(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableUpDownCounter<int, TotalDatabaseNumberOfIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfIndexes,
            observeValueFactory: () => new TotalDatabaseNumberOfIndexes(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, TotalDatabaseMapIndexIndexedPerSecond>(
            name: Constants.ServerWide.TotalDatabaseMapIndexIndexedPerSecond,
            observeValueFactory: () => new TotalDatabaseMapIndexIndexedPerSecond(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, TotalDatabaseMapReduceIndexMappedPerSecond>(
            name: Constants.ServerWide.TotalDatabaseMapReduceIndexMappedPerSecond,
            observeValueFactory: () => new TotalDatabaseMapReduceIndexMappedPerSecond(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, TotalDatabaseMapReduceIndexReducedPerSecond>(
            name: Constants.ServerWide.TotalDatabaseMapReduceIndexReducedPerSecond,
            observeValueFactory: () => new TotalDatabaseMapReduceIndexReducedPerSecond(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, TotalDatabaseWritesPerSecond>(
            name: Constants.ServerWide.TotalDatabaseWritesPerSecond,
            observeValueFactory: () => new TotalDatabaseWritesPerSecond(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, TotalDatabaseDataWrittenPerSecond>(
            name: Constants.ServerWide.TotalDatabaseDataWrittenPerSecond,
            observeValueFactory: () => new TotalDatabaseDataWrittenPerSecond(_server.ServerStore),
            family: configuration => configuration.TotalDatabasesInstruments,
            meter: ServerStatisticsMeter);
    }

    private void RegisterStorageInstruments()
    {
        if (Configuration.ServerStorage == false)
            return;

        CreateObservableGauge<long, ServerStorageUsedSize>(
            name: Constants.ServerWide.ServerStorageUsedSize,
            observeValueFactory: () => new ServerStorageUsedSize(_server.ServerStore),
            family: configuration => configuration.ServerStorageInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerStorageTotalSize>(
            name: Constants.ServerWide.ServerStorageTotalSize,
            observeValueFactory: () => new ServerStorageTotalSize(_server.ServerStore),
            family: configuration => configuration.ServerStorageInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerStorageDiskRemainingSpace>(
            name: Constants.ServerWide.ServerStorageDiskRemainingSpace,
            observeValueFactory: () => new ServerStorageDiskRemainingSpace(_server.ServerStore),
            family: configuration => configuration.ServerStorageInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, ServerStorageDiskRemainingSpacePercentage>(
            name: Constants.ServerWide.ServerStorageDiskRemainingSpacePercentage,
            observeValueFactory: () => new ServerStorageDiskRemainingSpacePercentage(_server.ServerStore),
            family: configuration => configuration.ServerStorageInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, ServerStorageDiskIosReadOperations>(
            name: Constants.ServerWide.ServerStorageDiskIosReadOperations,
            observeValueFactory: () => new ServerStorageDiskIosReadOperations(_server.ServerStore),
            family: configuration => configuration.ServerStorageInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<int, ServerStorageDiskIosWriteOperations>(
            name: Constants.ServerWide.ServerStorageDiskIosWriteOperations,
            observeValueFactory: () => new ServerStorageDiskIosWriteOperations(_server.ServerStore),
            family: configuration => configuration.ServerStorageInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerStorageDiskReadThroughput>(
            name: Constants.ServerWide.ServerStorageDiskReadThroughput,
            observeValueFactory: () => new ServerStorageDiskReadThroughput(_server.ServerStore),
            family: configuration => configuration.ServerStorageInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerStorageDiskWriteThroughput>(
            name: Constants.ServerWide.ServerStorageDiskWriteThroughput,
            observeValueFactory: () => new ServerStorageDiskWriteThroughput(_server.ServerStore),
            family: configuration => configuration.ServerStorageInstruments,
            meter: ServerStatisticsMeter);

        CreateObservableGauge<long, ServerStorageDiskQueueLength>(
            name: Constants.ServerWide.ServerStorageDiskQueueLength,
            observeValueFactory: () => new ServerStorageDiskQueueLength(_server.ServerStore),
            family: configuration => configuration.ServerStorageInstruments,
            meter: ServerStatisticsMeter);
    }

    private void RegisterGc(GCKind gcKind)
    {
        if (Configuration.GC == false)
            return;
        
        if (Configuration.GCKinds != null && Configuration.GCKinds.Contains(gcKind.ToString()) == false)
            return;
        
        var metrics = _server.MetricCacher;
        CreateObservableGaugeWithTags<byte, ServerGcCompacted>(
            name: Constants.ServerWide.GC.ServerGcCompacted,
            observeValueFactory: () => new ServerGcCompacted(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Specifies if this is a compacting GC or not.");
        
        CreateObservableGaugeWithTags<byte, ServerGcConcurrent>(
            name: Constants.ServerWide.GC.ServerGcConcurrent,
            observeValueFactory: () => new ServerGcConcurrent(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Specifies if this is a concurrent GC or not.");
        
        CreateObservableGaugeWithTags<long, ServerGcFinalizationPendingCount>(
            name: Constants.ServerWide.GC.ServerGcFinalizationPendingCount,
            observeValueFactory: () => new ServerGcFinalizationPendingCount(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the number of objects ready for finalization this GC observed.");
        
        CreateObservableGaugeWithTags<long, ServerGcFragmented>(
            name: Constants.ServerWide.GC.ServerGcFragmented,
            observeValueFactory: () => new ServerGcFragmented(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the total fragmentation (in MB) when the last garbage collection occurred.");
        
        CreateObservableGaugeWithTags<int, ServerGcGeneration>(
            name: Constants.ServerWide.GC.ServerGcGeneration,
            observeValueFactory: () => new ServerGcGeneration(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the generation this GC collected.");
        
        CreateObservableGaugeWithTags<long, ServerGcHeapSize>(
            name: Constants.ServerWide.GC.ServerGcHeapSize,
            observeValueFactory: () => new ServerGcHeapSize(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the total heap size (in MB) when the last garbage collection occurred.");
        
        CreateObservableGaugeWithTags<long, ServerGcHighMemoryLoadThreshold>(
            name: Constants.ServerWide.GC.ServerGcHighMemoryLoadThreshold,
            observeValueFactory: () => new ServerGcHighMemoryLoadThreshold(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the high memory load threshold (in MB) when the last garbage collection occurred.");
        
        CreateObservableGaugeWithTags<int, ServerGcIndex>(
            name: Constants.ServerWide.GC.ServerGcIndex,
            observeValueFactory: () => new ServerGcIndex(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "The index of this GC.");
        
        CreateObservableGaugeWithTags<long, ServerGcMemoryLoad>(
            name: Constants.ServerWide.GC.ServerGcMemoryLoad,
            observeValueFactory: () => new ServerGcMemoryLoad(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the memory load (in MB) when the last garbage collection occurred.");
        
        CreateObservableGaugeWithTags<long, ServerGcPauseDurations1>(
            name: Constants.ServerWide.GC.ServerGcPauseDurations1,
            observeValueFactory: () => new ServerGcPauseDurations1(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the pause durations. First item in the array.");
        
        CreateObservableGaugeWithTags<long, ServerGcPauseDurations2>(
            name: Constants.ServerWide.GC.ServerGcPauseDurations2,
            observeValueFactory: () => new ServerGcPauseDurations2(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription:"Gets the pause durations. Second item in the array.");
        
        CreateObservableGaugeWithTags<int, ServerGcPauseTimePercentage>(
            name: Constants.ServerWide.GC.ServerGcPauseTimePercentage,
            observeValueFactory: () => new ServerGcPauseTimePercentage(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the pause time percentage in the GC so far.");
        
        CreateObservableGaugeWithTags<long, ServerGcPinnedObjectsCount>(
            name: Constants.ServerWide.GC.ServerGcPinnedObjectsCount,
            observeValueFactory: () => new ServerGcPinnedObjectsCount(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the number of pinned objects this GC observed.");
        
        CreateObservableGaugeWithTags<long, ServerGcPromoted>(
            name: Constants.ServerWide.GC.ServerGcPromoted,
            observeValueFactory: () => new ServerGcPromoted(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the promoted MB for this GC.");
        
        CreateObservableGaugeWithTags<long, ServerGcTotalAvailableMemory>(
            name: Constants.ServerWide.GC.ServerGcTotalAvailableMemory,
            observeValueFactory: () => new ServerGcTotalAvailableMemory(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the total available memory (in MB) for the garbage collector to use when the last garbage collection occurred.");
        
        CreateObservableGaugeWithTags<long, ServerGcTotalCommitted>(
            name: Constants.ServerWide.GC.ServerGcTotalCommitted,
            observeValueFactory: () => new ServerGcTotalCommitted(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the total committed MB of the managed heap.");
        
        CreateObservableGaugeWithTags<long, ServerGcLohSize>(
            name: Constants.ServerWide.GC.ServerGcLohSize,
            observeValueFactory: () => new ServerGcLohSize(metrics, gcKind),
            family: configuration => configuration.GCInstruments,
            meter: ServerStatisticsMeter,
            overridenDescription: "Gets the large object heap size (in MB) after the last garbage collection of given kind occurred.");
    }
}
