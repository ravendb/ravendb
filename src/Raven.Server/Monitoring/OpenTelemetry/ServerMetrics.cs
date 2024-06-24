using System;
using System.Diagnostics.Metrics;
using Raven.Server.Monitoring.Snmp.Objects.Cluster;
using Raven.Server.Monitoring.Snmp.Objects.Database;
using Raven.Server.Monitoring.Snmp.Objects.Server;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.OpenTelemetry;

public class ServerMetrics : MetricsBase
{
    private static readonly Lazy<Meter> GeneralMeter = new(() => new(Constants.Meters.GeneralMeter));
    private static readonly Lazy<Meter> RequestsMeter = new(() => new(Constants.Meters.RequestsMeter));
    private static readonly Lazy<Meter> StorageMeter = new(() => new(Constants.Meters.StorageMeter));
    private static readonly Lazy<Meter> GcMeter = new(() => new(Constants.Meters.GcMeter));
    private static readonly Lazy<Meter> HardwareMeter = new(() => new(Constants.Meters.Hardware));
    private static readonly Lazy<Meter> TotalDatabasesMeter = new(() => new(Constants.Meters.TotalDatabasesMeter));    
    private static readonly Lazy<Meter> CpuCreditsMeter = new(() => new(Constants.Meters.CpuCreditsMeter));
    private readonly RavenServer _server;
    private ServerStore ServerStore => _server.ServerStore;

    public ServerMetrics(RavenServer server) : base(server.Configuration.Monitoring.OpenTelemetry)
    {
        _server = server;
        RegisterGeneralMeter();
        RegisterRequestsInstruments();
        RegisterCpuCreditsInstruments();
        RegisterServerHardwareInstruments();
        RegisterTotalDatabaseInstruments();
        RegisterStorageInstruments();
        RegisterGc(GCKind.Any);
        RegisterGc(GCKind.Background);
        RegisterGc(GCKind.Ephemeral);
        RegisterGc(GCKind.FullBlocking);
    }

    private void RegisterGeneralMeter()
    {
        CreateObservableUpDownCounter<byte, ClusterNodeState>(
            name: Constants.ServerWide.ClusterNodeState,
            observeValueFactory: () => new ClusterNodeState(_server.ServerStore),
            meter: GeneralMeter
        );

        CreateObservableUpDownCounter<long, ClusterTerm>(
            name: Constants.ServerWide.ClusterTerm,
            observeValueFactory: () => new ClusterTerm(_server.ServerStore),
            meter: GeneralMeter
        );
        
        CreateObservableUpDownCounter<long, ClusterIndex>(
            name: Constants.ServerWide.ClusterIndex,
            observeValueFactory: () => new ClusterIndex(_server.ServerStore),
            meter: GeneralMeter
        );
        
        CreateObservableGauge<int, ServerCertificateExpirationLeft>(
            name: Constants.ServerWide.ServerCertificateExpirationLeft,
            observeValueFactory: () => new ServerCertificateExpirationLeft(_server.ServerStore),
            meter: GeneralMeter
        );
        
        CreateObservableGauge<short, ServerLicenseType>(
            name: Constants.ServerWide.ServerLicenseType,
            observeValueFactory: () => new ServerLicenseType(_server.ServerStore),
            meter: GeneralMeter
        );
        
        CreateObservableGauge<int, ServerLicenseExpirationLeft>(
            name: Constants.ServerWide.ServerLicenseExpirationLeft,
            observeValueFactory: () => new ServerLicenseExpirationLeft(_server.ServerStore),
            meter: GeneralMeter
        );

        CreateObservableGauge<int, ServerLicenseUtilizedCpuCores>(
            name: Constants.ServerWide.ServerLicenseUtilizedCpuCores,
            observeValueFactory: () => new ServerLicenseUtilizedCpuCores(_server.ServerStore),
            meter: GeneralMeter
        );

        CreateObservableGauge<int, ServerLicenseMaxCpuCores>(
            name: Constants.ServerWide.ServerLicenseMaxCpuCores,
            observeValueFactory: () => new ServerLicenseMaxCpuCores(_server.ServerStore),
            meter: GeneralMeter
        );
    }

    private void RegisterRequestsInstruments()
    {
        if (Configuration.Requests == false)
            return;

        var metrics = _server.ServerStore.Server.Metrics;
        CreateObservableUpDownCounter<int, ServerConcurrentRequests>(
            name: Constants.ServerWide.ServerConcurrentRequests,
            observeValueFactory: () => new ServerConcurrentRequests(metrics),
            meter: RequestsMeter
        );

        CreateObservableUpDownCounter<int, ServerTotalRequests>(
            name: Constants.ServerWide.ServerTotalRequests,
            observeValueFactory: () => new ServerTotalRequests(metrics),
            meter: RequestsMeter
        );

        const string serverRequestsPerSecondDescription = "Number of requests per second.";
        CreateObservableGaugeWithTags<int, ServerRequestsPerSecond>(
            name: Constants.ServerWide.ServerRequestsPerSecond,
            observeValueFactory: () => new ServerRequestsPerSecond(metrics, ServerRequestsPerSecond.RequestRateType.OneMinute),
            meter: RequestsMeter,
            overridenDescription: serverRequestsPerSecondDescription
        );

        CreateObservableGaugeWithTags<int, ServerRequestsPerSecond>(
            name: Constants.ServerWide.ServerRequestsPerSecond,
            observeValueFactory: () => new ServerRequestsPerSecond(metrics, ServerRequestsPerSecond.RequestRateType.FiveSeconds),
            meter: RequestsMeter,
            overridenDescription: serverRequestsPerSecondDescription
        );

        CreateObservableGauge<int, ServerRequestAverageDuration>(
            name: Constants.ServerWide.ServerRequestAverageDuration,
            observeValueFactory: () => new ServerRequestAverageDuration(metrics),
            meter: RequestsMeter
        );

        CreateObservableGauge<long, TcpActiveConnections>(
            name: Constants.ServerWide.TcpActiveConnections,
            observeValueFactory: () => new TcpActiveConnections(),
            meter: RequestsMeter
        );
    }

    private void RegisterCpuCreditsInstruments()
    {
        if (Configuration.CPUCredits == false)
            return;

        CreateObservableUpDownCounter<int, CpuCreditsBase>(
            name: Constants.ServerWide.CpuCreditsBase,
            observeValueFactory: () => new CpuCreditsBase(ServerStore.Server.CpuCreditsBalance),
            meter: CpuCreditsMeter);

        CreateObservableUpDownCounter<int, CpuCreditsMax>(
            name: Constants.ServerWide.CpuCreditsMax,
            observeValueFactory: () => new CpuCreditsMax(ServerStore.Server.CpuCreditsBalance),
            meter: CpuCreditsMeter);

        CreateObservableGauge<int, CpuCreditsRemaining>(
            name: Constants.ServerWide.CpuCreditsRemaining,
            observeValueFactory: () => new CpuCreditsRemaining(ServerStore.Server.CpuCreditsBalance),
            meter: CpuCreditsMeter);

        CreateObservableUpDownCounter<double, CpuCreditsCurrentConsumption>(
            name: Constants.ServerWide.CpuCreditsCurrentConsumption,
            observeValueFactory: () => new CpuCreditsCurrentConsumption(ServerStore.Server.CpuCreditsBalance),
            meter: CpuCreditsMeter);

        CreateObservableGauge<byte, CpuCreditsBackgroundTasksAlertRaised>(
            name: Constants.ServerWide.CpuCreditsBackgroundTasksAlertRaised,
            observeValueFactory: () => new CpuCreditsBackgroundTasksAlertRaised(ServerStore.Server.CpuCreditsBalance),
            meter: CpuCreditsMeter);

        CreateObservableGauge<byte, CpuCreditsFailoverAlertRaised>(
            name: Constants.ServerWide.CpuCreditsFailoverAlertRaised,
            observeValueFactory: () => new CpuCreditsFailoverAlertRaised(ServerStore.Server.CpuCreditsBalance),
            meter: CpuCreditsMeter);

        CreateObservableGauge<byte, CpuCreditsAlertRaised>(
            name: Constants.ServerWide.CpuCreditsAlertRaised,
            observeValueFactory: () => new CpuCreditsAlertRaised(ServerStore.Server.CpuCreditsBalance),
            meter: CpuCreditsMeter);
    }

    private void RegisterServerHardwareInstruments()
    {
        if (Configuration.Hardware == false)
            return;

        CreateObservableGauge<int, ProcessCpu>(
            name: Constants.ServerWide.ProcessCpu,
            observeValueFactory: () => new ProcessCpu(_server.MetricCacher, _server.CpuUsageCalculator),
            meter: HardwareMeter);

        CreateObservableGauge<int, MachineCpu>(
            name: Constants.ServerWide.MachineCpu,
            observeValueFactory: () => new MachineCpu(_server.MetricCacher, _server.CpuUsageCalculator),
            meter: HardwareMeter);

        CreateObservableGauge<int, IoWait>(
            name: Constants.ServerWide.IoWait,
            observeValueFactory: () => new IoWait(_server.MetricCacher, _server.CpuUsageCalculator),
            meter: HardwareMeter);

        CreateObservableGauge<byte, ServerLowMemoryFlag>(
            name: Constants.ServerWide.ServerLowMemoryFlag,
            observeValueFactory: () => new ServerLowMemoryFlag(),
            meter: HardwareMeter);

        CreateObservableGauge<long, ServerTotalMemory>(
            name: Constants.ServerWide.ServerTotalMemory,
            observeValueFactory: () => new ServerTotalMemory(_server.MetricCacher),
            meter: HardwareMeter);

        CreateObservableGauge<long, ServerTotalSwapSize>(
            name: Constants.ServerWide.ServerTotalSwapSize,
            observeValueFactory: () => new ServerTotalSwapSize(_server.MetricCacher),
            meter: HardwareMeter);

        CreateObservableGauge<long, ServerTotalSwapUsage>(
            name: Constants.ServerWide.ServerTotalSwapUsage,
            observeValueFactory: () => new ServerTotalSwapUsage(_server.MetricCacher),
            meter: HardwareMeter);

        CreateObservableGauge<long, ServerDirtyMemory>(
            name: Constants.ServerWide.ServerDirtyMemory,
            observeValueFactory: () => new ServerDirtyMemory(),
            meter: HardwareMeter);

        CreateObservableGauge<long, ServerWorkingSetSwapUsage>(
            name: Constants.ServerWide.ServerWorkingSetSwapUsage,
            observeValueFactory: () => new ServerWorkingSetSwapUsage(_server.MetricCacher),
            meter: HardwareMeter);

        CreateObservableGauge<long, ServerManagedMemory>(
            name: Constants.ServerWide.ServerManagedMemory,
            observeValueFactory: () => new ServerManagedMemory(),
            meter: HardwareMeter);

        CreateObservableGauge<long, ServerUnmanagedMemory>(
            name: Constants.ServerWide.ServerUnmanagedMemory,
            observeValueFactory: () => new ServerUnmanagedMemory(),
            meter: HardwareMeter);

        CreateObservableGauge<long, ServerEncryptionBuffersMemoryInUse>(
            name: Constants.ServerWide.ServerEncryptionBuffersMemoryInUse,
            observeValueFactory: () => new ServerEncryptionBuffersMemoryInUse(),
            meter: HardwareMeter);

        CreateObservableGauge<long, ServerEncryptionBuffersMemoryInPool>(
            name: Constants.ServerWide.ServerEncryptionBuffersMemoryInPool,
            observeValueFactory: () => new ServerEncryptionBuffersMemoryInPool(),
            meter: HardwareMeter);

        CreateObservableGauge<long, ServerAvailableMemoryForProcessing>(
            name: Constants.ServerWide.ServerAvailableMemoryForProcessing,
            observeValueFactory: () => new ServerAvailableMemoryForProcessing(_server.MetricCacher),
            meter: HardwareMeter);

        CreateObservableUpDownCounter<int, MachineProcessorCount>(
            name: Constants.ServerWide.MachineProcessorCount,
            observeValueFactory: () => new MachineProcessorCount(),
            meter: HardwareMeter);

        CreateObservableUpDownCounter<int, MachineAssignedProcessorCount>(
            name: Constants.ServerWide.MachineAssignedProcessorCount,
            observeValueFactory: () => new MachineAssignedProcessorCount(),
            meter: HardwareMeter);

        CreateObservableGauge<int, ThreadPoolAvailableWorkerThreads>(
            name: Constants.ServerWide.ThreadPoolAvailableWorkerThreads,
            observeValueFactory: () => new ThreadPoolAvailableWorkerThreads(),
            meter: HardwareMeter);

        CreateObservableGauge<int, ThreadPoolAvailableCompletionPortThreads>(
            name: Constants.ServerWide.ThreadPoolAvailableCompletionPortThreads,
            observeValueFactory: () => new ThreadPoolAvailableCompletionPortThreads(),
            meter: HardwareMeter);
    }

    private void RegisterTotalDatabaseInstruments()
    {
        if (Configuration.TotalDatabases == false)
            return;

        CreateObservableUpDownCounter<int, DatabaseLoadedCount>(
            name: Constants.ServerWide.DatabaseLoadedCount,
            observeValueFactory: () => new DatabaseLoadedCount(_server.ServerStore.DatabasesLandlord),
            meter: TotalDatabasesMeter);

        CreateObservableUpDownCounter<int, DatabaseTotalCount>(
            name: Constants.ServerWide.DatabaseTotalCount,
            observeValueFactory: () => new DatabaseTotalCount(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableUpDownCounter<int, DatabaseDisabledCount>(
            name: Constants.ServerWide.DatabaseDisabledCount,
            observeValueFactory: () => new DatabaseDisabledCount(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableUpDownCounter<int, DatabaseEncryptedCount>(
            name: Constants.ServerWide.DatabaseEncryptedCount,
            observeValueFactory: () => new DatabaseEncryptedCount(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableUpDownCounter<int, DatabaseFaultedCount>(
            name: Constants.ServerWide.DatabaseFaultedCount,
            observeValueFactory: () => new DatabaseFaultedCount(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableUpDownCounter<int, DatabaseNodeCount>(
            name: Constants.ServerWide.DatabaseNodeCount,
            observeValueFactory: () => new DatabaseNodeCount(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableUpDownCounter<int, TotalDatabaseNumberOfIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfIndexes,
            observeValueFactory: () => new TotalDatabaseNumberOfIndexes(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableUpDownCounter<int, TotalDatabaseCountOfStaleIndexes>(
            name: Constants.ServerWide.TotalDatabaseCountOfStaleIndexes,
            observeValueFactory: () => new TotalDatabaseCountOfStaleIndexes(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableUpDownCounter<int, TotalDatabaseNumberOfErrorIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfErrorIndexes,
            observeValueFactory: () => new TotalDatabaseNumberOfErrorIndexes(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableUpDownCounter<int, TotalDatabaseNumberOfFaultyIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfFaultyIndexes,
            observeValueFactory: () => new TotalDatabaseNumberOfFaultyIndexes(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableUpDownCounter<int, TotalDatabaseNumberOfIndexes>(
            name: Constants.ServerWide.TotalDatabaseNumberOfIndexes,
            observeValueFactory: () => new TotalDatabaseNumberOfIndexes(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableGauge<int, TotalDatabaseMapIndexIndexedPerSecond>(
            name: Constants.ServerWide.TotalDatabaseMapIndexIndexedPerSecond,
            observeValueFactory: () => new TotalDatabaseMapIndexIndexedPerSecond(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableGauge<int, TotalDatabaseMapReduceIndexMappedPerSecond>(
            name: Constants.ServerWide.TotalDatabaseMapReduceIndexMappedPerSecond,
            observeValueFactory: () => new TotalDatabaseMapReduceIndexMappedPerSecond(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableGauge<int, TotalDatabaseMapReduceIndexReducedPerSecond>(
            name: Constants.ServerWide.TotalDatabaseMapReduceIndexReducedPerSecond,
            observeValueFactory: () => new TotalDatabaseMapReduceIndexReducedPerSecond(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableGauge<int, TotalDatabaseWritesPerSecond>(
            name: Constants.ServerWide.TotalDatabaseWritesPerSecond,
            observeValueFactory: () => new TotalDatabaseWritesPerSecond(_server.ServerStore),
            meter: TotalDatabasesMeter);

        CreateObservableGauge<int, TotalDatabaseDataWrittenPerSecond>(
            name: Constants.ServerWide.TotalDatabaseDataWrittenPerSecond,
            observeValueFactory: () => new TotalDatabaseDataWrittenPerSecond(_server.ServerStore),
            meter: TotalDatabasesMeter);
    }

    private void RegisterStorageInstruments()
    {
        if (Configuration.ServerStorage == false)
            return;

        CreateObservableGauge<long, ServerStorageUsedSize>(
            name: Constants.ServerWide.ServerStorageUsedSize,
            observeValueFactory: () => new ServerStorageUsedSize(_server.ServerStore),
            meter: StorageMeter);

        CreateObservableGauge<long, ServerStorageTotalSize>(
            name: Constants.ServerWide.ServerStorageTotalSize,
            observeValueFactory: () => new ServerStorageTotalSize(_server.ServerStore),
            meter: StorageMeter);

        CreateObservableGauge<long, ServerStorageDiskRemainingSpace>(
            name: Constants.ServerWide.ServerStorageDiskRemainingSpace,
            observeValueFactory: () => new ServerStorageDiskRemainingSpace(_server.ServerStore),
            meter: StorageMeter);

        CreateObservableGauge<int, ServerStorageDiskRemainingSpacePercentage>(
            name: Constants.ServerWide.ServerStorageDiskRemainingSpacePercentage,
            observeValueFactory: () => new ServerStorageDiskRemainingSpacePercentage(_server.ServerStore),
            meter: StorageMeter);

        CreateObservableGauge<int, ServerStorageDiskIosReadOperations>(
            name: Constants.ServerWide.ServerStorageDiskIosReadOperations,
            observeValueFactory: () => new ServerStorageDiskIosReadOperations(_server.ServerStore),
            meter: StorageMeter);

        CreateObservableGauge<int, ServerStorageDiskIosWriteOperations>(
            name: Constants.ServerWide.ServerStorageDiskIosWriteOperations,
            observeValueFactory: () => new ServerStorageDiskIosWriteOperations(_server.ServerStore),
            meter: StorageMeter);

        CreateObservableGauge<long, ServerStorageDiskReadThroughput>(
            name: Constants.ServerWide.ServerStorageDiskReadThroughput,
            observeValueFactory: () => new ServerStorageDiskReadThroughput(_server.ServerStore),
            meter: StorageMeter);

        CreateObservableGauge<long, ServerStorageDiskWriteThroughput>(
            name: Constants.ServerWide.ServerStorageDiskWriteThroughput,
            observeValueFactory: () => new ServerStorageDiskWriteThroughput(_server.ServerStore),
            meter: StorageMeter);

        CreateObservableGauge<long, ServerStorageDiskQueueLength>(
            name: Constants.ServerWide.ServerStorageDiskQueueLength,
            observeValueFactory: () => new ServerStorageDiskQueueLength(_server.ServerStore),
            meter: StorageMeter);
    }

    private void RegisterGc(GCKind gcKind)
    {
        if (Configuration.GcEnabled == false)
            return;

        var metrics = _server.MetricCacher;
        CreateObservableGaugeWithTags<byte, ServerGcCompacted>(
            name: Constants.ServerWide.GC.ServerGcCompacted,
            observeValueFactory: () => new ServerGcCompacted(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Specifies if this is a compacting GC or not.");

        CreateObservableGaugeWithTags<byte, ServerGcConcurrent>(
            name: Constants.ServerWide.GC.ServerGcConcurrent,
            observeValueFactory: () => new ServerGcConcurrent(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Specifies if this is a concurrent GC or not.");

        CreateObservableGaugeWithTags<long, ServerGcFinalizationPendingCount>(
            name: Constants.ServerWide.GC.ServerGcFinalizationPendingCount,
            observeValueFactory: () => new ServerGcFinalizationPendingCount(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the number of objects ready for finalization this GC observed.");

        CreateObservableGaugeWithTags<long, ServerGcFragmented>(
            name: Constants.ServerWide.GC.ServerGcFragmented,
            observeValueFactory: () => new ServerGcFragmented(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the total fragmentation (in MB) when the last garbage collection occurred.");

        CreateObservableGaugeWithTags<int, ServerGcGeneration>(
            name: Constants.ServerWide.GC.ServerGcGeneration,
            observeValueFactory: () => new ServerGcGeneration(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the generation this GC collected.");

        CreateObservableGaugeWithTags<long, ServerGcHeapSize>(
            name: Constants.ServerWide.GC.ServerGcHeapSize,
            observeValueFactory: () => new ServerGcHeapSize(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the total heap size (in MB) when the last garbage collection occurred.");

        CreateObservableGaugeWithTags<long, ServerGcHighMemoryLoadThreshold>(
            name: Constants.ServerWide.GC.ServerGcHighMemoryLoadThreshold,
            observeValueFactory: () => new ServerGcHighMemoryLoadThreshold(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the high memory load threshold (in MB) when the last garbage collection occurred.");

        CreateObservableGaugeWithTags<int, ServerGcIndex>(
            name: Constants.ServerWide.GC.ServerGcIndex,
            observeValueFactory: () => new ServerGcIndex(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "The index of this GC.");

        CreateObservableGaugeWithTags<long, ServerGcMemoryLoad>(
            name: Constants.ServerWide.GC.ServerGcMemoryLoad,
            observeValueFactory: () => new ServerGcMemoryLoad(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the memory load (in MB) when the last garbage collection occurred.");

        CreateObservableGaugeWithTags<long, ServerGcPauseDurations1>(
            name: Constants.ServerWide.GC.ServerGcPauseDurations1,
            observeValueFactory: () => new ServerGcPauseDurations1(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the pause durations. First item in the array.");

        CreateObservableGaugeWithTags<long, ServerGcPauseDurations2>(
            name: Constants.ServerWide.GC.ServerGcPauseDurations2,
            observeValueFactory: () => new ServerGcPauseDurations2(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the pause durations. Second item in the array.");

        CreateObservableGaugeWithTags<int, ServerGcPauseTimePercentage>(
            name: Constants.ServerWide.GC.ServerGcPauseTimePercentage,
            observeValueFactory: () => new ServerGcPauseTimePercentage(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the pause time percentage in the GC so far.");

        CreateObservableGaugeWithTags<long, ServerGcPinnedObjectsCount>(
            name: Constants.ServerWide.GC.ServerGcPinnedObjectsCount,
            observeValueFactory: () => new ServerGcPinnedObjectsCount(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the number of pinned objects this GC observed.");

        CreateObservableGaugeWithTags<long, ServerGcPromoted>(
            name: Constants.ServerWide.GC.ServerGcPromoted,
            observeValueFactory: () => new ServerGcPromoted(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the promoted MB for this GC.");

        CreateObservableGaugeWithTags<long, ServerGcTotalAvailableMemory>(
            name: Constants.ServerWide.GC.ServerGcTotalAvailableMemory,
            observeValueFactory: () => new ServerGcTotalAvailableMemory(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the total available memory (in MB) for the garbage collector to use when the last garbage collection occurred.");

        CreateObservableGaugeWithTags<long, ServerGcTotalCommitted>(
            name: Constants.ServerWide.GC.ServerGcTotalCommitted,
            observeValueFactory: () => new ServerGcTotalCommitted(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the total committed MB of the managed heap.");

        CreateObservableGaugeWithTags<long, ServerGcLohSize>(
            name: Constants.ServerWide.GC.ServerGcLohSize,
            observeValueFactory: () => new ServerGcLohSize(metrics, gcKind),
            meter: GcMeter,
            overridenDescription: "Gets the large object heap size (in MB) after the last garbage collection of given kind occurred.");
    }
}
