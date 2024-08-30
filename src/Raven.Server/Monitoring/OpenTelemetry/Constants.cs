namespace Raven.Server.Monitoring.OpenTelemetry;

public static class Constants
{
    public static class Meters
    {
        public const string GeneralMeter = "ravendb.server.general";
        public const string RequestsMeter = "ravendb.server.requests";
        public const string StorageMeter = "ravendb.server.storage";
        public const string GcMeter = "ravendb.server.gc";
        public const string Resources = "ravendb.server.resources";
        public const string TotalDatabasesMeter = "ravendb.server.totaldatabases";
        public const string CpuCreditsMeter = "ravendb.server.cpucredits";
 
    }
    
    public static class ServerWide
    {
        public const string ServerConcurrentRequests = "requests.concurrent_requests";
        public const string ServerTotalRequests = "total.requests";
        public const string ServerRequestsPerSecond = "requests.per_second";
        public const string ServerRequestAverageDuration = "requests.average_duration";
        public const string ProcessCpu = "cpu.process";
        public const string MachineCpu = "cpu.machine";
        public const string IoWait = "io_wait";
        public const string CpuCreditsBase = "base";
        public const string CpuCreditsMax = "max";
        public const string CpuCreditsRemaining = "remaining";
        public const string CpuCreditsCurrentConsumption = "consumption_current";
        public const string CpuCreditsBackgroundTasksAlertRaised = "background.tasks.alert_raised";
        public const string CpuCreditsFailoverAlertRaised = "failover.alert_raised";
        public const string CpuCreditsAlertRaised = "alert_raised";
        public const string ServerTotalMemory = "total_memory";
        public const string ServerLowMemoryFlag = "low_memory_flag";
        public const string ServerTotalSwapSize = "total.swap.size";
        public const string ServerTotalSwapUsage = "total.swap_usage";
        public const string ServerWorkingSetSwapUsage = "working_set_swap_usage";
        public const string ServerDirtyMemory = "dirty_memory";
        public const string ServerManagedMemory = "managed_memory";
        public const string ServerUnmanagedMemory = "unmanaged_memory";
        public const string ServerEncryptionBuffersMemoryInUse = "encryption_buffers.memory_in_use";
        public const string ServerEncryptionBuffersMemoryInPool = "encryption_buffers.memory_in_pool";
        public const string ServerAvailableMemoryForProcessing = "available_memory_for_processing";
        public const string DatabaseLoadedCount = "database.loaded_count";
        public const string DatabaseTotalCount = "database.total_count";
        public const string DatabaseDisabledCount = "database.disabled_count";
        public const string DatabaseEncryptedCount = "database.encrypted_count";
        public const string DatabaseFaultedCount = "database.faulted_count";
        public const string DatabaseNodeCount = "database.node_count";
        public const string TotalDatabaseNumberOfIndexes = "number_of_indexes";
        public const string TotalDatabaseCountOfStaleIndexes = "count_stale_indexes";
        public const string TotalDatabaseNumberOfErrorIndexes = "number_error_indexes";
        public const string TotalDatabaseNumberOfFaultyIndexes = "number.faulty_indexes";
        public const string TotalDatabaseMapIndexIndexedPerSecond = "map.index.indexed_per_second";
        public const string TotalDatabaseMapReduceIndexMappedPerSecond = "map_reduce.index.mapped_per_second";
        public const string TotalDatabaseMapReduceIndexReducedPerSecond = "map_reduce.index.reduced_per_second";
        public const string TotalDatabaseWritesPerSecond = "writes_per_second";
        public const string TotalDatabaseDataWrittenPerSecond = "data.written.per_second";
        public const string ClusterNodeState = "cluster.node.state";
        public const string ClusterIndex = "cluster.index";
        public const string ClusterTerm = "cluster.term";
        public const string ServerStorageUsedSize = "storage.used_size";
        public const string ServerStorageTotalSize = "storage.total_size";
        public const string ServerStorageDiskRemainingSpace = "storage.disk.remaining.space";
        public const string ServerStorageDiskRemainingSpacePercentage = "storage.disk.remaining.space_percentage";
        public const string ServerStorageDiskIosReadOperations = "storage.disk.ios.read_operations";
        public const string ServerStorageDiskIosWriteOperations = "storage.disk.ios.write_operations";
        public const string ServerStorageDiskReadThroughput = "storage.disk.read_throughput";
        public const string ServerStorageDiskWriteThroughput = "storage.disk.write_throughput";
        public const string ServerStorageDiskQueueLength = "storage.disk.queue_length";
        public const string MachineProcessorCount = "machine.processor_count";
        public const string MachineAssignedProcessorCount = "machine.assigned_processor_count";
        public const string ThreadPoolAvailableWorkerThreads = "thread_pool.available_worker_threads";
        public const string ThreadPoolAvailableCompletionPortThreads = "thread_pool.available_completion_port_threads";
        public const string TcpActiveConnections = "tcp.active.connections";
        public const string ServerCertificateExpirationLeft = "certificate_server_certificate_expiration_left_seconds";
        public const string ServerLicenseType = "license.type";
        public const string ServerLicenseExpirationLeft = "license.expiration_left_seconds";
        public const string ServerLicenseUtilizedCpuCores = "license.cpu.utilized";
        public const string ServerLicenseMaxCpuCores = "license.cores.max";

        public class GC
        {
            public const string ServerGcCompacted = "compacted";
            public const string ServerGcConcurrent = "concurrent";
            public const string ServerGcFinalizationPendingCount = "finalizationpendingcount";
            public const string ServerGcFragmented = "fragmented";
            public const string ServerGcGeneration = "generation";
            public const string ServerGcHeapSize = "heapsize";
            public const string ServerGcHighMemoryLoadThreshold = "highmemoryloadthreshold";
            public const string ServerGcIndex = "index";
            public const string ServerGcMemoryLoad = "memoryload";
            public const string ServerGcPauseDurations1 = "pausedurations1";
            public const string ServerGcPauseDurations2 = "pausedurations2";
            public const string ServerGcPauseTimePercentage = "timepercentage";
            public const string ServerGcPinnedObjectsCount = "pinnedobjectscount";
            public const string ServerGcPromoted = "promoted";
            public const string ServerGcTotalAvailableMemory = "totalavailablememory";
            public const string ServerGcTotalCommitted = "totalcommitted";
            public const string ServerGcLohSize = "gclohsize";
        }
    }
}
