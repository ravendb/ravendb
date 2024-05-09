using System.Collections.Generic;

namespace Raven.Server.Monitoring.OpenTelemetry;

public static class Constants
{
    private const string Prefix = "ravendb.server";

    public const string ServerWideMeterName = Prefix + ".serverwide";
    public const string ServerWideDatabasesMeterName = ServerWideMeterName + ".databases";
    public const string DatabaseStorageMeter = ServerWideDatabasesMeterName + ".storage";
    public const string IndexMeter = ServerWideDatabasesMeterName + ".indexes";
    
    public static class Tags
    {
        public const string Database = "database";
        public const string Index = "index";

        public static KeyValuePair<string, object> CreateNodeTagLabel(string nodeTag) => new("node", nodeTag);
        public const string NodeTag = "node";
    }
    
    public static class DatabaseWide
    {
        // ReSharper disable once MemberHidesStaticFromOuterClass
        private const string Prefix = Constants.Prefix + ".database.";

        public const string DatabaseCountOfIndexes = Prefix + "countofindexes";
        public const string DatabaseDocumentsStorageAllocatedSize = Prefix + "documents.allocatedsize";
        public const string DatabaseDocumentsStorageUsedSize = Prefix + "documents.usedsize";
        public const string DatabaseIndexStorageAllocatedSize = Prefix + "index.allocatedsize";
        public const string DatabaseIndexStorageUsedSize = Prefix + "index.usedsize";
        public const string DatabaseTotalStorageSize = Prefix + "total.storagesize";
        public const string DatabaseStorageDiskRemainingSpace = Prefix + "disk.remainingspace";
        public const string DatabaseStorageDiskIosReadOperations = Prefix + "disk.ios.readoperations";
        public const string DatabaseStorageDiskIosWriteOperations = Prefix + "disk.ios.writeoperations";
        public const string DatabaseStorageDiskReadThroughput = Prefix + "disk.throughput.read";
        public const string DatabaseStorageDiskWriteThroughput = Prefix + "disk.throughput.write";
        public const string DatabaseStorageDiskQueueLength = Prefix + "disk.queuelength";
        
        public static class IndexWide
        {
            // ReSharper disable once MemberHidesStaticFromOuterClass
            private const string Prefix = DatabaseWide.Prefix + "index.";

            public const string DatabaseIndexExists = Prefix + "exists";
            public const string DatabaseIndexPriority = Prefix + "priority";
            public const string DatabaseIndexState = Prefix + "state";
            public const string DatabaseIndexErrors = Prefix + "errors";
            public const string DatabaseIndexLastQueryTime = Prefix + "time.query.last";
            public const string DatabaseIndexLastIndexingTime = Prefix + "time.indexing.last";
            public const string DatabaseIndexTimeSinceLastQuery = Prefix + "time.since.lastquery";
            public const string DatabaseIndexTimeSinceLastIndexing = Prefix + "time.since.lastindexing";
            public const string DatabaseIndexLockMode = Prefix + "lockmode";
            public const string DatabaseIndexIsInvalid = Prefix + "isinvalid";
            public const string DatabaseIndexStatus = Prefix + "status";
            public const string DatabaseIndexMapsPerSec = Prefix + "mapspersec";
            public const string DatabaseIndexReducesPerSec = Prefix + "reducespersec";
            public const string DatabaseIndexType = Prefix + "type";
        }
    }




    public static class ServerWide
    {
        private const string Prefix = Constants.Prefix + ".serverwide.";
        
        public const string ServerUrl = Prefix + "server.url";
        public const string ServerPublicUrl = Prefix + "server.public.url";
        public const string ServerTcpUrl = Prefix + "server.tcp.url";
        public const string ServerPublicTcpUrl = Prefix + "server.public.tcp.url";
        public const string ServerVersion = Prefix + "server.version";
        public const string ServerFullVersion = Prefix + "server.full.version";
        public const string ServerUpTime = Prefix + "server.up.time";
        public const string ServerUpTimeGlobal = Prefix + "server.up.time.global";
        public const string ServerPid = Prefix + "server.pid";
        public const string ServerConcurrentRequests = Prefix + "server.concurrent.requests";
        public const string ServerTotalRequests = Prefix + "server.total.requests";
        public const string ServerRequestsPerSecond = Prefix + "server.requests.per.second";
        public const string ServerRequestAverageDuration = Prefix + "server.request.average.duration";
        public const string ProcessCpu = Prefix + "process.cpu";
        public const string MachineCpu = Prefix + "machine.cpu";
        public const string IoWait = Prefix + "io.wait";
        public const string CpuCreditsBase = Prefix + "cpu.credits.base";
        public const string CpuCreditsMax = Prefix + "cpu.credits.max";
        public const string CpuCreditsRemaining = Prefix + "cpu.credits.remaining";
        public const string CpuCreditsCurrentConsumption = Prefix + "cpu.credits.current.consumption";
        public const string CpuCreditsBackgroundTasksAlertRaised = Prefix + "cpu.credits.background.tasks.alert.raised";
        public const string CpuCreditsFailoverAlertRaised = Prefix + "cpu.credits.failover.alert.raised";
        public const string CpuCreditsAlertRaised = Prefix + "cpu.credits.alert.raised";
        public const string ServerTotalMemory = Prefix + "server.total.memory";
        public const string ServerLowMemoryFlag = Prefix + "server.low.memory.flag";
        public const string ServerTotalSwapSize = Prefix + "server.total.swap.size";
        public const string ServerTotalSwapUsage = Prefix + "server.total.swap.usage";
        public const string ServerWorkingSetSwapUsage = Prefix + "server.working.set.swap.usage";
        public const string ServerDirtyMemory = Prefix + "server.dirty.memory";
        public const string ServerManagedMemory = Prefix + "server.managed.memory";
        public const string ServerUnmanagedMemory = Prefix + "server.unmanaged.memory";
        public const string ServerEncryptionBuffersMemoryInUse = Prefix + "server.encryption.buffers.memory.in.use";
        public const string ServerEncryptionBuffersMemoryInPool = Prefix + "server.encryption.buffers.memory.in.pool";
        public const string ServerAvailableMemoryForProcessing = Prefix + "server.available.memory.for.processing";
        public const string ServerLastRequestTime = Prefix + "server.last.request.time";
        public const string ServerLastAuthorizedNonClusterAdminRequestTime = Prefix + "server.last.authorized.non.cluster.admin.request.time";
        public const string DatabaseLoadedCount = Prefix + "database.loaded.count";
        public const string DatabaseTotalCount = Prefix + "database.total.count";
        public const string DatabaseOldestBackup = Prefix + "database.oldest.backup";
        public const string DatabaseDisabledCount = Prefix + "database.disabled.count";
        public const string DatabaseEncryptedCount = Prefix + "database.encrypted.count";
        public const string DatabaseFaultedCount = Prefix + "database.faulted.count";
        public const string DatabaseNodeCount = Prefix + "database.node.count";
        public const string TotalDatabaseNumberOfIndexes = Prefix + "total.database.number.of.indexes";
        public const string TotalDatabaseCountOfStaleIndexes = Prefix + "total.database.count.of.stale.indexes";
        public const string TotalDatabaseNumberOfErrorIndexes = Prefix + "total.database.number.of.error.indexes";
        public const string TotalDatabaseNumberOfFaultyIndexes = Prefix + "total.database.number.of.faulty.indexes";
        public const string TotalDatabaseMapIndexIndexedPerSecond = Prefix + "total.database.map.index.indexed.per.second";
        public const string TotalDatabaseMapReduceIndexMappedPerSecond = Prefix + "total.database.map.reduce.index.mapped.per.second";
        public const string TotalDatabaseMapReduceIndexReducedPerSecond = Prefix + "total.database.map.reduce.index.reduced.per.second";
        public const string TotalDatabaseWritesPerSecond = Prefix + "total.database.writes.per.second";
        public const string TotalDatabaseDataWrittenPerSecond = Prefix + "total.database.data.written.per.second";
        public const string ClusterNodeState = Prefix + "cluster.node.state";
        public const string ClusterNodeTag = Prefix + "cluster.node.tag";
        public const string ClusterId = Prefix + "cluster.id";
        public const string ClusterIndex = Prefix + "cluster.index";
        public const string ClusterTerm = Prefix + "cluster.term";
        public const string ServerLicenseType = Prefix + "server.license.type";
        public const string ServerLicenseExpiration = Prefix + "server.license.expiration";
        public const string ServerLicenseExpirationLeft = Prefix + "server.license.expiration.left";
        public const string ServerLicenseUtilizedCpuCores = Prefix + "server.license.utilized.cpu.cores";
        public const string ServerLicenseMaxCpuCores = Prefix + "server.license.max.cpu.cores";
        public const string ServerStorageUsedSize = Prefix + "server.storage.used.size";
        public const string ServerStorageTotalSize = Prefix + "server.storage.total.size";
        public const string ServerStorageDiskRemainingSpace = Prefix + "server.storage.disk.remaining.space";
        public const string ServerStorageDiskRemainingSpacePercentage = Prefix + "server.storage.disk.remaining.space.percentage";
        public const string ServerStorageDiskIosReadOperations = Prefix + "server.storage.disk.ios.read.operations";
        public const string ServerStorageDiskIosWriteOperations = Prefix + "server.storage.disk.ios.write.operations";
        public const string ServerStorageDiskReadThroughput = Prefix + "server.storage.disk.read.throughput";
        public const string ServerStorageDiskWriteThroughput = Prefix + "server.storage.disk.write.throughput";
        public const string ServerStorageDiskQueueLength = Prefix + "server.storage.disk.queue.length";
        public const string ServerCertificateExpiration = Prefix + "server.certificate.expiration";
        public const string ServerCertificateExpirationLeft = Prefix + "server.certificate.expiration.left";
        public const string WellKnownAdminCertificates = Prefix + "well.known.admin.certificates";
        public const string WellKnownAdminIssuers = Prefix + "well.known.admin.issuers";
        public const string CertificateExpiringCount = Prefix + "certificate.expiring.count";
        public const string CertificateExpiredCount = Prefix + "certificate.expired.count";
        public const string MachineProcessorCount = Prefix + "machine.processor.count";
        public const string MachineAssignedProcessorCount = Prefix + "machine.assigned.processor.count";
        public const string ServerBackupsCurrent = Prefix + "server.backups.current";
        public const string ServerBackupsMax = Prefix + "server.backups.max";
        public const string ThreadPoolAvailableWorkerThreads = Prefix + "thread.pool.available.worker.threads";
        public const string ThreadPoolAvailableCompletionPortThreads = Prefix + "thread.pool.available.completion.port.threads";
        public const string TcpActiveConnections = Prefix + "tcp.active.connections";
        public const string FeatureAnyExperimental = Prefix + "feature.any.experimental";
    }

    static Constants()
    {
        //todo write code to assert conventions
    }
}
