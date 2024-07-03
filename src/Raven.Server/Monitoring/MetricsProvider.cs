using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Monitoring;
using Sparrow;
using Sparrow.Binary;
using Sparrow.LowMemory;
using Sparrow.Server.Extensions;
using Sparrow.Server.LowMemory;
using Sparrow.Server.Utils;
using Voron;
using Index = Raven.Server.Documents.Indexes.Index;
using Size = Sparrow.Size;

namespace Raven.Server.Monitoring;

public sealed class MetricsProvider
{
    private readonly RavenServer _server;
    private readonly ServerStore _serverStore;

    public MetricsProvider(RavenServer server)
    {
        _server = server;
        _serverStore = server.ServerStore;
    }

    public ServerMetrics CollectServerMetrics()
    {
        var result = new ServerMetrics();

        result.ServerVersion = ServerWide.ServerVersion.Version;
        result.ServerFullVersion = ServerWide.ServerVersion.FullVersion;
        result.UpTimeInSec = (int)_server.Statistics.UpTime.TotalSeconds;

        using (var currentProcess = Process.GetCurrentProcess())
            result.ServerProcessId = currentProcess.Id;

        result.Config = GetConfigMetrics();
        result.Backup = GetBackupMetrics();
        result.Cpu = GetCpuMetrics();
        result.Memory = GetMemoryMetrics();
        result.Network = GetNetworkMetrics();
        result.License = GetLicenseMetrics();
        result.Disk = GetDiskMetrics();
        result.Certificate = GetCertificateMetrics();
        result.Cluster = GetClusterMetrics();
        result.Databases = GetAllDatabasesMetrics();

        return result;
    }

    private ConfigurationMetrics GetConfigMetrics()
    {
        var configuration = _server.Configuration;

        var result = new ConfigurationMetrics();
        result.ServerUrls = configuration.Core.ServerUrls;
        result.PublicServerUrl = configuration.Core.PublicServerUrl?.UriValue;
        result.TcpServerUrls = configuration.Core.TcpServerUrls?.Length > 0
            ? configuration.Core.TcpServerUrls
            : null;

        if (configuration.Core.PublicTcpServerUrl.HasValue)
            result.PublicTcpServerUrls = new[] {configuration.Core.PublicTcpServerUrl.Value.UriValue};
        else if (configuration.Core.ExternalPublicTcpServerUrl != null && configuration.Core.ExternalPublicTcpServerUrl.Length > 0)
            result.PublicTcpServerUrls = configuration.Core.ExternalPublicTcpServerUrl.Select(x => x.UriValue).ToArray();
        return result;
    }

    private BackupMetrics GetBackupMetrics()
    {
        var result = new BackupMetrics();
        result.MaxNumberOfConcurrentBackups = _serverStore.ConcurrentBackupsCounter.MaxNumberOfConcurrentBackups;
        result.CurrentNumberOfRunningBackups = _serverStore.ConcurrentBackupsCounter.CurrentNumberOfRunningBackups;

        return result;
    }

    private NetworkMetrics GetNetworkMetrics()
    {
        var result = new NetworkMetrics();

        var properties = TcpExtensions.GetIPGlobalPropertiesSafely();
        var ipv4Stats = properties.GetTcpIPv4StatisticsSafely();
        var ipv6Stats = properties.GetTcpIPv6StatisticsSafely();

        var currentIpv4Connections = ipv4Stats.GetCurrentConnectionsSafely() ?? 0;
        var currentIpv6Connections = ipv6Stats.GetCurrentConnectionsSafely() ?? 0;

        result.TcpActiveConnections = currentIpv4Connections + currentIpv6Connections;

        result.ConcurrentRequestsCount = _server.Metrics.Requests.ConcurrentRequestsCount;
        result.TotalRequests = _server.Metrics.Requests.RequestsPerSec.Count;
        result.RequestsPerSec = _server.Metrics.Requests.RequestsPerSec.OneMinuteRate;

        result.LastRequestTimeInSec = _server.Statistics.LastRequestTime.HasValue
            ? (SystemTime.UtcNow - _server.Statistics.LastRequestTime.Value).TotalSeconds
            : (double?)null;

        result.LastAuthorizedNonClusterAdminRequestTimeInSec = _server.Statistics.LastAuthorizedNonClusterAdminRequestTime.HasValue
            ? (SystemTime.UtcNow - _server.Statistics.LastAuthorizedNonClusterAdminRequestTime.Value).TotalSeconds
            : (double?)null;

        return result;
    }

    private CpuMetrics GetCpuMetrics()
    {
        var result = new CpuMetrics();

        using (var currentProcess = Process.GetCurrentProcess())
#pragma warning disable CA1416 // Validate platform compatibility
            result.AssignedProcessorCount = (int)Bits.NumberOfSetBits(currentProcess.ProcessorAffinity.ToInt64());
#pragma warning restore CA1416 // Validate platform compatibility

        result.ProcessorCount = Environment.ProcessorCount;

        ThreadPool.GetAvailableThreads(out var workerThreads, out var completionPortThreads);
        result.ThreadPoolAvailableWorkerThreads = workerThreads;
        result.ThreadPoolAvailableCompletionPortThreads = completionPortThreads;

        var cpuUsage = _server.MetricCacher.GetValue(MetricCacher.Keys.Server.CpuUsage, _server.CpuUsageCalculator.Calculate);

        result.ProcessUsage = cpuUsage.ProcessCpuUsage;
        result.MachineUsage = cpuUsage.MachineCpuUsage;
        result.MachineIoWait = cpuUsage.MachineIoWait;

        return result;
    }

    private MemoryMetrics GetMemoryMetrics()
    {
        var result = new MemoryMetrics();
        var memoryInfoResult = _server.MetricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15Seconds);

        result.InstalledMemoryInMb = memoryInfoResult.InstalledMemory.GetValue(SizeUnit.Megabytes);
        result.PhysicalMemoryInMb = memoryInfoResult.TotalPhysicalMemory.GetValue(SizeUnit.Megabytes);
        result.AllocatedMemoryInMb = memoryInfoResult.WorkingSet.GetValue(SizeUnit.Megabytes);
        result.LowMemorySeverity = LowMemoryNotification.Instance.IsLowMemory(memoryInfoResult,
            new LowMemoryMonitor(), out _);

        result.TotalSwapSizeInMb = memoryInfoResult.TotalSwapSize.GetValue(SizeUnit.Megabytes);
        result.TotalSwapUsageInMb = memoryInfoResult.TotalSwapUsage.GetValue(SizeUnit.Megabytes);
        result.WorkingSetSwapUsageInMb = memoryInfoResult.WorkingSetSwapUsage.GetValue(SizeUnit.Megabytes);

        result.TotalDirtyInMb = MemoryInformation.GetDirtyMemoryState().TotalDirty.GetValue(SizeUnit.Megabytes);

        return result;
    }

    private LicenseMetrics GetLicenseMetrics()
    {
        var result = new LicenseMetrics();
        var licenseStatus = _server.ServerStore.LicenseManager.LicenseStatus;
        result.Type = licenseStatus.Type;
        result.ExpirationLeftInSec = licenseStatus.Expiration.HasValue
            ? Math.Max(0, (licenseStatus.Expiration.Value - SystemTime.UtcNow).TotalSeconds)
            : (double?)null;
        result.UtilizedCpuCores = _server.ServerStore.LicenseManager.GetCoresLimitForNode(out _);
        result.MaxCores = licenseStatus.MaxCores;
        return result;
    }

    private DiskMetrics GetDiskMetrics()
    {
        var result = new DiskMetrics();
        var environmentStats = _server.ServerStore._env.Stats();
        result.SystemStoreUsedDataFileSizeInMb = new Size(environmentStats.UsedDataFileSizeInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
        result.SystemStoreTotalDataFileSizeInMb = new Size(environmentStats.AllocatedDataFileSizeInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);

        if (_serverStore.Configuration.Core.RunInMemory == false)
        {
            var diskSpaceResult = _server.MetricCacher.GetValue<DiskSpaceResult>(MetricCacher.Keys.Server.DiskSpaceInfo);
            if (diskSpaceResult != null)
            {
                var total = Convert.ToDecimal(diskSpaceResult.TotalSize.GetValue(SizeUnit.Megabytes));
                var totalFreeMb = diskSpaceResult.TotalFreeSpace.GetValue(SizeUnit.Megabytes);
                var totalFree = Convert.ToDecimal(totalFreeMb);
                var percentage = Convert.ToInt32(Math.Round(totalFree / total * 100, 0, MidpointRounding.ToEven));
                result.TotalFreeSpaceInMb = totalFreeMb;
                result.RemainingStorageSpacePercentage = percentage;
            }

            var diskStatsResult = _server.DiskStatsGetter.Get(_serverStore._env.Options.DriveInfoByPath?.Value.BasePath.DriveName);
            if (diskStatsResult != null)
            {
                result.IoReadOperations = diskStatsResult.IoReadOperations;
                result.IoWriteOperations = diskStatsResult.IoWriteOperations;
                result.ReadThroughputInKb = diskStatsResult.ReadThroughput.GetValue(SizeUnit.Kilobytes);
                result.WriteThroughputInKb = diskStatsResult.WriteThroughput.GetValue(SizeUnit.Kilobytes);
                result.QueueLength = diskStatsResult.QueueLength;
            }
        }

        return result;
    }

    private CertificateMetrics GetCertificateMetrics()
    {
        var result = new CertificateMetrics();
        var certificateHolder = _serverStore.Server.Certificate;
        if (certificateHolder?.Certificate != null)
        {
            var notAfter = certificateHolder.Certificate.NotAfter.ToUniversalTime();
            var timeLeft = notAfter - SystemTime.UtcNow;
            result.ServerCertificateExpirationLeftInSec = (timeLeft.TotalSeconds > 0 ? timeLeft : TimeSpan.Zero).TotalSeconds;
        }
        else
        {
            result.ServerCertificateExpirationLeftInSec = -1;
        }

        result.WellKnownAdminCertificates = _serverStore.Configuration.Security.WellKnownAdminCertificates;
        result.WellKnownAdminIssuers = _server.WellKnownIssuersThumbprints;
        return result;
    }

    private ClusterMetrics GetClusterMetrics()
    {
        var result = new ClusterMetrics();

        var nodeTag = _serverStore.NodeTag;

        result.NodeTag = nodeTag;

        if (string.IsNullOrWhiteSpace(nodeTag) == false)
        {
            result.NodeState = _serverStore.CurrentRachisState;
        }

        result.CurrentTerm = _serverStore.Engine.CurrentCommittedState.Term;
        result.Index = _serverStore.LastRaftCommitIndex;
        result.Id = _serverStore.Engine.ClusterId;

        return result;
    }

    private AllDatabasesMetrics GetAllDatabasesMetrics()
    {
        var result = new AllDatabasesMetrics();

        using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var items = _serverStore.Cluster.ItemsStartingWith(context, Constants.Documents.Prefix, 0, long.MaxValue);
            result.TotalCount = items.Count();
        }

        result.LoadedCount = _serverStore.DatabasesLandlord.DatabasesCache.Count;

        return result;
    }

    public DatabaseMetrics CollectDatabaseMetrics(DocumentDatabase database)
    {
        var result = new DatabaseMetrics();

        result.DatabaseName = database.Name;

        result.DatabaseId = database.DocumentsStorage.Environment.DbId.ToString();
        result.UptimeInSec = (int)(SystemTime.UtcNow - database.StartTime).TotalSeconds;
        var lastBackup = database.PeriodicBackupRunner?.GetBackupInfo()?.LastBackup;
        result.TimeSinceLastBackupInSec = lastBackup.HasValue
            ? (SystemTime.UtcNow - lastBackup.Value).TotalSeconds
            : (double?)null;

        result.Counts = GetDatabaseCounts(database);
        result.Indexes = GetDatabaseIndexesMetrics(database);
        result.Storage = GetDatabaseStorageMetrics(database);
        result.Statistics = GetDatabaseStatistics(database);

        return result;
    }


    private DatabaseCounts GetDatabaseCounts(DocumentDatabase database)
    {
        var result = new DatabaseCounts();

        var documentsStorage = database.DocumentsStorage;

        using (var context = QueryOperationContext.Allocate(database, needsServerContext: true))
        using (context.OpenReadTransaction())
        {
            result.Documents = documentsStorage.GetNumberOfDocuments(context.Documents);
            result.Revisions = documentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context.Documents);
            var attachments = documentsStorage.AttachmentsStorage.GetNumberOfAttachments(context.Documents);
            result.Attachments = attachments.AttachmentCount;
            result.UniqueAttachments = attachments.StreamsCount;
        }

        result.Alerts = database.NotificationCenter.GetAlertCount();

        var topology = database.ServerStore.LoadDatabaseTopology(database.Name);
        result.Rehabs = topology.Rehabs?.Count ?? 0;
        result.PerformanceHints = database.NotificationCenter.GetPerformanceHintCount();
        result.ReplicationFactor = topology.ReplicationFactor;

        return result;
    }

    private DatabaseIndexesMetrics GetDatabaseIndexesMetrics(DocumentDatabase database)
    {
        var result = new DatabaseIndexesMetrics();

        var indexes = database.IndexStore.GetIndexes().ToList();

        result.Count = database.IndexStore.Count;

        var indexErrorsCount = 0L;
        foreach (var index in indexes)
            indexErrorsCount += index.GetErrorCount();

        result.ErrorsCount = indexErrorsCount;

        result.StaticCount = indexes.Count(x => x.Type.IsStatic());
        result.AutoCount = indexes.Count(x => x.Type.IsAuto());
        result.IdleCount = indexes.Count(x => x.State == IndexState.Idle);
        result.DisabledCount = indexes.Count(x => x.State == IndexState.Disabled);
        result.ErroredCount = indexes.Count(x => x.State == IndexState.Error);

        using (var context = QueryOperationContext.Allocate(database, needsServerContext: true))
        using (context.OpenReadTransaction())
        {
            result.StaleCount = indexes
                .Count(x => x.IsStale(context));
        }

        return result;
    }

    private DatabaseStorageMetrics GetDatabaseStorageMetrics(DocumentDatabase database)
    {
        var result = new DatabaseStorageMetrics();

        var documentsAllocatedDataFileSizeInBytes = 0L;
        var documentsUsedDataFileSizeInBytes = 0L;

        var indexesAllocatedDataFileSizeInBytes = 0L;
        var indexesUsedDataFileSizeInBytes = 0L;

        var totalAllocatedDataFileSizeInBytes = 0L;

        foreach (StorageEnvironmentWithType storageEnvironmentWithType in database.GetAllStoragesEnvironment())
        {
            var stats = storageEnvironmentWithType.Environment.Stats();
            totalAllocatedDataFileSizeInBytes += stats.AllocatedDataFileSizeInBytes;

            switch (storageEnvironmentWithType.Type)
            {
                case StorageEnvironmentWithType.StorageEnvironmentType.Documents:
                    documentsAllocatedDataFileSizeInBytes += stats.AllocatedDataFileSizeInBytes;
                    documentsUsedDataFileSizeInBytes += stats.UsedDataFileSizeInBytes;
                    break;

                case StorageEnvironmentWithType.StorageEnvironmentType.Index:
                    indexesAllocatedDataFileSizeInBytes += stats.AllocatedDataFileSizeInBytes;
                    indexesUsedDataFileSizeInBytes += stats.UsedDataFileSizeInBytes;
                    break;
            }
        }

        result.DocumentsAllocatedDataFileInMb = new Size(documentsAllocatedDataFileSizeInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
        result.DocumentsUsedDataFileInMb = new Size(documentsUsedDataFileSizeInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
        result.IndexesAllocatedDataFileInMb = new Size(indexesAllocatedDataFileSizeInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
        result.IndexesUsedDataFileInMb = new Size(indexesUsedDataFileSizeInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
        result.TotalAllocatedStorageFileInMb = new Size(totalAllocatedDataFileSizeInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);

        result.TotalFreeSpaceInMb = -1;

        if (database.Configuration.Core.RunInMemory == false)
        {
            var diskSpaceResult = database.MetricCacher.GetValue<DiskSpaceResult>(MetricCacher.Keys.Database.DiskSpaceInfo);
            if (diskSpaceResult != null)
            {
                result.TotalFreeSpaceInMb = diskSpaceResult.TotalFreeSpace.GetValue(SizeUnit.Megabytes);
            }

            var diskStatsResult = _server.DiskStatsGetter.Get(database.DocumentsStorage.Environment.Options.DriveInfoByPath?.Value.BasePath.DriveName);
            if (diskStatsResult != null)
            {
                result.IoReadOperations = diskStatsResult.IoReadOperations;
                result.IoWriteOperations = diskStatsResult.IoWriteOperations;
                result.ReadThroughputInKb = diskStatsResult.ReadThroughput.GetValue(SizeUnit.Kilobytes);
                result.WriteThroughputInKb = diskStatsResult.WriteThroughput.GetValue(SizeUnit.Kilobytes);
                result.QueueLength = diskStatsResult.QueueLength;
            }
        }

        return result;
    }

    private DatabaseStatistics GetDatabaseStatistics(DocumentDatabase database)
    {
        var result = new DatabaseStatistics();
        result.DocPutsPerSec = database.Metrics.Docs.PutsPerSec.OneMinuteRate;
        result.MapIndexIndexesPerSec = database.Metrics.MapIndexes.IndexedPerSec.OneMinuteRate;
        result.MapReduceIndexMappedPerSec = database.Metrics.MapReduceIndexes.MappedPerSec.OneMinuteRate;
        result.MapReduceIndexReducedPerSec = database.Metrics.MapReduceIndexes.ReducedPerSec.OneMinuteRate;
        result.RequestsPerSec = database.Metrics.Requests.RequestsPerSec.OneMinuteRate;
        result.RequestsCount = (int)database.Metrics.Requests.RequestsPerSec.Count;
        result.RequestAverageDurationInMs = database.Metrics.Requests.AverageDuration.GetRate();
        return result;
    }

    public IndexMetrics CollectIndexMetrics(Index index)
    {
        var result = new IndexMetrics();

        result.IndexName = index.Name;

        result.Priority = index.Definition.Priority;
        result.State = index.State;
        result.Errors = (int)index.GetErrorCount();

        var stats = index.GetStats();
        if (stats.LastQueryingTime.HasValue)
        {
            var lastQueryingTime = stats.LastQueryingTime.Value;
            result.TimeSinceLastQueryInSec = (SystemTime.UtcNow - lastQueryingTime).TotalSeconds;
        }

        if (stats.LastIndexingTime.HasValue)
        {
            var lastIndexingType = stats.LastIndexingTime.Value;
            result.TimeSinceLastIndexingInSec = (SystemTime.UtcNow - lastIndexingType).TotalSeconds;
        }

        result.ArchivedDataProcessingBehavior = index.ArchivedDataProcessingBehavior;
        result.LockMode = index.Definition.LockMode;
        result.IsInvalid = stats.IsInvalidIndex;
        result.Status = index.Status;

        result.MappedPerSec = index.MapsPerSec?.OneMinuteRate ?? 0;
        result.ReducedPerSec = index.ReducesPerSec?.OneMinuteRate ?? 0;

        result.Type = index.Type;
        result.EntriesCount = stats.EntriesCount;

        return result;
    }
}
