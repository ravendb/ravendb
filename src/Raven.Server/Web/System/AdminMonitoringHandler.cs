// -----------------------------------------------------------------------
//  <copyright file="AdminMonitoringHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Monitoring;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.LowMemory;
using Sparrow.Server.Extensions;
using Sparrow.Server.Utils;
using Voron;
using Index = Raven.Server.Documents.Indexes.Index;
using Size = Sparrow.Size;

namespace Raven.Server.Web.System
{
    public class AdminMonitoringHandler : RequestHandler
    {
        [RavenAction("/admin/monitoring/v1/server", "GET", AuthorizationStatus.Operator)]
        public Task MonitoringServer()
        {
            AssertMonitoring();

            var result = new ServerMetrics();

            result.ServerVersion = ServerWide.ServerVersion.Version;
            result.ServerFullVersion = ServerWide.ServerVersion.FullVersion;
            result.UpTimeInSec = (int)Server.Statistics.UpTime.TotalSeconds;
            result.CurrentNumberOfRunningBackups = ServerStore.ConcurrentBackupsCounter.CurrentNumberOfRunningBackups;
            
            using (var currentProcess = Process.GetCurrentProcess())
                result.ServerProcessId = currentProcess.Id;

            result.Config = GetConfigMetrics();
            result.Cpu = GetCpuMetrics();
            result.Memory = GetMemoryMetrics();
            result.Network = GetNetworkMetrics();
            result.License = GetLicenseMetrics();
            result.Disk = GetDiskMetrics();
            result.Certificate = GetCertificateMetrics();
            result.Cluster = GetClusterMetrics();
            result.Databases = GetAllDatabasesMetrics();
            
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
            return Task.CompletedTask;
        }

        private ConfigurationMetrics GetConfigMetrics()
        {
            var configuration = Server.Configuration;
            
            var result = new ConfigurationMetrics();
            result.Urls = configuration.Core.ServerUrls;
            result.PublicUrl = configuration.Core.PublicServerUrl?.UriValue;
            result.TcpUrls = configuration.Core.TcpServerUrls?.Length > 0
                ? configuration.Core.TcpServerUrls
                : null;
            
            if (configuration.Core.PublicTcpServerUrl.HasValue)
                result.PublicTcpUrls = new [] { configuration.Core.PublicTcpServerUrl.Value.UriValue };
            else if (configuration.Core.ExternalPublicTcpServerUrl != null && configuration.Core.ExternalPublicTcpServerUrl.Length > 0)
                result.PublicTcpUrls = configuration.Core.ExternalPublicTcpServerUrl.Select(x => x.UriValue).ToArray();
            
            result.MaxNumberOfConcurrentBackups = ServerStore.ConcurrentBackupsCounter.MaxNumberOfConcurrentBackups;
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

            result.ConcurrentRequestsCount = Server.Metrics.Requests.ConcurrentRequestsCount;
            result.TotalRequests = Server.Metrics.Requests.RequestsPerSec.Count;
            result.RequestsPerSecond = Server.Metrics.Requests.RequestsPerSec.OneMinuteRate;

            result.LastRequestTimeInSec = Server.Statistics.LastRequestTime.HasValue 
                ? (SystemTime.UtcNow - Server.Statistics.LastRequestTime.Value).TotalSeconds
                : (double?)null;

            result.LastAuthorizedNonClusterAdminRequestTimeInSec = Server.Statistics.LastAuthorizedNonClusterAdminRequestTime.HasValue
                ? (SystemTime.UtcNow - Server.Statistics.LastAuthorizedNonClusterAdminRequestTime.Value).TotalSeconds
                : (double?)null;

            return result;
        }
        
        private CpuMetrics GetCpuMetrics()
        {
            var result = new CpuMetrics();
            
            using (var currentProcess = Process.GetCurrentProcess())
                result.AssignedProcessorCount = (int)Bits.NumberOfSetBits(currentProcess.ProcessorAffinity.ToInt64());
            
            result.ProcessorCount = Environment.ProcessorCount;
            
            ThreadPool.GetAvailableThreads(out var workerThreads, out var completionPortThreads);
            result.ThreadPoolAvailableWorkerThreads = workerThreads;
            result.ThreadPoolAvailableCompletionPortThreads = completionPortThreads;
            
            var cpuUsage = Server.MetricCacher.GetValue(MetricCacher.Keys.Server.CpuUsage, Server.CpuUsageCalculator.Calculate);

            result.ProcessCpuUsage = cpuUsage.ProcessCpuUsage;
            result.MachineCpuUsage = cpuUsage.MachineCpuUsage;
            result.MachineIoWait = cpuUsage.MachineIoWait;
            
            return result;
        }
        
        private MemoryMetrics GetMemoryMetrics()
        {
            var result = new MemoryMetrics();
            var memoryInfoResult = Server.MetricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended);
            result.TotalMemoryInMb = memoryInfoResult.WorkingSet.GetValue(SizeUnit.Megabytes);

            result.LowState = LowMemoryNotification.Instance.LowMemoryState;

            result.TotalSwapSizeInMb = memoryInfoResult.TotalSwapSize.GetValue(SizeUnit.Megabytes);
            result.TotalSwapUsageInMb = memoryInfoResult.TotalSwapUsage.GetValue(SizeUnit.Megabytes);
            result.WorkingSetSwapUsageInMb = memoryInfoResult.WorkingSetSwapUsage.GetValue(SizeUnit.Megabytes);
            
            var totalDirtyInBytes = MemoryInformation.GetDirtyMemoryState().TotalDirtyInBytes;
            result.TotalDirtyInMb = new Size(totalDirtyInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
            
            return result;
        }

        private LicenseMetrics GetLicenseMetrics()
        {
            var result = new LicenseMetrics();
            var licenseStatus = Server.ServerStore.LicenseManager.LicenseStatus;
            result.Type = licenseStatus.Type;
            result.Expiration = licenseStatus.Expiration;
            result.ExpirationLeftInSec = licenseStatus.Expiration.HasValue
                ? Math.Max(0, (licenseStatus.Expiration.Value - SystemTime.UtcNow).TotalSeconds) 
                : (double?) null;
            result.UtilizedCpuCores = Server.ServerStore.LicenseManager.GetCoresLimitForNode(out _);
            result.MaxCores = licenseStatus.MaxCores;
            return result;
        }

        private DiskMetrics GetDiskMetrics()
        {
            var result = new DiskMetrics();
            var environmentStats = Server.ServerStore._env.Stats();
            result.UsedDataFileSizeInMb = environmentStats.UsedDataFileSizeInBytes / 1024L / 1024L;
            result.TotalDataFileSizeInMb = environmentStats.AllocatedDataFileSizeInBytes / 1024L / 1024L;

            if (!ServerStore.Configuration.Core.RunInMemory)
            {
                var diskSpaceResult = Server.MetricCacher.GetValue<DiskSpaceResult>(MetricCacher.Keys.Server.DiskSpaceInfo);
                if (diskSpaceResult != null)
                {
                    var total = Convert.ToDecimal(diskSpaceResult.TotalSize.GetValue(SizeUnit.Megabytes));
                    var totalFreeMb = diskSpaceResult.TotalFreeSpace.GetValue(SizeUnit.Megabytes);
                    var totalFree = Convert.ToDecimal(totalFreeMb);
                    var percentage = Convert.ToInt32(Math.Round((totalFree / total) * 100, 0, MidpointRounding.ToEven));
                    result.TotalFreeSpaceInMb = totalFreeMb;
                    result.RemainingStorageSpacePercentage = percentage;
                }
            }

            return result;
        }

        private CertificateMetrics GetCertificateMetrics()
        {
            var result = new CertificateMetrics();
            var certificateHolder = ServerStore.Server.Certificate;
            if (certificateHolder?.Certificate != null)
            {
                var notAfter = certificateHolder.Certificate.NotAfter.ToUniversalTime();
                result.ServerCertificateExpiration = notAfter != DateTime.MinValue ? notAfter : (DateTime?)null;
                var timeLeft = notAfter - SystemTime.UtcNow;
                result.ServerCertificateExpirationLeftInSec = (timeLeft.TotalSeconds > 0 ? timeLeft : TimeSpan.Zero).TotalSeconds;
            }
            
            result.WellKnownAdminCertificates = ServerStore.Configuration.Security.WellKnownAdminCertificates;
            return result;
        }

        private ClusterMetrics GetClusterMetrics()
        {
            var result = new ClusterMetrics();

            var nodeTag = ServerStore.NodeTag;
            
            result.NodeTag = nodeTag;

            if (string.IsNullOrWhiteSpace(nodeTag) == false)
            {
                result.NodeState = ServerStore.CurrentRachisState;
            }
            
            result.CurrentTerm = ServerStore.Engine.CurrentTerm;
            result.Index = ServerStore.LastRaftCommitIndex;
            result.Id = ServerStore.Engine.ClusterId;
            
            return result;
        }

        private AllDatabasesMetrics GetAllDatabasesMetrics()
        {
            var result = new AllDatabasesMetrics();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var items = ServerStore.Cluster.ItemsStartingWith(context, Constants.Documents.Prefix, 0, long.MaxValue);
                result.TotalCount = items.Count();
            }

            result.LoadedCount = ServerStore.DatabasesLandlord.DatabasesCache.Count;

            return result;
        }

        [RavenAction("/admin/monitoring/v1/databases", "GET", AuthorizationStatus.Operator)]
        public Task MonitoringDatabases()
        {
            AssertMonitoring();

            var databases = GetDatabases();
            
            var result = new DatabasesMetrics();
            
            foreach (DocumentDatabase documentDatabase in databases)
            {
                var metrics = GetDatabaseMetrics(documentDatabase);
                result.Results.Add(metrics);
            }
            
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
            
            return Task.CompletedTask;
        }
        
        private List<DocumentDatabase> GetDatabases()
        {
            var names = GetStringValuesQueryString("name", required: false);

            var databases = new List<DocumentDatabase>();
            var landlord = ServerStore.DatabasesLandlord;

            if (names.Count == 0)
            {
                foreach (Task<DocumentDatabase> value in landlord.DatabasesCache.Values)
                {
                    if (value.IsCompletedSuccessfully == false)
                        continue;

                    databases.Add(value.Result);
                }
            }
            else
            {
                foreach (string name in names)
                {
                    if (landlord.IsDatabaseLoaded(name))
                    {
                        var database = landlord.TryGetOrCreateResourceStore(name).Result;
                        databases.Add(database);
                    }
                }
            }

            return databases;
        }

        private DatabaseMetrics GetDatabaseMetrics(DocumentDatabase database)
        {
            var result = new DatabaseMetrics();

            result.Name = database.Name;
            
            result.DatabaseId = database.DocumentsStorage.Environment.DbId.ToString();
            result.UptimeInSec = (SystemTime.UtcNow - database.StartTime).TotalSeconds;
            result.LastBackup = database.PeriodicBackupRunner?.GetBackupInfo()?.LastBackup;
            
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
            
            result.IndexCount = database.IndexStore.Count;
            
            var indexErrorsCount = 0L;
            foreach (var index in indexes)
                indexErrorsCount += index.GetErrorCount();

            result.IndexErrorsCount = indexErrorsCount;

            result.StaticIndexesCount = indexes.Count(x => x.Type.IsStatic());
            result.AutoIndexesCount = indexes.Count(x => x.Type.IsAuto());
            result.IdleIndexesCount = indexes.Count(x => x.State == IndexState.Idle);
            result.DisabledIndexesCount = indexes.Count(x => x.State == IndexState.Disabled);
            result.ErrorIndexesCount = indexes.Count(x => x.State == IndexState.Error);
            
            using (var context = QueryOperationContext.Allocate(database, needsServerContext: true))
            using (context.OpenReadTransaction())
            {
                result.StaleIndexesCount = indexes
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

            result.DocumentsAllocatedDataFileInMb = documentsAllocatedDataFileSizeInBytes / 1024L / 1024L;
            result.DocumentsUsedDataFileInMb = documentsUsedDataFileSizeInBytes / 1024L / 1024L;
            result.IndexesAllocatedDataFileInMb = indexesAllocatedDataFileSizeInBytes / 1024L / 1024L;
            result.IndexesUsedDataFileInMb = indexesUsedDataFileSizeInBytes / 1024L / 1024L;
            result.TotalAllocatedStorageFileInMb = totalAllocatedDataFileSizeInBytes / 1024L / 1024L;
            
            result.TotalFreeSpaceInMb = -1;

            if (database.Configuration.Core.RunInMemory == false)
            {
                var diskSpaceResult = database.MetricCacher.GetValue<DiskSpaceResult>(MetricCacher.Keys.Database.DiskSpaceInfo);
                if (diskSpaceResult != null)
                {
                    result.TotalFreeSpaceInMb = diskSpaceResult.TotalFreeSpace.GetValue(SizeUnit.Megabytes);
                }
            }

            return result;
        }

        private DatabaseStatistics GetDatabaseStatistics(DocumentDatabase database)
        {
            var result = new DatabaseStatistics();
            result.DocPutsPerSecond = (int)database.Metrics.Docs.PutsPerSec.OneMinuteRate;
            result.MapIndexIndexesPerSecond = (int)database.Metrics.MapIndexes.IndexedPerSec.OneMinuteRate;
            result.MapReduceIndexMappedPerSecond = (int)database.Metrics.MapReduceIndexes.MappedPerSec.OneMinuteRate;
            result.MapReduceIndexReducedPerSecond = (int)database.Metrics.MapReduceIndexes.ReducedPerSec.OneMinuteRate;
            result.RequestsPerSecond = (int)database.Metrics.Requests.RequestsPerSec.OneMinuteRate;
            result.RequestsCount = (int)database.Metrics.Requests.RequestsPerSec.Count;
            result.RequestAverageDuration = (int)database.Metrics.Requests.AverageDuration.GetRate();
            return result;
        }

        [RavenAction("/admin/monitoring/v1/indexes", "GET", AuthorizationStatus.Operator)]
        public Task MonitoringIndexes()
        {
            AssertMonitoring();

            var databases = GetDatabases();

            var result = new IndexesMetrics();
            
            foreach (DocumentDatabase documentDatabase in databases)
            {
                foreach (var index in documentDatabase.IndexStore.GetIndexes())
                {
                    var indexMetrics = GetIndexMetrics(documentDatabase, index);
                    result.Results.Add(indexMetrics);
                }
            }
            
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, nameof(IndexesMetrics.Results), result.Results, (w, c, metrics) =>
                {
                    context.Write(w, metrics.ToJson());
                });
                writer.WriteEndObject();
                
            }
            return Task.CompletedTask;
            
        }

        private IndexMetrics GetIndexMetrics(DocumentDatabase documentDatabase, Index index)
        {
            var result = new IndexMetrics();

            result.DatabaseName = documentDatabase.Name;
            result.IndexName = index.Name;
            result.Priority = index.Definition.Priority;
            result.State = index.State;
            result.Errors = (int)index.GetErrorCount();

            var stats = index.GetStats();
            if (stats.LastQueryingTime.HasValue)
            {
                var lastQueryingTime = stats.LastQueryingTime.Value;
                result.LastQueryingTime = lastQueryingTime;
                result.TimeSinceLastQueryInSec = (SystemTime.UtcNow - lastQueryingTime).TotalSeconds;
            }

            if (stats.LastIndexingTime.HasValue)
            {
                var lastIndexingType = stats.LastIndexingTime.Value;
                result.LastIndexingTime = lastIndexingType;
                result.TimeSinceLastIndexingInSec = (SystemTime.UtcNow - lastIndexingType).TotalSeconds;
            }

            result.LockMode = index.Definition.LockMode;
            result.IsInvalid = stats.IsInvalidIndex;
            result.Status = index.Status;

            result.MappedPerSecond = (int)(index.MapsPerSec?.OneMinuteRate ?? 0);
            result.ReducedPerSecond = (int)(index.ReducesPerSec?.OneMinuteRate ?? 0);

            result.Type = index.Type;
            result.EntriesCount = stats.EntriesCount;
            
            return result;
        }

        private void AssertMonitoring()
        {
            /* TODO: 
            if (ServerStore.LicenseManager.CanUseEndpointsMonitoring(withNotification: false) == false)
                throw new InvalidOperationException("Your license does not allow monitoring endpoints to be used.");
              */  
        }
    }
}
