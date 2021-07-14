//-----------------------------------------------------------------------
// <copyright file="InMemoryRavenConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.ComponentModel.Composition.Hosting;
using System.ComponentModel.Composition.Primitives;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Plugins.Catalogs;
using Raven.Database.Server;
using Raven.Database.FileSystem.Util;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Enum = System.Enum;
using Raven.Abstractions;
using Raven.Database.Impl;

namespace Raven.Database.Config
{
    public class InMemoryRavenConfiguration
    {
        public const string VoronTypeName = "voron";
        public const string EsentTypeName = "esent";

        private CompositionContainer container;
        private bool containerExternallySet;
        private string pluginsDirectory;

        public ReplicationConfiguration Replication { get; private set; }

        public SqlReplicationConfiguration SqlReplication { get; private set; }

        public PrefetcherConfiguration Prefetcher { get; private set; }

        public StorageConfiguration Storage { get; private set; }

        public FileSystemConfiguration FileSystem { get; private set; }

        public CounterConfiguration Counter { get; private set; }
        
        public TimeSeriesConfiguration TimeSeries { get; private set; }

        public EncryptionConfiguration Encryption { get; private set; }

        public IndexingConfiguration Indexing { get; set; }

        public ClusterConfiguration Cluster { get; private set; }

        public MonitoringConfiguration Monitoring { get; private set; }

        public WebSocketsConfiguration WebSockets { get; private set; }

        public StudioConfiguration Studio { get; private set; }

        public InMemoryRavenConfiguration()
        {
            Replication = new ReplicationConfiguration();
            SqlReplication = new SqlReplicationConfiguration();
            Prefetcher = new PrefetcherConfiguration();
            Storage = new StorageConfiguration();
            FileSystem = new FileSystemConfiguration();
            Counter = new CounterConfiguration();
            TimeSeries = new TimeSeriesConfiguration();
            Encryption = new EncryptionConfiguration();
            Indexing = new IndexingConfiguration();
            WebSockets = new WebSocketsConfiguration();
            Cluster = new ClusterConfiguration();
            Monitoring = new MonitoringConfiguration();
            Studio = new StudioConfiguration();

            Settings = new NameValueCollection(StringComparer.OrdinalIgnoreCase);

            CreateAutoIndexesForAdHocQueriesIfNeeded = true;

            CreatePluginsDirectoryIfNotExisting = true;
            CreateAnalyzersDirectoryIfNotExisting = true;

            IndexingClassifier = new DefaultIndexingClassifier();

            Catalog = new AggregateCatalog(CurrentAssemblyCatalog);

            Catalog.Changed += (sender, args) => ResetContainer();
        }

        public string DatabaseName { get; set; }

        public string FileSystemName { get; set; }

        public string CounterStorageName { get; set; }

        public string TimeSeriesName { get; set; }

        public void PostInit()
        {
            CheckDirectoryPermissions();

            FilterActiveBundles();

            SetupOAuth();

            SetupGC();
        }

        public InMemoryRavenConfiguration Initialize()
        {
            int defaultMaxNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 128 * 1024 : 16 * 1024;
            int defaultInitialNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 512 : 256;

            var ravenSettings = new StronglyTypedRavenSettings(Settings);
            ravenSettings.Setup(defaultMaxNumberOfItemsToIndexInSingleBatch, defaultInitialNumberOfItemsToIndexInSingleBatch);

            WorkingDirectory = CalculateWorkingDirectory(ravenSettings.WorkingDir.Value);
            DataDirectory = ravenSettings.DataDir.Value;
            FileSystem.InitializeFrom(this);
            Counter.InitializeFrom(this);
            TimeSeries.InitializeFrom(this);
            Studio.InitializeFrom(this);

            MaxPrecomputedBatchSizeForNewIndex = ravenSettings.MaxPrecomputedBatchSizeForNewIndex.Value;

            MaxPrecomputedBatchTotalDocumentSizeInBytes = ravenSettings.MaxPrecomputedBatchTotalDocumentSizeInBytes.Value;

            MaxClauseCount = ravenSettings.MaxClauseCount.Value;

            AllowScriptsToAdjustNumberOfSteps = ravenSettings.AllowScriptsToAdjustNumberOfSteps.Value;

            IndexAndTransformerReplicationLatencyInSec = ravenSettings.IndexAndTransformerReplicationLatencyInSec.Value;

            BulkImportBatchTimeout = ravenSettings.BulkImportBatchTimeout.Value;

            // Important! this value is synchronized with the max sessions number in esent
            // since we cannot have more requests in the system than we have sessions for them
            // and we also need to allow sessions for background operations and for multi get requests
            MaxConcurrentServerRequests = ravenSettings.MaxConcurrentServerRequests.Value;

            MaxConcurrentRequestsForDatabaseDuringLoad = ravenSettings.MaxConcurrentRequestsForDatabaseDuringLoad.Value;

            MaxSecondsForTaskToWaitForDatabaseToLoad = ravenSettings.MaxSecondsForTaskToWaitForDatabaseToLoad.Value;
            MaxConcurrentMultiGetRequests = ravenSettings.MaxConcurrentMultiGetRequests.Value;
            if (ConcurrentMultiGetRequests == null)
                ConcurrentMultiGetRequests = new SemaphoreSlim(MaxConcurrentMultiGetRequests);

            MemoryLimitForProcessingInMb = ravenSettings.MemoryLimitForProcessing.Value;

            LowMemoryForLinuxDetectionInMB = ravenSettings.LowMemoryLimitForLinuxDetectionInMB.Value;
            PrefetchingDurationLimit = ravenSettings.PrefetchingDurationLimit.Value;

            // Core settings

            MinThreadPoolCompletionThreads = ravenSettings.MinThreadPoolCompletionThreads.Value;
            MinThreadPoolWorkerThreads = ravenSettings.MinThreadPoolWorkerThreads.Value;
            MaxPageSize = ravenSettings.MaxPageSize.Value;

            MemoryCacheLimitMegabytes = ravenSettings.MemoryCacheLimitMegabytes.Value;

            MemoryCacheExpiration = ravenSettings.MemoryCacheExpiration.Value;

            MemoryCacheLimitPercentage = ravenSettings.MemoryCacheLimitPercentage.Value;

            MemoryCacheLimitCheckInterval = ravenSettings.MemoryCacheLimitCheckInterval.Value;

            if (!string.IsNullOrWhiteSpace(ravenSettings.MemoryCacher.Value))
            {
                CustomMemoryCacher = config =>
                {
                    var customCacherType = Type.GetType(ravenSettings.MemoryCacher.Value);

                    var argTypes = new Type[] { typeof(InMemoryRavenConfiguration) };
                    var argValues = new object[] { config };

                    var ctor = customCacherType.GetConstructor(argTypes);
                    var obj = ctor.Invoke(argValues);

                    var cacher = obj as IDocumentCacher;

                    return cacher;
                };
            }

            // Discovery
            DisableClusterDiscovery = ravenSettings.DisableClusterDiscovery.Value;

            ServerName = ravenSettings.ServerName.Value;

            MaxStepsForScript = ravenSettings.MaxStepsForScript.Value;
            AdditionalStepsForScriptBasedOnDocumentSize = ravenSettings.AdditionalStepsForScriptBasedOnDocumentSize.Value;
            TurnOffDiscoveryClient = ravenSettings.TurnOffDiscoveryClient.Value;

            // Index settings
            MaxProcessingRunLatency = ravenSettings.MaxProcessingRunLatency.Value;
            MaxIndexWritesBeforeRecreate = ravenSettings.MaxIndexWritesBeforeRecreate.Value;
            MaxSimpleIndexOutputsPerDocument = ravenSettings.MaxSimpleIndexOutputsPerDocument.Value;
            MaxMapReduceIndexOutputsPerDocument = ravenSettings.MaxMapReduceIndexOutputsPerDocument.Value;

            PrewarmFacetsOnIndexingMaxAge = ravenSettings.PrewarmFacetsOnIndexingMaxAge.Value;
            PrewarmFacetsSyncronousWaitTime = ravenSettings.PrewarmFacetsSyncronousWaitTime.Value;

            MaxNumberOfItemsToProcessInSingleBatch = ravenSettings.MaxNumberOfItemsToProcessInSingleBatch.Value;
            FlushIndexToDiskSizeInMb = ravenSettings.FlushIndexToDiskSizeInMb.Value;
            CacheDocumentsInMemory = ravenSettings.CacheDocumentsInMemory.Value;

            var initialNumberOfItemsToIndexInSingleBatch = Settings["Raven/InitialNumberOfItemsToProcessInSingleBatch"] ?? Settings["Raven/InitialNumberOfItemsToIndexInSingleBatch"];
            if (initialNumberOfItemsToIndexInSingleBatch != null)
            {
                InitialNumberOfItemsToProcessInSingleBatch = Math.Min(int.Parse(initialNumberOfItemsToIndexInSingleBatch),
                                                                    MaxNumberOfItemsToProcessInSingleBatch);
            }
            else
            {
                InitialNumberOfItemsToProcessInSingleBatch = MaxNumberOfItemsToProcessInSingleBatch == ravenSettings.MaxNumberOfItemsToProcessInSingleBatch.Default ?
                 defaultInitialNumberOfItemsToIndexInSingleBatch :
                 Math.Max(16, Math.Min(MaxNumberOfItemsToProcessInSingleBatch / 256, defaultInitialNumberOfItemsToIndexInSingleBatch));
            }
            AvailableMemoryForRaisingBatchSizeLimit = ravenSettings.AvailableMemoryForRaisingBatchSizeLimit.Value;

            MaxNumberOfItemsToReduceInSingleBatch = ravenSettings.MaxNumberOfItemsToReduceInSingleBatch.Value;
            InitialNumberOfItemsToReduceInSingleBatch = MaxNumberOfItemsToReduceInSingleBatch == ravenSettings.MaxNumberOfItemsToReduceInSingleBatch.Default ?
                 defaultInitialNumberOfItemsToIndexInSingleBatch / 2 :
                 Math.Max(16, Math.Min(MaxNumberOfItemsToProcessInSingleBatch / 256, defaultInitialNumberOfItemsToIndexInSingleBatch / 2));

            NumberOfItemsToExecuteReduceInSingleStep = ravenSettings.NumberOfItemsToExecuteReduceInSingleStep.Value;

            var initialNumberOfItemsToReduceInSingleBatch = Settings["Raven/InitialNumberOfItemsToReduceInSingleBatch"];
            if (initialNumberOfItemsToReduceInSingleBatch != null)
            {
                InitialNumberOfItemsToReduceInSingleBatch = Math.Min(int.Parse(initialNumberOfItemsToReduceInSingleBatch),
                                                                    MaxNumberOfItemsToReduceInSingleBatch);
            }

            MaxNumberOfParallelProcessingTasks = ravenSettings.MaxNumberOfParallelProcessingTasks.Value;

            NewIndexInMemoryMaxBytes = ravenSettings.NewIndexInMemoryMaxMb.Value;

            NewIndexInMemoryMaxTime = ravenSettings.NewIndexInMemoryMaxTime.Value;

            MaxIndexCommitPointStoreTimeInterval = ravenSettings.MaxIndexCommitPointStoreTimeInterval.Value;

            MinIndexingTimeIntervalToStoreCommitPoint = ravenSettings.MinIndexingTimeIntervalToStoreCommitPoint.Value;

            MaxNumberOfStoredCommitPoints = ravenSettings.MaxNumberOfStoredCommitPoints.Value;

            // Data settings
            RunInMemory = ravenSettings.RunInMemory.Value;

            if (string.IsNullOrEmpty(DefaultStorageTypeName))
            {
                DefaultStorageTypeName = ravenSettings.DefaultStorageTypeName.Value;
            }

            CreateAutoIndexesForAdHocQueriesIfNeeded = ravenSettings.CreateAutoIndexesForAdHocQueriesIfNeeded.Value;

            DatabaseOperationTimeout = ravenSettings.DatbaseOperationTimeout.Value;

            TimeToWaitBeforeRunningIdleIndexes = ravenSettings.TimeToWaitBeforeRunningIdleIndexes.Value;
            TimeToWaitBeforeMarkingAutoIndexAsIdle = ravenSettings.TimeToWaitBeforeMarkingAutoIndexAsIdle.Value;

            TimeToWaitBeforeMarkingIdleIndexAsAbandoned = ravenSettings.TimeToWaitBeforeMarkingIdleIndexAsAbandoned.Value;
            TimeToWaitBeforeRunningAbandonedIndexes = ravenSettings.TimeToWaitBeforeRunningAbandonedIndexes.Value;

            ResetIndexOnUncleanShutdown = ravenSettings.ResetIndexOnUncleanShutdown.Value;
            DisableInMemoryIndexing = ravenSettings.DisableInMemoryIndexing.Value;

            SetupTransactionMode();

            var indexStoragePathSettingValue = ravenSettings.IndexStoragePath.Value;
            if (string.IsNullOrEmpty(indexStoragePathSettingValue) == false)
            {
                IndexStoragePath = indexStoragePathSettingValue;
            }

            // HTTP settings
            HostName = ravenSettings.HostName.Value;

            ExposeConfigOverTheWire = ravenSettings.ExposeConfigOverTheWire.Value;

            if (string.IsNullOrEmpty(DatabaseName)) // we only use this for root database
            {
                Port = PortUtil.GetPort(ravenSettings.Port.Value, RunInMemory);
                Encryption.UseSsl = ravenSettings.Encryption.UseSsl.Value;
                Encryption.UseFips = ravenSettings.Encryption.UseFips.Value;
            }

            SetVirtualDirectory();

            HttpCompression = ravenSettings.HttpCompression.Value;

            AccessControlAllowOrigin = ravenSettings.AccessControlAllowOrigin.Value == null ? new HashSet<string>() : new HashSet<string>(ravenSettings.AccessControlAllowOrigin.Value.Split());
            AccessControlMaxAge = ravenSettings.AccessControlMaxAge.Value;
            AccessControlAllowMethods = ravenSettings.AccessControlAllowMethods.Value;
            AccessControlRequestHeaders = ravenSettings.AccessControlRequestHeaders.Value;

            AnonymousUserAccessMode = GetAnonymousUserAccessMode();

            RedirectStudioUrl = ravenSettings.RedirectStudioUrl.Value;

            DisableDocumentPreFetching = ravenSettings.DisableDocumentPreFetching.Value;

            MaxNumberOfItemsToPreFetch = ravenSettings.MaxNumberOfItemsToPreFetch.Value;

            // Misc settings
            WebDir = ravenSettings.WebDir.Value;

            PluginsDirectory = ravenSettings.PluginsDirectory.Value;
            AssembliesDirectory = ravenSettings.AssembliesDirectory.Value;
            CompiledIndexCacheDirectory = ravenSettings.CompiledIndexCacheDirectory.Value;

            EmbeddedFilesDirectory = ravenSettings.EmbeddedFilesDirectory.Value.ToFullPath();

            var taskSchedulerType = ravenSettings.TaskScheduler.Value;
            if (taskSchedulerType != null)
            {
                var type = Type.GetType(taskSchedulerType);
                CustomTaskScheduler = (TaskScheduler)Activator.CreateInstance(type);
            }

            RejectClientsMode = ravenSettings.RejectClientsModeEnabled.Value;

            Storage.PutSerialLockDuration = ravenSettings.PutSerialLockDuration.Value;
            Storage.SkipConsistencyCheck = ravenSettings.SkipConsistencyCheck.Value;

            // Voron settings
            Storage.Voron.MaxBufferPoolSize = Math.Max(2, ravenSettings.Voron.MaxBufferPoolSize.Value);
            Storage.Voron.InitialFileSize = ravenSettings.Voron.InitialFileSize.Value;
            Storage.Voron.MaxScratchBufferSize = ravenSettings.Voron.MaxScratchBufferSize.Value;
            Storage.Voron.MaxSizePerScratchBufferFile = ravenSettings.Voron.MaxSizePerScratchBufferFile.Value;
            Storage.Voron.ScratchBufferSizeNotificationThreshold = ravenSettings.Voron.ScratchBufferSizeNotificationThreshold.Value;
            Storage.Voron.AllowIncrementalBackups = ravenSettings.Voron.AllowIncrementalBackups.Value;
            Storage.Voron.TempPath = ravenSettings.Voron.TempPath.Value;
            Storage.Voron.JournalsStoragePath = ravenSettings.Voron.JournalsStoragePath.Value;
            Storage.Voron.AllowOn32Bits = ravenSettings.Voron.AllowOn32Bits.Value;

            // Esent settings
            Storage.Esent.JournalsStoragePath = ravenSettings.Esent.JournalsStoragePath.Value;
            Storage.Esent.CacheSizeMax = ravenSettings.Esent.CacheSizeMax.Value;
            Storage.Esent.MaxVerPages = ravenSettings.Esent.MaxVerPages.Value;
            Storage.Esent.PreferredVerPages = ravenSettings.Esent.PreferredVerPages.Value;
            Storage.Esent.DbExtensionSize = ravenSettings.Esent.DbExtensionSize.Value;
            Storage.Esent.LogFileSize = ravenSettings.Esent.LogFileSize.Value;
            Storage.Esent.LogBuffers = ravenSettings.Esent.LogBuffers.Value;
            Storage.Esent.MaxCursors = ravenSettings.Esent.MaxCursors.Value;
            Storage.Esent.CircularLog = ravenSettings.Esent.CircularLog.Value;
            Storage.Esent.MaxSessions = ravenSettings.Esent.MaxSessions.Value;
            Storage.Esent.CheckpointDepthMax = ravenSettings.Esent.CheckpointDepthMax.Value;
            Storage.Esent.MaxInstances = ravenSettings.Esent.MaxInstances.Value;

            Storage.PreventSchemaUpdate = ravenSettings.FileSystem.PreventSchemaUpdate.Value;

            Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds = ravenSettings.Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds.Value;
            Prefetcher.MaximumSizeAllowedToFetchFromStorageInMb = ravenSettings.Prefetcher.MaximumSizeAllowedToFetchFromStorageInMb.Value;

            Replication.FetchingFromDiskTimeoutInSeconds = ravenSettings.Replication.FetchingFromDiskTimeoutInSeconds.Value;
            Replication.ReplicationRequestTimeoutInMilliseconds = ravenSettings.Replication.ReplicationRequestTimeoutInMilliseconds.Value;
            Replication.ForceReplicationRequestBuffering = ravenSettings.Replication.ForceReplicationRequestBuffering.Value;
            Replication.MaxNumberOfItemsToReceiveInSingleBatch = ravenSettings.Replication.MaxNumberOfItemsToReceiveInSingleBatch.Value;
            Replication.ReplicationPropagationDelayInSeconds = ravenSettings.Replication.ReplicationPropagationDelayInSeconds.Value;
            Replication.CertificatePath = ravenSettings.Replication.CertificatePath.Value;
            Replication.CertificatePassword = ravenSettings.Replication.CertificatePassword.Value;

            SqlReplication.CommandTimeoutInSec = ravenSettings.SqlReplication.CommandTimeoutInSec.Value;

            FileSystem.MaximumSynchronizationInterval = ravenSettings.FileSystem.MaximumSynchronizationInterval.Value;
            FileSystem.DataDirectory = ravenSettings.FileSystem.DataDir.Value;
            FileSystem.IndexStoragePath = ravenSettings.FileSystem.IndexStoragePath.Value;
            if (string.IsNullOrEmpty(FileSystem.DefaultStorageTypeName))
                FileSystem.DefaultStorageTypeName = ravenSettings.FileSystem.DefaultStorageTypeName.Value;
            FileSystem.DisableRDC = ravenSettings.FileSystem.DisableRDC.Value;
            FileSystem.SynchronizationBatchProcessing = ravenSettings.FileSystem.SynchronizationBatchProcessing.Value;

            Studio.AllowNonAdminUsersToSetupPeriodicExport = ravenSettings.Studio.AllowNonAdminUsersToSetupPeriodicExport.Value;

            Counter.DataDirectory = ravenSettings.Counter.DataDir.Value;
            Counter.TombstoneRetentionTime = ravenSettings.Counter.TombstoneRetentionTime.Value;
            Counter.DeletedTombstonesInBatch = ravenSettings.Counter.DeletedTombstonesInBatch.Value;
            Counter.ReplicationLatencyInMs = ravenSettings.Counter.ReplicationLatencyInMs.Value;
            Counter.BatchTimeout = ravenSettings.Counter.BatchTimeout.Value;

            TimeSeries.DataDirectory = ravenSettings.TimeSeries.DataDir.Value;
            TimeSeries.TombstoneRetentionTime = ravenSettings.TimeSeries.TombstoneRetentionTime.Value;
            TimeSeries.DeletedTombstonesInBatch = ravenSettings.TimeSeries.DeletedTombstonesInBatch.Value;
            TimeSeries.ReplicationLatencyInMs = ravenSettings.TimeSeries.ReplicationLatencyInMs.Value;

            Encryption.EncryptionKeyBitsPreference = ravenSettings.Encryption.EncryptionKeyBitsPreference.Value;

            Indexing.MaxNumberOfItemsToProcessInTestIndexes = ravenSettings.Indexing.MaxNumberOfItemsToProcessInTestIndexes.Value;
            Indexing.MaxNumberOfStoredIndexingBatchInfoElements = ravenSettings.Indexing.MaxNumberOfStoredIndexingBatchInfoElements.Value;
            Indexing.UseLuceneASTParser = ravenSettings.Indexing.UseLuceneASTParser.Value;
            Indexing.DisableIndexingFreeSpaceThreshold = ravenSettings.Indexing.DisableIndexingFreeSpaceThreshold.Value;
            Indexing.DisableMapReduceInMemoryTracking = ravenSettings.Indexing.DisableMapReduceInMemoryTracking.Value;
            Indexing.SkipRecoveryOnStartup = ravenSettings.Indexing.SkipRecoveryOnStartup.Value;

            Cluster.ElectionTimeout = ravenSettings.Cluster.ElectionTimeout.Value;
            Cluster.HeartbeatTimeout = ravenSettings.Cluster.HeartbeatTimeout.Value;
            Cluster.MaxLogLengthBeforeCompaction = ravenSettings.Cluster.MaxLogLengthBeforeCompaction.Value;
            Cluster.MaxEntriesPerRequest = ravenSettings.Cluster.MaxEntriesPerRequest.Value;
            Cluster.MaxStepDownDrainTime = ravenSettings.Cluster.MaxStepDownDrainTime.Value;
            Cluster.MaxReplicationLatency = ravenSettings.Cluster.MaxReplicationLatency.Value;

            TombstoneRetentionTime = ravenSettings.TombstoneRetentionTime.Value;

            ImplicitFetchFieldsFromDocumentMode = ravenSettings.ImplicitFetchFieldsFromDocumentMode.Value;

            IgnoreSslCertificateErrors = GetIgnoreSslCertificateErrorModeMode();

            WebSockets.InitialBufferPoolSize = ravenSettings.WebSockets.InitialBufferPoolSize.Value;

            MaxConcurrentResourceLoads = ravenSettings.MaxConcurrentResourceLoads.Value;
            ConcurrentResourceLoadTimeout = ravenSettings.ConcurrentResourceLoadTimeout.Value;
            TempPath = ravenSettings.TempPath.Value;

            FillMonitoringSettings(ravenSettings);

            PostInit();

            return this;
        }

        private void FillMonitoringSettings(StronglyTypedRavenSettings settings)
        {
            Monitoring.Snmp.Enabled = settings.Monitoring.Snmp.Enabled.Value;
            Monitoring.Snmp.Community = settings.Monitoring.Snmp.Community.Value;
            Monitoring.Snmp.Port = settings.Monitoring.Snmp.Port.Value;
        }

        private static string CalculateWorkingDirectory(string workingDirectory)
        {
            if (string.IsNullOrEmpty(workingDirectory))
                workingDirectory = @"~\";

            if (workingDirectory.StartsWith("APPDRIVE:", StringComparison.OrdinalIgnoreCase))
            {
                var baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
                var rootPath = Path.GetPathRoot(baseDirectory);
                if (string.IsNullOrEmpty(rootPath) == false)
                    workingDirectory = Regex.Replace(workingDirectory, "APPDRIVE:", rootPath.TrimEnd('\\'), RegexOptions.IgnoreCase);
            }

            return FilePathTools.MakeSureEndsWithSlash(workingDirectory.ToFullPath());
        }

        public int MaxPrecomputedBatchSizeForNewIndex { get; set; }

        public int MaxPrecomputedBatchTotalDocumentSizeInBytes { get; set; }

        public TimeSpan ConcurrentResourceLoadTimeout { get; private set; }

        public int MaxConcurrentResourceLoads { get; private set; }

        public int MaxClauseCount { get; set; }

        public int MaxSecondsForTaskToWaitForDatabaseToLoad { get; set; }

        public int IndexAndTransformerReplicationLatencyInSec { get; internal set; }

        public bool AllowScriptsToAdjustNumberOfSteps { get; set; }

        /// <summary>
        /// Determines how long replication and periodic backup tombstones will be kept by a database. After the specified time they will be automatically
        /// purged on next database startup. Default: 14 days.
        /// </summary>
        public TimeSpan TombstoneRetentionTime { get; set; }

        public int MaxConcurrentServerRequests { get; set; }

        public int MaxConcurrentRequestsForDatabaseDuringLoad { get; set; }

        public int MaxConcurrentMultiGetRequests { get; set; }

        public int PrefetchingDurationLimit { get; private set; }

        public TimeSpan BulkImportBatchTimeout { get; set; }

        /// <summary>
        /// This limits the number of concurrent multi get requests,
        /// Note that this plays with the max number of requests allowed as well as the max number
        /// of sessions
        /// </summary>
        [JsonIgnore]
        public SemaphoreSlim ConcurrentMultiGetRequests;

        /// <summary>
        /// The time to wait before canceling a database operation such as load (many) or query
        /// </summary>
        public TimeSpan DatabaseOperationTimeout { get; set; }

        public TimeSpan TimeToWaitBeforeRunningIdleIndexes { get; internal set; }

        public TimeSpan TimeToWaitBeforeRunningAbandonedIndexes { get; private set; }

        public TimeSpan TimeToWaitBeforeMarkingAutoIndexAsIdle { get; private set; }

        public TimeSpan TimeToWaitBeforeMarkingIdleIndexAsAbandoned { get; private set; }

        public TimeSpan CheckReferenceBecauseOfDocumentUpdateTimeout { get; set; }

        private void CheckDirectoryPermissions()
        {
            var tempPath = TempPath;
            var tempFileName = Guid.NewGuid().ToString("N");
            var tempFilePath = Path.Combine(tempPath, tempFileName);

            try
            {
                IOExtensions.CreateDirectoryIfNotExists(tempPath);
                File.WriteAllText(tempFilePath, string.Empty);
                File.Delete(tempFilePath);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(string.Format("Could not access temp path '{0}'. Please check if you have sufficient privileges to access this path or change 'Raven/TempPath' value.", tempPath), e);
            }
        }

        private void FilterActiveBundles()
        {
            if (container != null)
                container.Dispose();
            container = null;

            var catalog = GetUnfilteredCatalogs(Catalog.Catalogs);
            Catalog = new AggregateCatalog(new List<ComposablePartCatalog> { new BundlesFilteredCatalog(catalog, ActiveBundles.ToArray()) });
        }

        public IEnumerable<string> ActiveBundles
        {
            get
            {
                var activeBundles = Settings[Constants.ActiveBundles] ?? string.Empty;

                return BundlesHelper.ProcessActiveBundles(activeBundles)
                    .GetSemicolonSeparatedValues()
                    .Distinct();
            }
        }

        private HashSet<string> headersToIgnore;
        public HashSet<string> HeadersToIgnore
        {
            get
            {
                if (headersToIgnore != null)
                    return headersToIgnore;

                var headers = Settings["Raven/Headers/Ignore"] ?? string.Empty;
                return headersToIgnore = new HashSet<string>(headers.GetSemicolonSeparatedValues(), StringComparer.OrdinalIgnoreCase);
            }
        }

        internal static ComposablePartCatalog GetUnfilteredCatalogs(ICollection<ComposablePartCatalog> catalogs)
        {
            if (catalogs.Count != 1)
                return new AggregateCatalog(catalogs.Select(GetUnfilteredCatalog));
            return GetUnfilteredCatalog(catalogs.First());
        }

        private static ComposablePartCatalog GetUnfilteredCatalog(ComposablePartCatalog x)
        {
            var filteredCatalog = x as BundlesFilteredCatalog;
            if (filteredCatalog != null)
                return GetUnfilteredCatalog(filteredCatalog.CatalogToFilter);
            return x;
        }

        public TaskScheduler CustomTaskScheduler { get; set; }

        public string RedirectStudioUrl { get; set; }

        private void SetupTransactionMode()
        {
            var transactionMode = Settings["Raven/TransactionMode"];
            TransactionMode result;
            if (Enum.TryParse(transactionMode, true, out result) == false)
                result = TransactionMode.Safe;
            TransactionMode = result;
        }

        private void SetVirtualDirectory()
        {
            var defaultVirtualDirectory = "/";
            try
            {
                if (HttpContext.Current != null)
                    defaultVirtualDirectory = HttpContext.Current.Request.ApplicationPath;
            }
            catch (HttpException)
            {
                // explicitly ignoring this because we might be running in embedded mode
                // inside IIS during init stages, in which case we can't access the HttpContext
                // nor do we actually care
            }

            VirtualDirectory = Settings["Raven/VirtualDirectory"] ?? defaultVirtualDirectory;

        }

        public bool UseDefaultOAuthTokenServer
        {
            get { return Settings["Raven/OAuthTokenServer"] == null; }
        }

        private void SetupOAuth()
        {
            OAuthTokenServer = Settings["Raven/OAuthTokenServer"] ??
                               (ServerUrl.EndsWith("/") ? ServerUrl + "OAuth/API-Key" : ServerUrl + "/OAuth/API-Key");
            OAuthTokenKey = GetOAuthKey();
        }

        private void SetupGC()
        {
            //GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
        }

        private static readonly Lazy<byte[]> DefaultOauthKey = new Lazy<byte[]>(() =>
            {
                using (var rsa = Encryptor.Current.CreateAsymmetrical())
                {
                    return rsa.ExportCspBlob(true);
                }
            });

        private byte[] GetOAuthKey()
        {
            var key = Settings["Raven/OAuthTokenCertificate"];
            if (string.IsNullOrEmpty(key) == false)
            {
                return Convert.FromBase64String(key);
            }
            return DefaultOauthKey.Value; // ensure we only create this once per process
        }

        public NameValueCollection Settings { get; set; }

        public string ServerUrl
        {
            get
            {
                HttpRequest httpRequest = null;
                try
                {
                    if (HttpContext.Current != null)
                        httpRequest = HttpContext.Current.Request;
                }
                catch (Exception)
                {
                    // the issue is probably Request is not available in this context
                    // we can safely ignore this, at any rate
                }
                if (httpRequest != null)// running in IIS, let us figure out how
                {
                    var url = httpRequest.Url;
                    return new UriBuilder(url)
                    {
                        Path = httpRequest.ApplicationPath,
                        Query = ""
                    }.Uri.ToString();
                }
                return new UriBuilder(Encryption.UseSsl ? "https" : "http", (HostName ?? Environment.MachineName), Port, VirtualDirectory).Uri.ToString();
            }
        }

        #region Core settings

        /// <summary>
        /// When the database is shut down rudely, determine whatever to reset the index or to check it.
        /// Checking the index may take some time on large databases
        /// </summary>
        public bool ResetIndexOnUncleanShutdown { get; set; }

        /// <summary>
        /// Minimum threads for .net thread pool worker threads
        /// Default: system default
        /// Min: 2
        /// </summary>
        public int MinThreadPoolWorkerThreads { get; set; }

        /// <summary>
        /// Minimum threads for .net thread pool async io completion threads
        /// Default: system default
        /// Min: 2
        /// </summary>
        public int MinThreadPoolCompletionThreads { get; set; }


        /// <summary>
        /// The maximum allowed page size for queries. 
        /// Default: 1024
        /// Minimum: 10
        /// </summary>
        public int MaxPageSize { get; set; }

        /// <summary>
        /// Percentage of physical memory used for caching
        /// Allowed values: 0-99 (0 = autosize)
        /// </summary>
        public int MemoryCacheLimitPercentage { get; set; }

        /// <summary>
        /// An integer value that specifies the maximum allowable size, in megabytes, that caching 
        /// document instances will use
        /// </summary>
        public int MemoryCacheLimitMegabytes { get; set; }

        /// <summary>
        /// Interval for checking the memory cache limits
        /// Allowed values: max precision is 1 second
        /// Default: 00:02:00 (or value provided by system.runtime.caching app config)
        /// </summary>
        public TimeSpan MemoryCacheLimitCheckInterval { get; set; }
        #endregion

        #region Index settings

        /// <summary>
        /// The indexing scheduler to use
        /// </summary>
        public IIndexingClassifier IndexingClassifier { get; set; }

        /// <summary>
        /// Max number of items to take for indexing in a batch
        /// Minimum: 128
        /// </summary>
        public int MaxNumberOfItemsToProcessInSingleBatch { get; set; }

        /// <summary>
        /// The initial number of items to take when processing a batch
        /// Default: 512 or 256 depending on CPU architecture
        /// </summary>
        public int InitialNumberOfItemsToProcessInSingleBatch { get; set; }

        /// <summary>
        /// Max number of items to take for reducing in a batch
        /// Minimum: 128
        /// </summary>
        public int MaxNumberOfItemsToReduceInSingleBatch { get; set; }

        /// <summary>
        /// The initial number of items to take when reducing a batch
        /// Default: 256 or 128 depending on CPU architecture
        /// </summary>
        public int InitialNumberOfItemsToReduceInSingleBatch { get; set; }

        /// <summary>
        /// The number that controls the if single step reduce optimization is performed.
        /// If the count of mapped results if less than this value then the reduce is executed in single step.
        /// Default: 1024
        /// </summary>
        public int NumberOfItemsToExecuteReduceInSingleStep { get; set; }

        /// <summary>
        /// The maximum number of indexing, replication and sql replication tasks allowed to run in parallel
        /// Default: The number of processors in the current machine
        /// </summary>
        public int MaxNumberOfParallelProcessingTasks
        {
            get
            {
                if (MemoryStatistics.MaxParallelismSet)
                    return Math.Min(maxNumberOfParallelIndexTasks ?? MemoryStatistics.MaxParallelism, MemoryStatistics.MaxParallelism);
                return maxNumberOfParallelIndexTasks ?? Environment.ProcessorCount;
            }
            set
            {
                if (value == 0)
                    throw new ArgumentException("You cannot set the number of parallel tasks to zero");
                maxNumberOfParallelIndexTasks = value;
            }
        }

        /// <summary>
        /// New indexes are kept in memory until they reach this integer value in bytes or until they're non-stale
        /// Default: 64 MB
        /// Minimum: 1 MB
        /// </summary>
        public int NewIndexInMemoryMaxBytes { get; set; }

        #endregion

        #region HTTP settings

        /// <summary>
        /// The hostname to use when creating the http listener (null to accept any hostname or address)
        /// Default: none, binds to all host names
        /// </summary>
        public string HostName { get; set; }

        /// <summary>
        /// The port to use when creating the http listener. 
        /// Default: 8080. You can set it to *, in which case it will find the first available port from 8080 and upward.
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Allow to get config information over the wire.
        /// Applies to endpoints: /debug/config, /debug...
        /// Default: Open. You can set it to AdminOnly.
        /// </summary>
        public string ExposeConfigOverTheWire { get; set; }

        /// <summary>
        /// Determine the value of the Access-Control-Allow-Origin header sent by the server. 
        /// Indicates the URL of a site trusted to make cross-domain requests to this server.
        /// Allowed values: null (don't send the header), *, http://example.org (space separated if multiple sites)
        /// </summary>
        public HashSet<string> AccessControlAllowOrigin { get; set; }

        /// <summary>
        /// Determine the value of the Access-Control-Max-Age header sent by the server.
        /// Indicates how long (seconds) the browser should cache the Access Control settings.
        /// Ignored if AccessControlAllowOrigin is not specified.
        /// Default: 1728000 (20 days)
        /// </summary>
        public string AccessControlMaxAge { get; set; }

        /// <summary>
        /// Determine the value of the Access-Control-Allow-Methods header sent by the server.
        /// Indicates which HTTP methods (verbs) are permitted for requests from allowed cross-domain origins.
        /// Ignored if AccessControlAllowOrigin is not specified.
        /// Default: PUT,PATCH,GET,DELETE,POST
        /// </summary>
        public string AccessControlAllowMethods { get; set; }

        /// <summary>
        /// Determine the value of the Access-Control-Request-Headers header sent by the server.
        /// Indicates which HTTP headers are permitted for requests from allowed cross-domain origins.
        /// Ignored if AccessControlAllowOrigin is not specified.
        /// Allowed values: null (allow whatever headers are being requested), HTTP header field name
        /// </summary>
        public string AccessControlRequestHeaders { get; set; }

        private string virtualDirectory;

        /// <summary>
        /// The virtual directory to use when creating the http listener. 
        /// Default: / 
        /// </summary>
        public string VirtualDirectory
        {
            get { return virtualDirectory; }
            set
            {
                virtualDirectory = value;

                if (virtualDirectory.EndsWith("/"))
                    virtualDirectory = virtualDirectory.Substring(0, virtualDirectory.Length - 1);
                if (virtualDirectory.StartsWith("/") == false)
                    virtualDirectory = "/" + virtualDirectory;
            }
        }

        /// <summary>
        /// Whether to use http compression or not. 
        /// Allowed values: true/false; 
        /// Default: true
        /// </summary>
        public bool HttpCompression { get; set; }

        /// <summary>
        /// Defines which operations are allowed for anonymous users.
        /// Allowed values: All, Get, None
        /// Default: Get
        /// </summary>
        public AnonymousUserAccessMode AnonymousUserAccessMode { get; set; }
        
        /// <summary>
        /// If set all client request to the server will be rejected with 
        /// the http 503 response.
        /// Other servers or the studio could still access the server.
        /// </summary>
        public bool RejectClientsMode { get; set; }

        /// <summary>
        /// The certificate to use when verifying access token signatures for OAuth
        /// </summary>
        public byte[] OAuthTokenKey { get; set; }

        public IgnoreSslCertificateErrorsMode IgnoreSslCertificateErrors { get; set; }

        #endregion

        #region Data settings

        public string WorkingDirectory { get; private set; }

        /// <summary>
        /// The directory for the RavenDB database. 
        /// You can use the ~\ prefix to refer to RavenDB's base directory. 
        /// Default: ~\Databases\System
        /// </summary>
        public string DataDirectory
        {
            get { return countersDataDirectory; }
            set { countersDataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(WorkingDirectory, value); }
        }

        /// <summary>
        /// What storage type to use (see: RavenDB Storage engines)
        /// Allowed values: esent, voron
        /// Default: esent
        /// </summary>
        public string DefaultStorageTypeName
        {
            get { return defaultStorageTypeName; }
            set { if (!string.IsNullOrEmpty(value)) defaultStorageTypeName = value; }
        }
        private string defaultStorageTypeName;

        private bool runInMemory;

        /// <summary>
        /// Should RavenDB's storage be in-memory. If set to true, Voron would be used as the
        /// storage engine, regardless of what was specified for StorageTypeName
        /// Allowed values: true/false
        /// Default: false
        /// </summary>
        public bool RunInMemory
        {
            get { return runInMemory; }
            set
            {
                runInMemory = value;
                Settings[Constants.RunInMemory] = value.ToString();
            }
        }

        /// <summary>
        /// Prevent index from being kept in memory. Default: false
        /// </summary>
        public bool DisableInMemoryIndexing { get; set; }

        /// <summary>
        /// What sort of transaction mode to use. 
        /// Allowed values: 
        /// Lazy - faster, but can result in data loss in the case of server crash. 
        /// Safe - slower, but will never lose data 
        /// Default: Safe 
        /// </summary>
        public TransactionMode TransactionMode { get; set; }

        #endregion

        #region Misc settings

        /// <summary>
        /// The directory to search for RavenDB's WebUI. 
        /// This is usually only useful if you are debugging RavenDB's WebUI. 
        /// Default: ~/Raven/WebUI 
        /// </summary>
        public string WebDir { get; set; }

        /// <summary>
        /// Where to look for plugins for RavenDB. 
        /// Default: ~\Plugins
        /// </summary>
        public string PluginsDirectory
        {
            get { return pluginsDirectory; }
            set
            {
                ResetContainer();
                // remove old directory catalog
                var matchingCatalogs = Catalog.Catalogs.OfType<DirectoryCatalog>()
                    .Concat(Catalog.Catalogs.OfType<Plugins.Catalogs.FilteredCatalog>()
                                .Select(x => x.CatalogToFilter as DirectoryCatalog)
                                .Where(x => x != null)
                    )
                    .Where(c => c.Path == pluginsDirectory)
                    .ToArray();
                foreach (var cat in matchingCatalogs)
                {
                    Catalog.Catalogs.Remove(cat);
                }

                pluginsDirectory = FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(WorkingDirectory, value);

                // add new one
                if (Directory.Exists(pluginsDirectory))
                {
                    var patterns = Settings["Raven/BundlesSearchPattern"] ?? "*.dll";
                    foreach (var pattern in patterns.Split(new[] { ";" }, StringSplitOptions.RemoveEmptyEntries))
                    {
                        Catalog.Catalogs.Add(new BuiltinFilteringCatalog(new DirectoryCatalog(pluginsDirectory, pattern)));
                    }
                }
            }
        }

        private string assembliesDirectory;

        /// <summary>
        /// Where the internal assemblies will be extracted to.
        /// Default: ~\Assemblies
        /// </summary>
        public string AssembliesDirectory
        {
            get
            {
                return assembliesDirectory;
            }
            set
            {
                assembliesDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(WorkingDirectory, value);
            }
        }

        /// <summary>
        /// Where we search for embedded files.
        /// Default: null
        /// </summary>
        public string EmbeddedFilesDirectory { get; set; }

        public bool CreatePluginsDirectoryIfNotExisting { get; set; }
        public bool CreateAnalyzersDirectoryIfNotExisting { get; set; }

        private string compiledIndexCacheDirectory;

        /// <summary>
        /// Where to cache the compiled indexes. Absolute path or relative to TEMP directory.
        /// Default: ~\CompiledIndexCache
        /// </summary>
        public string CompiledIndexCacheDirectory
        {
            get
            {
                return compiledIndexCacheDirectory;
            }
            set
            {
                compiledIndexCacheDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(WorkingDirectory, value);
            }
        }

        public string OAuthTokenServer { get; set; }

        #endregion

        [JsonIgnore]
        public CompositionContainer Container
        {
            get { return container ?? (container = new CompositionContainer(Catalog, CompositionOptions.IsThreadSafe)); }
            set
            {
                containerExternallySet = true;
                container = value;
            }
        }

        public bool DisableDocumentPreFetching { get; set; }

        public int MaxNumberOfItemsToPreFetch { get; set; }

        [JsonIgnore]
        public AggregateCatalog Catalog { get; set; }
        public bool RunInUnreliableYetFastModeThatIsNotSuitableForProduction { get; set; }

        private string indexStoragePath;

        private string countersDataDirectory;
        private int? maxNumberOfParallelIndexTasks;

        //this is static so repeated initializations in the same process would not trigger reflection on all MEF plugins
        private readonly static AssemblyCatalog CurrentAssemblyCatalog = new AssemblyCatalog(typeof(DocumentDatabase).Assembly);

        /// <summary>
        /// The expiration value for documents in the internal managed cache
        /// </summary>
        public TimeSpan MemoryCacheExpiration { get; set; }

        /// <summary>
        /// Controls whatever RavenDB will create temporary indexes 
        /// for queries that cannot be directed to standard indexes
        /// </summary>
        public bool CreateAutoIndexesForAdHocQueriesIfNeeded { get; set; }

        /// <summary>
        /// Maximum time interval for storing commit points for map indexes when new items were added.
        /// The commit points are used to restore index if unclean shutdown was detected.
        /// Default: 00:05:00 
        /// </summary>
        public TimeSpan MaxIndexCommitPointStoreTimeInterval { get; set; }

        /// <summary>
        /// Minumum interval between between successive indexing that will allow to store a  commit point
        /// Default: 00:01:00
        /// </summary>
        public TimeSpan MinIndexingTimeIntervalToStoreCommitPoint { get; set; }

        /// <summary>
        /// Maximum number of kept commit points to restore map index after unclean shutdown
        /// Default: 5
        /// </summary>
        public int MaxNumberOfStoredCommitPoints { get; set; }

        /// <summary>
        /// Limit of how much memory a batch processing can take (in MBytes)
        /// </summary>
        public int MemoryLimitForProcessingInMb { get; set; }

        /// <summary>
        /// Custom MemoryCacher type to use for caching database documents.
        /// </summary>
        public Func<InMemoryRavenConfiguration, IDocumentCacher> CustomMemoryCacher { get; set; }

        public long DynamicMemoryLimitForProcessing
        {
            get
            {
                var availableMemory = MemoryStatistics.AvailableMemoryInMb;
                var minFreeMemory = (MemoryLimitForProcessingInMb * 2L);
                // we have more memory than the twice the limit, we can use the default limit
                if (availableMemory > minFreeMemory)
                    return MemoryLimitForProcessingInMb * 1024L * 1024L;

                // we don't have enough room to play with, if two databases will request the max memory limit
                // at the same time, we'll start paging because we'll run out of free memory. 
                // Because of that, we'll dynamically adjust the amount
                // of memory available for processing based on the amount of memory we actually have available,
                // assuming that we have multiple concurrent users of memory at the same time.
                // we limit that at 16 MB, if we have less memory than that, we can't really do much anyway
                return Math.Min(availableMemory * 1024L * 1024L / 4, 16 * 1024 * 1024);

            }
        }

        // <summary>
        /// Limit for low mem detection in linux
        /// </summary>
        public int LowMemoryForLinuxDetectionInMB { get; set; }

        public string IndexStoragePath
        {
            get
            {
                if (string.IsNullOrEmpty(indexStoragePath))
                    indexStoragePath = Path.Combine(DataDirectory, "Indexes");
                return indexStoragePath;
            }
            set { indexStoragePath = value.ToFullPath(); }
        }

        public int AvailableMemoryForRaisingBatchSizeLimit { get; set; }

        public TimeSpan MaxProcessingRunLatency { get; set; }

        internal bool IsTenantDatabase { get; set; }

        /// <summary>
        /// If True, cluster discovery will be disabled. Default is False
        /// </summary>
        public bool DisableClusterDiscovery { get; set; }

        /// <summary>
        /// If True, turns off the discovery client.
        /// </summary>
        public bool TurnOffDiscoveryClient { get; set; }

        /// <summary>
        /// The server name
        /// </summary>
        public string ServerName { get; set; }

        /// <summary>
        /// The maximum number of steps (instructions) to give a script before timing out.
        /// Default: 10,000
        /// </summary>
        public int MaxStepsForScript { get; set; }

        /// <summary>
        /// The number of additional steps to add to a given script based on the processed document's quota.
        /// Set to 0 to give use a fixed size quota. This value is multiplied with the doucment size.
        /// Default: 5
        /// </summary>
        public int AdditionalStepsForScriptBasedOnDocumentSize { get; set; }

        public int MaxIndexWritesBeforeRecreate { get; set; }

        /// <summary>
        /// Limits the number of map outputs that a simple index is allowed to create for a one source document. If a map operation applied to the one document
        /// produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and
        /// the appropriate error message will be added to the indexing errors.
        /// Default value: 15. In order to disable this check set value to -1.
        /// </summary>
        public int MaxSimpleIndexOutputsPerDocument { get; set; }

        /// <summary>
        /// Limits the number of map outputs that a map-reduce index is allowed to create for a one source document. If a map operation applied to the one document
        /// produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document will be skipped and
        /// the appropriate error message will be added to the indexing errors.
        /// Default value: 50. In order to disable this check set value to -1.
        /// </summary>
        public int MaxMapReduceIndexOutputsPerDocument { get; set; }

        /// <summary>
        /// What is the maximum age of a facet query that we should consider when prewarming
        /// the facet cache when finishing an indexing batch
        /// </summary>
        [Browsable(false)]
        public TimeSpan PrewarmFacetsOnIndexingMaxAge { get; set; }

        /// <summary>
        /// The time we should wait for pre-warming the facet cache from existing query after an indexing batch
        /// in a syncronous manner (after that, the pre warm still runs, but it will do so in a background thread).
        /// Facet queries that will try to use it will have to wait until it is over
        /// </summary>
        public TimeSpan PrewarmFacetsSyncronousWaitTime { get; set; }

        /// <summary>
        /// Indexes are flushed to a disk only if their in-memory size exceed the specified value. Default: 5MB
        /// </summary>
        public long FlushIndexToDiskSizeInMb { get; set; }

        public bool EnableResponseLoggingForEmbeddedDatabases { get; set; }

        /// <summary>
        /// Maximum number of blocks that are cached in embedded mode in the response stream. Default: 0 (Unlimited)
        /// </summary>
        public int EmbeddedResponseStreamMaxCachedBlocks { get; set; }

        /// <summary>
        /// How long can we keep the new index in memory before we have to flush it
        /// </summary>
        public TimeSpan NewIndexInMemoryMaxTime { get; set; }

        /// <summary>
        /// How FieldsToFetch are extracted from the document.
        /// Default: Enabled. 
        /// Other values are: 
        ///     DoNothing (fields are not fetched from the document)
        ///     Exception (an exception is thrown if we need to fetch fields from the document itself)
        /// </summary>
        public ImplicitFetchFieldsMode ImplicitFetchFieldsFromDocumentMode { get; set; }

        /// <summary>
        /// Use memory cache as document cacher
        /// </summary>
        public bool CacheDocumentsInMemory { get; set; }

        /// <summary>
        /// Path to temporary directory used by server.
        /// Default: Current user's temporary directory
        /// </summary>
        public string TempPath { get; set; }
        
        [Browsable(false)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void SetSystemDatabase()
        {
            IsTenantDatabase = false;
        }

        [Browsable(false)]
        [EditorBrowsable(EditorBrowsableState.Never)]
        public bool IsSystemDatabase()
        {
            return IsTenantDatabase == false;
        }

        protected void ResetContainer()
        {
            if (Container != null && containerExternallySet == false)
            {
                Container.Dispose();
                Container = null;
                containerExternallySet = false;
            }
        }

        protected AnonymousUserAccessMode GetAnonymousUserAccessMode()
        {
            if (string.IsNullOrEmpty(Settings["Raven/AnonymousAccess"]) == false)
            {
                var val = Enum.Parse(typeof(AnonymousUserAccessMode), Settings["Raven/AnonymousAccess"]);
                return (AnonymousUserAccessMode)val;
            }
            return AnonymousUserAccessMode.Admin;
        }

        protected IgnoreSslCertificateErrorsMode GetIgnoreSslCertificateErrorModeMode()
        {
            if (string.IsNullOrEmpty(Settings["Raven/IgnoreSslCertificateErrors"]) == false)
            {
                var val = Enum.Parse(typeof(IgnoreSslCertificateErrorsMode), Settings["Raven/IgnoreSslCertificateErrors"]);
                return (IgnoreSslCertificateErrorsMode)val;
            }
            return IgnoreSslCertificateErrorsMode.None;
        }

        public Uri GetFullUrl(string baseUrl)
        {
            baseUrl = Uri.EscapeUriString(baseUrl);

            if (baseUrl.StartsWith("/"))
                baseUrl = baseUrl.Substring(1);

            var url = VirtualDirectory.EndsWith("/") ? VirtualDirectory + baseUrl : VirtualDirectory + "/" + baseUrl;
            return new Uri(url, UriKind.RelativeOrAbsolute);
        }

        public T? GetConfigurationValue<T>(string configName) where T : struct
        {
            // explicitly fail if we can't convert it
            if (string.IsNullOrEmpty(Settings[configName]) == false)
                return (T)Convert.ChangeType(Settings[configName], typeof(T));
            return null;
        }

        [CLSCompliant(false)]
        public ITransactionalStorage CreateTransactionalStorage(string storageEngine, Action notifyAboutWork, Action handleStorageInaccessible, Action onNestedTransactionEnter = null, Action onNestedTransactionExit = null)
        {
            if (EnvironmentUtils.RunningOnPosix)
                storageEngine = "voron";
            storageEngine = StorageEngineAssemblyNameByTypeName(storageEngine);
            var type = Type.GetType(storageEngine);

            if (type == null)
                throw new InvalidOperationException("Could not find transactional storage type: " + storageEngine);
            Action dummyAction = () => { };

            return (ITransactionalStorage)Activator.CreateInstance(type, this, notifyAboutWork, handleStorageInaccessible, onNestedTransactionEnter ?? dummyAction, onNestedTransactionExit ?? dummyAction);
        }


        public static string StorageEngineAssemblyNameByTypeName(string typeName)
        {
            switch (typeName.ToLowerInvariant())
            {
                case EsentTypeName:
                    typeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName;
                    break;
                case VoronTypeName:
                    typeName = typeof(Raven.Storage.Voron.TransactionalStorage).AssemblyQualifiedName;
                    break;
                default:
                    throw new ArgumentException("Invalid storage engine type name: " + typeName);
            }
            return typeName;
        }

        public string SelectDatabaseStorageEngineAndFetchTypeName()
        {
            if (RunInMemory)
            {
                if (!string.IsNullOrWhiteSpace(DefaultStorageTypeName) &&
                    DefaultStorageTypeName.Equals(EsentTypeName, StringComparison.InvariantCultureIgnoreCase))
                    return EsentTypeName;
                return VoronTypeName;
            }

            if (string.IsNullOrEmpty(DataDirectory) == false && Directory.Exists(DataDirectory))
            {
                if (File.Exists(Path.Combine(DataDirectory, Voron.Impl.Constants.DatabaseFilename)))
                    return VoronTypeName;

                if (File.Exists(Path.Combine(DataDirectory, "Data")))
                    return EsentTypeName;
            }

            if (string.IsNullOrEmpty(DefaultStorageTypeName))
                return EsentTypeName;

            return DefaultStorageTypeName;
        }

        public void Dispose()
        {
            if (containerExternallySet)
                return;
            if (container == null)
                return;

            container.Dispose();
            container = null;
        }

        private ExtensionsLog GetExtensionsFor(Type type)
        {
            var enumerable =
                Container.GetExports(new ImportDefinition(x => true, type.FullName, ImportCardinality.ZeroOrMore, false, false)).
                    ToArray();
            if (enumerable.Length == 0)
                return null;
            return new ExtensionsLog
            {
                Name = type.Name,
                Installed = enumerable.Select(export => new ExtensionsLogDetail
                {
                    Assembly = export.Value.GetType().Assembly.GetName().Name,
                    Name = export.Value.GetType().Name
                }).ToArray()
            };
        }

        public IEnumerable<ExtensionsLog> ReportExtensions(params Type[] types)
        {
            return types.Select(GetExtensionsFor).Where(extensionsLog => extensionsLog != null);
        }

        public void CustomizeValuesForDatabaseTenant(string tenantId)
        {
            if (string.IsNullOrEmpty(Settings[Constants.RavenIndexPath]) == false)
                Settings[Constants.RavenIndexPath] = Path.Combine(Settings[Constants.RavenIndexPath], "Databases", tenantId);

            if (string.IsNullOrEmpty(Settings[Constants.RavenEsentLogsPath]) == false)
                Settings[Constants.RavenEsentLogsPath] = Path.Combine(Settings[Constants.RavenEsentLogsPath], "Databases", tenantId);

            if (string.IsNullOrEmpty(Settings[Constants.RavenTxJournalPath]) == false)
                Settings[Constants.RavenTxJournalPath] = Path.Combine(Settings[Constants.RavenTxJournalPath], "Databases", tenantId);

            if (string.IsNullOrEmpty(Settings[Constants.Voron.TempPath]) == false)
                Settings[Constants.Voron.TempPath] = Path.Combine(Settings[Constants.Voron.TempPath], "Databases", tenantId, "VoronTemp");
        }

        public void CustomizeValuesForFileSystemTenant(string tenantId)
        {
            if (string.IsNullOrEmpty(Settings[Constants.FileSystem.DataDirectory]) == false)
                Settings[Constants.FileSystem.DataDirectory] = Path.Combine(Settings[Constants.FileSystem.DataDirectory], "FileSystems", tenantId);

            if (string.IsNullOrEmpty(Settings[Constants.RavenIndexPath]) == false)
                Settings[Constants.RavenIndexPath] = Path.Combine(Settings[Constants.RavenIndexPath], "FileSystems", tenantId);

            if (string.IsNullOrEmpty(Settings[Constants.RavenEsentLogsPath]) == false)
                Settings[Constants.RavenEsentLogsPath] = Path.Combine(Settings[Constants.RavenEsentLogsPath], "FileSystems", tenantId);

            if (string.IsNullOrEmpty(Settings[Constants.RavenTxJournalPath]) == false)
                Settings[Constants.RavenTxJournalPath] = Path.Combine(Settings[Constants.RavenTxJournalPath], "FileSystems", tenantId);

            if (string.IsNullOrEmpty(Settings[Constants.Voron.TempPath]) == false)
                Settings[Constants.Voron.TempPath] = Path.Combine(Settings[Constants.Voron.TempPath], "FileSystems", tenantId, "VoronTemp");
        }

        public void CustomizeValuesForCounterStorageTenant(string tenantId)
        {
            if (string.IsNullOrEmpty(Settings[Constants.Counter.DataDirectory]) == false)
                Settings[Constants.Counter.DataDirectory] = Path.Combine(Settings[Constants.Counter.DataDirectory], "Counters", tenantId);
        }

        public void CustomizeValuesForTimeSeriesTenant(string tenantId)
        {
            if (string.IsNullOrEmpty(Settings[Constants.TimeSeries.DataDirectory]) == false)
                Settings[Constants.TimeSeries.DataDirectory] = Path.Combine(Settings[Constants.TimeSeries.DataDirectory], "TimeSeries", tenantId);
        }

        public void CopyParentSettings(InMemoryRavenConfiguration defaultConfiguration)
        {
            Port = defaultConfiguration.Port;
            OAuthTokenKey = defaultConfiguration.OAuthTokenKey;
            OAuthTokenServer = defaultConfiguration.OAuthTokenServer;
            Replication.ReplicationPropagationDelayInSeconds = defaultConfiguration.Replication.ReplicationPropagationDelayInSeconds;
            FileSystem.MaximumSynchronizationInterval = defaultConfiguration.FileSystem.MaximumSynchronizationInterval;

            Encryption.UseSsl = defaultConfiguration.Encryption.UseSsl;
            Encryption.UseFips = defaultConfiguration.Encryption.UseFips;

            AssembliesDirectory = defaultConfiguration.AssembliesDirectory;
            Storage.Voron.AllowOn32Bits = defaultConfiguration.Storage.Voron.AllowOn32Bits;
            Storage.SkipConsistencyCheck = defaultConfiguration.Storage.SkipConsistencyCheck;
        }

        public IEnumerable<string> GetConfigOptionsDocs()
        {
            return ConfigOptionDocs.OptionsDocs;
        }

        public class StorageConfiguration
        {
            public StorageConfiguration()
            {
                Voron = new VoronConfiguration();
                Esent = new EsentConfiguration();
            }
            public bool PreventSchemaUpdate { get; set; }

            public bool SkipConsistencyCheck { get; set; }
            
            public TimeSpan PutSerialLockDuration { get; set; }

            public VoronConfiguration Voron { get; private set; }

            public EsentConfiguration Esent { get; private set; }

            public class EsentConfiguration
            {
                public string JournalsStoragePath { get; set; }
                public int CacheSizeMax { get; set; }
                public int MaxVerPages { get; set; }
                public int PreferredVerPages { get; set; }
                public int DbExtensionSize { get; set; }
                public int LogFileSize { get; set; }
                public int LogBuffers { get; set; }
                public int MaxCursors { get; set; }
                public bool CircularLog { get; set; }
                public int? MaxSessions { get; set; }
                public int? CheckpointDepthMax { get; set; }
                public int MaxInstances { get; set; }
            }

            public class VoronConfiguration
            {
                /// <summary>
                /// You can use this setting to specify a maximum buffer pool size that can be used for transactional storage (in gigabytes). 
                /// By default it is 4.
                /// Minimum value is 2.
                /// </summary>
                public int MaxBufferPoolSize { get; set; }

                /// <summary>
                /// You can use this setting to specify an initial file size for data file (in bytes).
                /// </summary>
                public int? InitialFileSize { get; set; }

                /// <summary>
                /// The maximum scratch buffer size that can be used by Voron. The value is in megabytes. 
                /// Default: 6144.
                /// </summary>
                public int MaxScratchBufferSize { get; set; }

                /// <summary>
                /// The maximum per scratch buffer file size. The value is in megabytes. 
                /// Default: 256.
                /// </summary>
                public int MaxSizePerScratchBufferFile { get; set; }

                /// <summary>
                /// The minimum number of megabytes after which each scratch buffer size increase will create a notification. Used for indexing batch size tuning.
                /// Default: 
                /// 1024 when MaxScratchBufferSize > 1024, 
                /// 512 when MaxScratchBufferSize > 512
                /// -1 otherwise (disabled) 
                /// </summary>
                public int ScratchBufferSizeNotificationThreshold { get; set; }

                /// <summary>
                /// If you want to use incremental backups, you need to turn this to true, but then journal files will not be deleted after applying them to the data file. They will be deleted only after a successful backup. 
                /// Default: false.
                /// </summary>
                public bool AllowIncrementalBackups { get; set; }

                /// <summary>
                /// You can use this setting to specify a different path to temporary files. By default it is empty, which means that temporary files will be created at same location as data file.
                /// </summary>
                public string TempPath { get; set; }

                public string JournalsStoragePath { get; set; }

                /// <summary>
                /// Whether to allow Voron to run in 32 bits process.
                /// </summary>
                public bool AllowOn32Bits { get; set; }
            }
        }

        public class PrefetcherConfiguration
        {
            /// <summary>
            /// Number of seconds after which prefetcher will stop reading documents from disk. Default: 5.
            /// </summary>
            public int FetchingDocumentsFromDiskTimeoutInSeconds { get; set; }

            /// <summary>
            /// Maximum number of megabytes after which prefetcher will stop reading documents from disk. Default: 256.
            /// </summary>
            public int MaximumSizeAllowedToFetchFromStorageInMb { get; set; }
        }

        public class ReplicationConfiguration
        {
            /// <summary>
            /// Number of seconds after which replication will stop reading documents/attachments from disk. Default: 30.
            /// </summary>
            public int FetchingFromDiskTimeoutInSeconds { get; set; }

            /// <summary>
            /// Number of milliseconds before replication requests will timeout. Default: 60 * 1000.
            /// </summary>
            public int ReplicationRequestTimeoutInMilliseconds { get; set; }

            /// <summary>
            /// Force us to buffer replication requests (useful if using windows auth under certain scenarios).
            /// </summary>
            public bool ForceReplicationRequestBuffering { get; set; }

            /// <summary>
            /// Maximum number of items replication will receive in single batch. Min: 512. Default: null (let source server decide).
            /// </summary>
            public int? MaxNumberOfItemsToReceiveInSingleBatch { get; set; }

            /// <summary>
            /// Indicates how many seconds replication task will wait before propagating replication documents
            /// </summary>
            public int ReplicationPropagationDelayInSeconds { get; set; }

            public string CertificatePath { get; set; }

            public string CertificatePassword { get; set; }
        }

        public class SqlReplicationConfiguration
        {
            /// <summary>
            /// Number of seconds after which SQL command will timeout. Default: -1 (use provider default). Can be overriden by setting CommandTimeout property value in SQL Replication configuration.
            /// </summary>
            public int CommandTimeoutInSec { get; set; }
        }

        public class FileSystemConfiguration
        {
            public void InitializeFrom(InMemoryRavenConfiguration configuration)
            {
                workingDirectory = configuration.WorkingDirectory;
                defaultSystemStorageTypeName = configuration.DefaultStorageTypeName;
                runInMemory = configuration.RunInMemory;
            }

            private string fileSystemDataDirectory;

            private string fileSystemIndexStoragePath;

            private string defaultFileSystemStorageTypeName;

            private string workingDirectory;

            private string defaultSystemStorageTypeName;

            private bool runInMemory;

            public TimeSpan MaximumSynchronizationInterval { get; set; }

            /// <summary>
            /// The directory for the RavenDB file system. 
            /// You can use the ~\ prefix to refer to RavenDB's base directory. 
            /// </summary>
            public string DataDirectory
            {
                get { return fileSystemDataDirectory; }
                set { fileSystemDataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(workingDirectory, value); }
            }

            public string IndexStoragePath
            {
                get
                {
                    if (string.IsNullOrEmpty(fileSystemIndexStoragePath))
                        fileSystemIndexStoragePath = Path.Combine(DataDirectory, "Indexes");
                    return fileSystemIndexStoragePath;
                }
                set { fileSystemIndexStoragePath = value.ToFullPath(); }
            }

            /// <summary>
            /// What storage type to use in RavenFS (see: RavenFS Storage engines)
            /// Allowed values: esent, voron
            /// Default: esent
            /// </summary>
            public string DefaultStorageTypeName
            {
                get { return defaultFileSystemStorageTypeName; }
                set { if (!string.IsNullOrEmpty(value)) defaultFileSystemStorageTypeName = value; }
            }

            public string SelectFileSystemStorageEngineAndFetchTypeName()
            {
                if (runInMemory)
                {
                    if (!string.IsNullOrWhiteSpace(DefaultStorageTypeName) &&
                        DefaultStorageTypeName.Equals(EsentTypeName, StringComparison.InvariantCultureIgnoreCase))
                        return EsentTypeName;
                    return VoronTypeName;
                }

                if (string.IsNullOrEmpty(DataDirectory) == false && Directory.Exists(DataDirectory))
                {
                    if (File.Exists(Path.Combine(DataDirectory, Voron.Impl.Constants.DatabaseFilename)))
                        return VoronTypeName;

                    if (File.Exists(Path.Combine(DataDirectory, "Data.ravenfs")))
                        return EsentTypeName;
                }

                if (string.IsNullOrEmpty(DefaultStorageTypeName) == false)
                    return DefaultStorageTypeName; // We select the most specific

                if (string.IsNullOrEmpty(defaultSystemStorageTypeName) == false)
                    return defaultSystemStorageTypeName; // We choose the system wide if not defined

                return EsentTypeName; // We choose esent by default
            }


            public bool DisableRDC { get; set; }

            public bool SynchronizationBatchProcessing { get; set; }
        }

        public class StudioConfiguration
        {
            public void InitializeFrom(InMemoryRavenConfiguration configuration)
            {
                AllowNonAdminUsersToSetupPeriodicExport = configuration.Studio.AllowNonAdminUsersToSetupPeriodicExport;
            }

            public bool AllowNonAdminUsersToSetupPeriodicExport { get; set; }
        }

        public class CounterConfiguration
        {
            public void InitializeFrom(InMemoryRavenConfiguration configuration)
            {
                workingDirectory = configuration.WorkingDirectory;
            }

            private string workingDirectory;

            private string countersDataDirectory;

            /// <summary>
            /// The directory for the RavenDB counters. 
            /// You can use the ~\ prefix to refer to RavenDB's base directory. 
            /// </summary>
            public string DataDirectory
            {
                get { return countersDataDirectory; }
                set { countersDataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(workingDirectory, value); }
            }

            /// <summary>
            /// Determines how long tombstones will be kept by a counter storage. After the specified time they will be automatically
            /// Purged on next counter storage startup. Default: 14 days.
            /// </summary>
            public TimeSpan TombstoneRetentionTime { get; set; }

            public int DeletedTombstonesInBatch { get; set; }

            public int ReplicationLatencyInMs { get; set; }

            public TimeSpan BatchTimeout { get; set; }
        }

        public class TimeSeriesConfiguration
        {
            public void InitializeFrom(InMemoryRavenConfiguration configuration)
            {
                workingDirectory = configuration.WorkingDirectory;
            }

            private string workingDirectory;

            private string timeSeriesDataDirectory;

            /// <summary>
            /// The directory for the RavenDB time series. 
            /// You can use the ~\ prefix to refer to RavenDB's base directory. 
            /// </summary>
            public string DataDirectory
            {
                get { return timeSeriesDataDirectory; }
                set { timeSeriesDataDirectory = value == null ? null : FilePathTools.ApplyWorkingDirectoryToPathAndMakeSureThatItEndsWithSlash(workingDirectory, value); }
            }

            /// <summary>
            /// Determines how long tombstones will be kept by a time series. After the specified time they will be automatically
            /// Purged on next time series startup. Default: 14 days.
            /// </summary>
            public TimeSpan TombstoneRetentionTime { get; set; }

            public int DeletedTombstonesInBatch { get; set; }

            public int ReplicationLatencyInMs { get; set; }
        }

        public class EncryptionConfiguration
        {
            /// <summary>
            /// Whatever we should use FIPS compliant encryption algorithms
            /// </summary>
            public bool UseFips { get; set; }

            public int EncryptionKeyBitsPreference { get; set; }

            /// <summary>
            /// Whatever we should use SSL for this connection
            /// </summary>
            public bool UseSsl { get; set; }
        }

        public class IndexingConfiguration
        {
            public int MaxNumberOfItemsToProcessInTestIndexes { get; set; }

            public int DisableIndexingFreeSpaceThreshold { get; set; }

            public bool DisableMapReduceInMemoryTracking { get; set; }
            public int MaxNumberOfStoredIndexingBatchInfoElements { get; set; }
            public bool UseLuceneASTParser
            {
                get { return useLuceneASTParser; }
                set
                {
                    if (value == useLuceneASTParser)
                        return;
                    QueryBuilder.UseLuceneASTParser = useLuceneASTParser = value;
                }
            }
            private bool useLuceneASTParser = true;

            public bool SkipRecoveryOnStartup { get; set; }
        }

        public class ClusterConfiguration
        {
            public int ElectionTimeout { get; set; }
            public int HeartbeatTimeout { get; set; }
            public int MaxLogLengthBeforeCompaction { get; set; }
            public TimeSpan MaxStepDownDrainTime { get; set; }
            public int MaxEntriesPerRequest { get; set; }
            public TimeSpan MaxReplicationLatency { get; set; }
        }

        public class MonitoringConfiguration
        {
            public MonitoringConfiguration()
            {
                Snmp = new SnmpConfiguration();
            }

            public SnmpConfiguration Snmp { get; private set; }

            public class SnmpConfiguration
            {
                public bool Enabled { get; set; }

                public int Port { get; set; }

                public string Community { get; set; }
            }
        }

        public class WebSocketsConfiguration
        {
            public int InitialBufferPoolSize { get; set; }
        }

        public void UpdateDataDirForLegacySystemDb()
        {
            if (RunInMemory)
                return;
            var legacyPath = Settings["Raven/DataDir/Legacy"];
            if (string.IsNullOrEmpty(legacyPath))
                return;
            var fullLegacyPath = FilePathTools.MakeSureEndsWithSlash(legacyPath.ToFullPath());

            // if we already have a system database in the legacy path, we want to keep it.
            // The idea is that we don't want to have the user experience "missing databases" because
            // we change the path to make it nicer.
            if (Directory.Exists(fullLegacyPath))
            {
                DataDirectory = legacyPath;
            }
        }
    }
}

