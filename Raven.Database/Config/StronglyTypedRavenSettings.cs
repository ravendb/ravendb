// -----------------------------------------------------------------------
//  <copyright file="StronglyTypedRavenSettings.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Rachis;
using Raven.Abstractions.Data;
using Raven.Database.Config.Settings;
using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Runtime.Caching;
using System.Threading;

namespace Raven.Database.Config
{
    internal class StronglyTypedRavenSettings
    {
        private readonly NameValueCollection settings;

        public ReplicationConfiguration Replication { get; private set; }

        public SqlReplicationConfiguration SqlReplication { get; private set; }

        public VoronConfiguration Voron { get; private set; }

        public EsentConfiguration Esent { get; private set; }

        public PrefetcherConfiguration Prefetcher { get; private set; }

        public FileSystemConfiguration FileSystem { get; private set; }

        public CounterConfiguration Counter { get; private set; }

        public TimeSeriesConfiguration TimeSeries { get; private set; }

        public EncryptionConfiguration Encryption { get; private set; }

        public IndexingConfiguration Indexing { get; set; }

        public ClusterConfiguration Cluster { get; private set; }

        public WebSocketsConfiguration WebSockets { get; private set; }

        public MonitoringConfiguration Monitoring { get; private set; }

        public StudioConfiguration Studio { get; private set; }

        public StronglyTypedRavenSettings(NameValueCollection settings)
        {
            Replication = new ReplicationConfiguration();
            SqlReplication = new SqlReplicationConfiguration();
            Voron = new VoronConfiguration();
            Esent = new EsentConfiguration();
            Prefetcher = new PrefetcherConfiguration();
            FileSystem = new FileSystemConfiguration();
            Counter = new CounterConfiguration();
            TimeSeries = new TimeSeriesConfiguration();
            Encryption = new EncryptionConfiguration();
            Indexing = new IndexingConfiguration();
            WebSockets = new WebSocketsConfiguration();
            Cluster = new ClusterConfiguration();
            Monitoring = new MonitoringConfiguration();
            Studio = new StudioConfiguration();

            this.settings = settings;
        }

        public void Setup(int defaultMaxNumberOfItemsToIndexInSingleBatch, int defaultInitialNumberOfItemsToIndexInSingleBatch)
        {
            const int defaultPrecomputedBatchSize = 32 * 1024;
            MaxPrecomputedBatchSizeForNewIndex = new IntegerSetting(settings["Raven/MaxPrecomputedBatchSizeForNewIndex"], defaultPrecomputedBatchSize);

            const int defaultPrecomputedBatchTotalDocumentSizeInBytes = 1024 * 1024 * 250;  //250 mb
            MaxPrecomputedBatchTotalDocumentSizeInBytes = new IntegerSetting(settings["Raven/MaxPrecomputedBatchTotalDocumentSizeInBytes"], defaultPrecomputedBatchTotalDocumentSizeInBytes);

            //1024 is Lucene.net default - so if the setting is not set it will be the same as not touching Lucene's settings at all
            MaxClauseCount = new IntegerSetting(settings[Constants.MaxClauseCount], 1024);

            AllowScriptsToAdjustNumberOfSteps = new BooleanSetting(settings[Constants.AllowScriptsToAdjustNumberOfSteps], false);

            IndexAndTransformerReplicationLatencyInSec = new IntegerSetting(settings[Constants.RavenIndexAndTransformerReplicationLatencyInSec], Constants.DefaultRavenIndexAndTransformerReplicationLatencyInSec);

            PrefetchingDurationLimit = new IntegerSetting(settings[Constants.RavenPrefetchingDurationLimit], Constants.DefaultPrefetchingDurationLimit);

            BulkImportBatchTimeout = new TimeSpanSetting(settings[Constants.BulkImportBatchTimeout], TimeSpan.FromMilliseconds(Constants.BulkImportDefaultTimeoutInMs), TimeSpanArgumentType.FromParse);

            MaxConcurrentServerRequests = new IntegerSetting(settings[Constants.MaxConcurrentServerRequests], 512);

            MaxConcurrentRequestsForDatabaseDuringLoad = new IntegerSetting(settings[Constants.MaxConcurrentRequestsForDatabaseDuringLoad], 50);

            MaxSecondsForTaskToWaitForDatabaseToLoad = new IntegerSetting(settings[Constants.MaxSecondsForTaskToWaitForDatabaseToLoad], 5);

            MaxConcurrentMultiGetRequests = new IntegerSetting(settings[Constants.MaxConcurrentMultiGetRequests], 192);

            MemoryLimitForProcessing = new IntegerSetting(settings[Constants.MemoryLimitForProcessing] ?? settings[Constants.MemoryLimitForProcessing_BackwardCompatibility],
                // we allow 1 GB by default, or up to 75% of available memory on startup, if less than that is available
                Math.Min(1024, (int)(MemoryStatistics.AvailableMemoryInMb * 0.75)));

            int workerThreads;
            int completionThreads;
            ThreadPool.GetMinThreads(out workerThreads, out completionThreads);
            MinThreadPoolWorkerThreads =
                new IntegerSettingWithMin(settings["Raven/MinThreadPoolWorkerThreads"], workerThreads, 2);
            MinThreadPoolCompletionThreads =
                new IntegerSettingWithMin(settings["Raven/MinThreadPoolCompletionThreads"], completionThreads, 2);

            LowMemoryLimitForLinuxDetectionInMB =
                new IntegerSetting(settings[Constants.LowMemoryLimitForLinuxDetectionInMB],
                    Math.Min(16, (int)(MemoryStatistics.AvailableMemoryInMb * 0.10))); // AvailableMemory reports in MB
            MaxPageSize =
                new IntegerSettingWithMin(settings["Raven/MaxPageSize"], 1024, 10);

            MemoryCacheLimitMegabytes =
                new IntegerSetting(settings["Raven/MemoryCacheLimitMegabytes"], GetDefaultMemoryCacheLimitMegabytes);
            MemoryCacheExpiration =
                new TimeSpanSetting(settings["Raven/MemoryCacheExpiration"], TimeSpan.FromMinutes(60),
                                    TimeSpanArgumentType.FromSeconds);
            MemoryCacheLimitPercentage =
                new IntegerSetting(settings["Raven/MemoryCacheLimitPercentage"], 0 /* auto size */);
            MemoryCacheLimitCheckInterval =
                new TimeSpanSetting(settings["Raven/MemoryCacheLimitCheckInterval"], MemoryCache.Default.PollingInterval,
                                    TimeSpanArgumentType.FromParse);

            MemoryCacher = new StringSetting(settings["Raven/MemoryCacher"], (string)null);

            PrewarmFacetsSyncronousWaitTime =
                new TimeSpanSetting(settings["Raven/PrewarmFacetsSyncronousWaitTime"], TimeSpan.FromSeconds(3),
                                    TimeSpanArgumentType.FromParse);

            PrewarmFacetsOnIndexingMaxAge =
                new TimeSpanSetting(settings["Raven/PrewarmFacetsOnIndexingMaxAge"], TimeSpan.FromMinutes(10),
                                    TimeSpanArgumentType.FromParse);

            MaxProcessingRunLatency =
                new TimeSpanSetting(settings["Raven/MaxProcessingRunLatency"] ?? settings["Raven/MaxIndexingRunLatency"], TimeSpan.FromMinutes(5),
                                    TimeSpanArgumentType.FromParse);
            MaxIndexWritesBeforeRecreate =
                new IntegerSetting(settings["Raven/MaxIndexWritesBeforeRecreate"], 256 * 1024);
            MaxSimpleIndexOutputsPerDocument =
                new IntegerSetting(settings["Raven/MaxSimpleIndexOutputsPerDocument"], 15);

            MaxMapReduceIndexOutputsPerDocument =
                new IntegerSetting(settings["Raven/MaxMapReduceIndexOutputsPerDocument"], 50);

            MaxNumberOfItemsToProcessInSingleBatch =
                new IntegerSettingWithMin(settings["Raven/MaxNumberOfItemsToProcessInSingleBatch"] ?? settings["Raven/MaxNumberOfItemsToIndexInSingleBatch"],
                                          defaultMaxNumberOfItemsToIndexInSingleBatch, 128);
            AvailableMemoryForRaisingBatchSizeLimit =
                new IntegerSetting(settings["Raven/AvailableMemoryForRaisingBatchSizeLimit"] ?? settings["Raven/AvailableMemoryForRaisingIndexBatchSizeLimit"],
                                   Math.Min(768, MemoryStatistics.TotalPhysicalMemory / 2));
            MaxNumberOfItemsToReduceInSingleBatch =
                new IntegerSettingWithMin(settings["Raven/MaxNumberOfItemsToReduceInSingleBatch"],
                                          defaultMaxNumberOfItemsToIndexInSingleBatch / 2, 128);
            NumberOfItemsToExecuteReduceInSingleStep =
                new IntegerSetting(settings["Raven/NumberOfItemsToExecuteReduceInSingleStep"], 1024);
            MaxNumberOfParallelProcessingTasks =
                new IntegerSettingWithMin(settings["Raven/MaxNumberOfParallelProcessingTasks"] ?? settings["Raven/MaxNumberOfParallelIndexTasks"], Environment.ProcessorCount, 1);

            NewIndexInMemoryMaxTime =
                new TimeSpanSetting(settings["Raven/NewIndexInMemoryMaxTime"], TimeSpan.FromMinutes(15), TimeSpanArgumentType.FromParse);
            NewIndexInMemoryMaxMb =
                new MultipliedIntegerSetting(new IntegerSettingWithMin(settings["Raven/NewIndexInMemoryMaxMB"], 64, 1), 1024 * 1024);
            RunInMemory =
                new BooleanSetting(settings[Constants.RunInMemory], false);
            CreateAutoIndexesForAdHocQueriesIfNeeded =
                new BooleanSetting(settings["Raven/CreateAutoIndexesForAdHocQueriesIfNeeded"], true);
            ResetIndexOnUncleanShutdown =
                new BooleanSetting(settings["Raven/ResetIndexOnUncleanShutdown"], false);
            DisableInMemoryIndexing =
                new BooleanSetting(settings["Raven/DisableInMemoryIndexing"], false);
            WorkingDir =
                new StringSetting(settings["Raven/WorkingDir"], @"~\");
            DataDir =
                new StringSetting(settings["Raven/DataDir"], @"~\Databases\System");
            IndexStoragePath =
                new StringSetting(settings["Raven/IndexStoragePath"], (string)null);

            HostName =
                new StringSetting(settings["Raven/HostName"], (string)null);
            Port =
                new StringSetting(settings["Raven/Port"], "*");
            ExposeConfigOverTheWire =
                new StringSetting(settings[Constants.ExposeConfigOverTheWire], "Open");
            HttpCompression =
                new BooleanSetting(settings["Raven/HttpCompression"], true);
            AccessControlAllowOrigin =
                new StringSetting(settings["Raven/AccessControlAllowOrigin"], (string)null);
            AccessControlMaxAge =
                new StringSetting(settings["Raven/AccessControlMaxAge"], "1728000" /* 20 days */);
            AccessControlAllowMethods =
                new StringSetting(settings["Raven/AccessControlAllowMethods"], "PUT,PATCH,GET,DELETE,POST");
            AccessControlRequestHeaders =
                new StringSetting(settings["Raven/AccessControlRequestHeaders"], (string)null);
            RedirectStudioUrl =
                new StringSetting(settings["Raven/RedirectStudioUrl"], (string)null);
            DisableDocumentPreFetching =
                new BooleanSetting(settings["Raven/DisableDocumentPreFetching"] ?? settings["Raven/DisableDocumentPreFetchingForIndexing"], false);
            MaxNumberOfItemsToPreFetch =
                new IntegerSettingWithMin(settings["Raven/MaxNumberOfItemsToPreFetch"] ?? settings["Raven/MaxNumberOfItemsToPreFetchForIndexing"],
                                          defaultMaxNumberOfItemsToIndexInSingleBatch, 128);
            WebDir =
                new StringSetting(settings["Raven/WebDir"], GetDefaultWebDir);
            PluginsDirectory =
                new StringSetting(settings["Raven/PluginsDirectory"], @"~\Plugins");
            AssembliesDirectory =
                new StringSetting(settings["Raven/AssembliesDirectory"], @"~\Assemblies");
            EmbeddedFilesDirectory =
                new StringSetting(settings["Raven/EmbeddedFilesDirectory"], (string)null);
            CompiledIndexCacheDirectory =
                new StringSetting(settings["Raven/CompiledIndexCacheDirectory"], @"~\CompiledIndexCache");
            TaskScheduler =
                new StringSetting(settings["Raven/TaskScheduler"], (string)null);
            RejectClientsModeEnabled =
                new BooleanSetting(settings[Constants.RejectClientsModeEnabled], false);

            MaxIndexCommitPointStoreTimeInterval =
                new TimeSpanSetting(settings["Raven/MaxIndexCommitPointStoreTimeInterval"], TimeSpan.FromMinutes(5),
                                    TimeSpanArgumentType.FromParse);
            MaxNumberOfStoredCommitPoints =
                new IntegerSetting(settings["Raven/MaxNumberOfStoredCommitPoints"], 5);
            MinIndexingTimeIntervalToStoreCommitPoint =
                new TimeSpanSetting(settings["Raven/MinIndexingTimeIntervalToStoreCommitPoint"], TimeSpan.FromMinutes(1),
                                    TimeSpanArgumentType.FromParse);

            TimeToWaitBeforeRunningIdleIndexes = new TimeSpanSetting(settings["Raven/TimeToWaitBeforeRunningIdleIndexes"], TimeSpan.FromMinutes(10), TimeSpanArgumentType.FromParse);

            DatbaseOperationTimeout = new TimeSpanSetting(settings["Raven/DatabaseOperationTimeout"], TimeSpan.FromMinutes(5), TimeSpanArgumentType.FromParse);

            TimeToWaitBeforeMarkingAutoIndexAsIdle = new TimeSpanSetting(settings["Raven/TimeToWaitBeforeMarkingAutoIndexAsIdle"], TimeSpan.FromHours(1), TimeSpanArgumentType.FromParse);

            TimeToWaitBeforeMarkingIdleIndexAsAbandoned = new TimeSpanSetting(settings["Raven/TimeToWaitBeforeMarkingIdleIndexAsAbandoned"], TimeSpan.FromHours(72), TimeSpanArgumentType.FromParse);

            CheckReferenceBecauseOfDocumentUpdateTimeout = new TimeSpanSetting(settings["Raven/CheckReferenceBecauseOfDocumentUpdateTimeoutInSeconds"], TimeSpan.FromSeconds(30), TimeSpanArgumentType.FromParse);

            TimeToWaitBeforeRunningAbandonedIndexes = new TimeSpanSetting(settings["Raven/TimeToWaitBeforeRunningAbandonedIndexes"], TimeSpan.FromHours(3), TimeSpanArgumentType.FromParse);

            DisableClusterDiscovery = new BooleanSetting(settings["Raven/DisableClusterDiscovery"], false);

            TurnOffDiscoveryClient = new BooleanSetting(settings["Raven/TurnOffDiscoveryClient"], false);

            ServerName = new StringSetting(settings["Raven/ServerName"], (string)null);

            MaxStepsForScript = new IntegerSetting(settings["Raven/MaxStepsForScript"], 10 * 1000);
            AdditionalStepsForScriptBasedOnDocumentSize = new IntegerSetting(settings["Raven/AdditionalStepsForScriptBasedOnDocumentSize"], 5);

            SkipConsistencyCheck = new BooleanSetting(settings["Raven/Storage/SkipConsistencyCheck"], false);
            PutSerialLockDuration = new TimeSpanSetting(settings["Raven/Storage/PutSerialLockDurationInSeconds"], TimeSpan.FromMinutes(2), TimeSpanArgumentType.FromSeconds);

            Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds = new IntegerSetting(settings["Raven/Prefetcher/FetchingDocumentsFromDiskTimeout"], 5);
            Prefetcher.MaximumSizeAllowedToFetchFromStorageInMb = new IntegerSetting(settings["Raven/Prefetcher/MaximumSizeAllowedToFetchFromStorage"], 256);

            Voron.MaxBufferPoolSize = new IntegerSetting(settings[Constants.Voron.MaxBufferPoolSize], 4);
            Voron.InitialFileSize = new NullableIntegerSetting(settings[Constants.Voron.InitialFileSize], (int?)null);
            Voron.MaxScratchBufferSize = new IntegerSetting(settings[Constants.Voron.MaxScratchBufferSize], 6144);
            Voron.MaxSizePerScratchBufferFile = new IntegerSetting(settings[Constants.Voron.MaxSizePerScratchBufferFile], 256);

            var maxScratchBufferSize = Voron.MaxScratchBufferSize.Value;
            var scratchBufferSizeNotificationThreshold = -1;
            if (maxScratchBufferSize > 1024)
                scratchBufferSizeNotificationThreshold = 1024;
            else if (maxScratchBufferSize > 512)
                scratchBufferSizeNotificationThreshold = 512;
            Voron.ScratchBufferSizeNotificationThreshold = new IntegerSetting(settings[Constants.Voron.ScratchBufferSizeNotificationThreshold], scratchBufferSizeNotificationThreshold);

            Voron.AllowIncrementalBackups = new BooleanSetting(settings[Constants.Voron.AllowIncrementalBackups], false);
            Voron.AllowOn32Bits = new BooleanSetting(settings[Constants.Voron.AllowOn32Bits], false);
            Voron.TempPath = new StringSetting(settings[Constants.Voron.TempPath], (string)null);

            var txJournalPath = settings[Constants.RavenTxJournalPath];
            var esentLogsPath = settings[Constants.RavenEsentLogsPath];

            Voron.JournalsStoragePath = new StringSetting(string.IsNullOrEmpty(txJournalPath) ? esentLogsPath : txJournalPath, (string)null);

            Esent.JournalsStoragePath = new StringSetting(string.IsNullOrEmpty(esentLogsPath) ? txJournalPath : esentLogsPath, (string)null);

            var defaultCacheSize = Environment.Is64BitProcess ? Math.Min(1024, (MemoryStatistics.TotalPhysicalMemory / 4)) : 256;
            Esent.CacheSizeMax = new IntegerSetting(settings[Constants.Esent.CacheSizeMax], defaultCacheSize);
            Esent.MaxVerPages = new IntegerSetting(settings[Constants.Esent.MaxVerPages], 512);
            Esent.PreferredVerPages = new IntegerSetting(settings[Constants.Esent.PreferredVerPages], 472);
            Esent.DbExtensionSize = new IntegerSetting(settings[Constants.Esent.DbExtensionSize], 8);
            Esent.LogFileSize = new IntegerSetting(settings[Constants.Esent.LogFileSize], 64);
            Esent.LogBuffers = new IntegerSetting(settings[Constants.Esent.LogBuffers], 8192);
            Esent.MaxCursors = new IntegerSetting(settings[Constants.Esent.MaxCursors], 2048);
            Esent.CircularLog = new BooleanSetting(settings[Constants.Esent.CircularLog], true);
            Esent.MaxSessions = new NullableIntegerSetting(settings[Constants.Esent.MaxSessions], (int?)null);
            Esent.CheckpointDepthMax = new NullableIntegerSetting(settings[Constants.Esent.CheckpointDepthMax], (int?)null);
            Esent.MaxInstances = new IntegerSetting(settings[Constants.Esent.MaxInstances], 1024);

            Replication.FetchingFromDiskTimeoutInSeconds = new IntegerSetting(settings["Raven/Replication/FetchingFromDiskTimeout"], 30);
            Replication.ReplicationRequestTimeoutInMilliseconds = new IntegerSetting(settings["Raven/Replication/ReplicationRequestTimeout"], 60 * 1000);
            Replication.ForceReplicationRequestBuffering = new BooleanSetting(settings["Raven/Replication/ForceReplicationRequestBuffering"], false);
            Replication.MaxNumberOfItemsToReceiveInSingleBatch = new NullableIntegerSettingWithMin(settings["Raven/Replication/MaxNumberOfItemsToReceiveInSingleBatch"], (int?)null, 512);
            Replication.ReplicationPropagationDelayInSeconds = new IntegerSetting(settings[Constants.ReplicationPropagationDelayInSeconds], 15);
            Replication.CertificatePath = new StringSetting(settings["Raven/Replication/CertificatePath"], (string)null);
            Replication.CertificatePassword = new StringSetting(settings["Raven/Replication/CertificatePassword"], (string)null);

            SqlReplication.CommandTimeoutInSec = new IntegerSetting(settings["Raven/SqlReplication/CommandTimeoutInSec"], -1);

            FileSystem.MaximumSynchronizationInterval = new TimeSpanSetting(settings[Constants.FileSystem.MaximumSynchronizationInterval], TimeSpan.FromSeconds(60), TimeSpanArgumentType.FromParse);
            FileSystem.IndexStoragePath = new StringSetting(settings[Constants.FileSystem.IndexStorageDirectory], string.Empty);
            FileSystem.DataDir = new StringSetting(settings[Constants.FileSystem.DataDirectory], @"~\FileSystems");
            FileSystem.DefaultStorageTypeName = new StringSetting(settings[Constants.FileSystem.Storage], string.Empty);
            FileSystem.PreventSchemaUpdate = new BooleanSetting(settings[Constants.FileSystem.PreventSchemaUpdate], false);
            FileSystem.DisableRDC = new BooleanSetting(settings[Constants.FileSystem.DisableRDC], false);
            FileSystem.SynchronizationBatchProcessing = new BooleanSetting(settings[Constants.FileSystem.SynchronizationBatchProcessing], false);

            Studio.AllowNonAdminUsersToSetupPeriodicExport = new BooleanSetting(settings[Constants.AllowNonAdminUsersToSetupPeriodicExport], false);

            Counter.DataDir = new StringSetting(settings[Constants.Counter.DataDirectory], @"~\Counters");
            Counter.TombstoneRetentionTime = new TimeSpanSetting(settings[Constants.Counter.TombstoneRetentionTime], TimeSpan.FromDays(14), TimeSpanArgumentType.FromParse);
            Counter.DeletedTombstonesInBatch = new IntegerSetting(settings[Constants.Counter.DeletedTombstonesInBatch], 1000);
            Counter.ReplicationLatencyInMs = new IntegerSetting(settings[Constants.Counter.ReplicationLatencyMs], 30 * 1000);
            Counter.BatchTimeout = new TimeSpanSetting(settings[Constants.Counter.BatchTimeout], TimeSpan.FromSeconds(360), TimeSpanArgumentType.FromParse);

            TimeSeries.DataDir = new StringSetting(settings[Constants.TimeSeries.DataDirectory], @"~\TimeSeries");
            TimeSeries.TombstoneRetentionTime = new TimeSpanSetting(settings[Constants.TimeSeries.TombstoneRetentionTime], TimeSpan.FromDays(14), TimeSpanArgumentType.FromParse);
            TimeSeries.DeletedTombstonesInBatch = new IntegerSetting(settings[Constants.TimeSeries.DeletedTombstonesInBatch], 1000);
            TimeSeries.ReplicationLatencyInMs = new IntegerSetting(settings[Constants.TimeSeries.ReplicationLatencyMs], 30 * 1000);

            Encryption.UseFips = new BooleanSetting(settings["Raven/Encryption/FIPS"], false);
            Encryption.EncryptionKeyBitsPreference = new IntegerSetting(settings[Constants.EncryptionKeyBitsPreferenceSetting], Constants.DefaultKeySizeToUseInActualEncryptionInBits);
            Encryption.UseSsl = new BooleanSetting(settings["Raven/UseSsl"], false);

            Indexing.MaxNumberOfItemsToProcessInTestIndexes = new IntegerSetting(settings[Constants.MaxNumberOfItemsToProcessInTestIndexes], 512);
            Indexing.DisableIndexingFreeSpaceThreshold = new IntegerSetting(settings[Constants.Indexing.DisableIndexingFreeSpaceThreshold], 2048);
            Indexing.DisableMapReduceInMemoryTracking = new BooleanSetting(settings[Constants.Indexing.DisableMapReduceInMemoryTracking], false);
            Indexing.MaxNumberOfStoredIndexingBatchInfoElements = new IntegerSetting(settings[Constants.MaxNumberOfStoredIndexingBatchInfoElements], 512);
            Indexing.UseLuceneASTParser = new BooleanSetting(settings[Constants.UseLuceneASTParser], true);
            Indexing.SkipRecoveryOnStartup = new BooleanSetting(settings[Constants.Indexing.SkipRecoveryOnStartup], false);

            Cluster.ElectionTimeout = new IntegerSetting(settings["Raven/Cluster/ElectionTimeout"], RaftEngineOptions.DefaultElectionTimeout);
            Cluster.HeartbeatTimeout = new IntegerSetting(settings["Raven/Cluster/HeartbeatTimeout"], RaftEngineOptions.DefaultHeartbeatTimeout);
            Cluster.MaxLogLengthBeforeCompaction = new IntegerSetting(settings["Raven/Cluster/MaxLogLengthBeforeCompaction"], RaftEngineOptions.DefaultMaxLogLengthBeforeCompaction);
            Cluster.MaxEntriesPerRequest = new IntegerSetting(settings["Raven/Cluster/MaxEntriesPerRequest"], RaftEngineOptions.DefaultMaxEntiresPerRequest);
            Cluster.MaxStepDownDrainTime = new TimeSpanSetting(settings["Raven/Cluster/MaxStepDownDrainTime"], RaftEngineOptions.DefaultMaxStepDownDrainTime, TimeSpanArgumentType.FromParse);
            Cluster.MaxReplicationLatency = new TimeSpanSetting(settings["Raven/Cluster/MaxReplicationLatency"], TimeSpan.FromMinutes(2), TimeSpanArgumentType.FromParse);

            DefaultStorageTypeName = new StringSetting(settings["Raven/StorageTypeName"] ?? settings["Raven/StorageEngine"], string.Empty);

            FlushIndexToDiskSizeInMb = new IntegerSetting(settings["Raven/Indexing/FlushIndexToDiskSizeInMb"], 5);

            TombstoneRetentionTime = new TimeSpanSetting(settings["Raven/TombstoneRetentionTime"], TimeSpan.FromDays(14), TimeSpanArgumentType.FromParse);

            ImplicitFetchFieldsFromDocumentMode = new EnumSetting<ImplicitFetchFieldsMode>(settings["Raven/ImplicitFetchFieldsFromDocumentMode"], ImplicitFetchFieldsMode.Enabled);

            if (settings["Raven/MaxServicePointIdleTime"] != null)
                ServicePointManager.MaxServicePointIdleTime = Convert.ToInt32(settings["Raven/MaxServicePointIdleTime"]);

            WebSockets.InitialBufferPoolSize = new IntegerSetting(settings["Raven/WebSockets/InitialBufferPoolSize"], 128 * 1024);

            MaxConcurrentResourceLoads = new IntegerSetting(settings[Constants.RavenMaxConcurrentResourceLoads], 8);
            ConcurrentResourceLoadTimeout = new TimeSpanSetting(settings[Constants.ConcurrentResourceLoadTimeout],
                TimeSpan.FromSeconds(15),
                TimeSpanArgumentType.FromParse);

            CacheDocumentsInMemory = new BooleanSetting(settings["Raven/CacheDocumentsInMemory"], true);
            TempPath = new StringSetting(settings[Constants.TempPath], Path.GetTempPath());

            FillMonitoringSettings();
        }

        private void FillMonitoringSettings()
        {
            Monitoring.Snmp.Enabled = new BooleanSetting(settings[Constants.Monitoring.Snmp.Enabled], false);
            Monitoring.Snmp.Community = new StringSetting(settings[Constants.Monitoring.Snmp.Community], "ravendb");
            Monitoring.Snmp.Port = new IntegerSetting(settings[Constants.Monitoring.Snmp.Port], 161);
        }

        private string GetDefaultWebDir()
        {
            return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"Raven/WebUI");
        }

        private int GetDefaultMemoryCacheLimitMegabytes()
        {
            var cacheSizeMaxSetting = new IntegerSetting(settings[Constants.Esent.CacheSizeMax], 1024);

            // we need to leave ( a lot ) of room for other things as well, so we min the cache size
            var val = (MemoryStatistics.TotalPhysicalMemory / 2) -
                                    // reduce the unmanaged cache size from the default min
                                    cacheSizeMaxSetting.Value;

            if (val < 0)
                return 128; // if machine has less than 1024 MB, then only use 128 MB

            return val;
        }

        public IntegerSetting MaxPrecomputedBatchTotalDocumentSizeInBytes { get; private set; }

        public IntegerSetting MaxPrecomputedBatchSizeForNewIndex { get; private set; }

        public BooleanSetting CacheDocumentsInMemory { get; set; }

        public IntegerSetting MaxConcurrentResourceLoads { get; private set; }

        public TimeSpanSetting ConcurrentResourceLoadTimeout { get; private set; }

        public IntegerSetting MaxClauseCount { get; private set; }

        public BooleanSetting AllowScriptsToAdjustNumberOfSteps { get; private set; }

        public IntegerSetting IndexAndTransformerReplicationLatencyInSec { get; private set; }

        public IntegerSetting MemoryLimitForProcessing { get; private set; }

        public IntegerSettingWithMin MinThreadPoolWorkerThreads { get; private set; }

        public IntegerSettingWithMin MinThreadPoolCompletionThreads { get; private set; }

        public IntegerSetting LowMemoryLimitForLinuxDetectionInMB { get; private set; }
        public IntegerSetting MaxConcurrentServerRequests { get; private set; }

        public IntegerSetting MaxConcurrentRequestsForDatabaseDuringLoad { get; private set; }

        public IntegerSetting MaxSecondsForTaskToWaitForDatabaseToLoad { get; set; }

        public IntegerSetting MaxConcurrentMultiGetRequests { get; private set; }

        public IntegerSetting PrefetchingDurationLimit { get; private set; }

        public TimeSpanSetting BulkImportBatchTimeout { get; private set; }

        public IntegerSettingWithMin MaxPageSize { get; private set; }

        public IntegerSetting MemoryCacheLimitMegabytes { get; private set; }

        public TimeSpanSetting MemoryCacheExpiration { get; private set; }

        public IntegerSetting MemoryCacheLimitPercentage { get; private set; }

        public TimeSpanSetting MemoryCacheLimitCheckInterval { get; private set; }

        public StringSetting MemoryCacher { get; private set; }

        public TimeSpanSetting MaxProcessingRunLatency { get; private set; }

        public TimeSpanSetting PrewarmFacetsOnIndexingMaxAge { get; private set; }

        public TimeSpanSetting PrewarmFacetsSyncronousWaitTime { get; private set; }

        public IntegerSettingWithMin MaxNumberOfItemsToProcessInSingleBatch { get; private set; }

        public IntegerSetting AvailableMemoryForRaisingBatchSizeLimit { get; private set; }

        public IntegerSettingWithMin MaxNumberOfItemsToReduceInSingleBatch { get; private set; }

        public IntegerSetting NumberOfItemsToExecuteReduceInSingleStep { get; private set; }

        public IntegerSettingWithMin MaxNumberOfParallelProcessingTasks { get; private set; }

        public MultipliedIntegerSetting NewIndexInMemoryMaxMb { get; private set; }

        public TimeSpanSetting NewIndexInMemoryMaxTime { get; private set; }

        public BooleanSetting RunInMemory { get; private set; }

        public BooleanSetting CreateAutoIndexesForAdHocQueriesIfNeeded { get; private set; }

        public BooleanSetting ResetIndexOnUncleanShutdown { get; private set; }

        public BooleanSetting DisableInMemoryIndexing { get; private set; }

        public StringSetting WorkingDir { get; private set; }

        public StringSetting DataDir { get; private set; }

        public StringSetting IndexStoragePath { get; private set; }

        public StringSetting HostName { get; private set; }

        public StringSetting Port { get; private set; }

        public StringSetting ExposeConfigOverTheWire { get; set; }

        public BooleanSetting HttpCompression { get; private set; }

        public StringSetting AccessControlAllowOrigin { get; private set; }

        public StringSetting AccessControlMaxAge { get; private set; }

        public StringSetting AccessControlAllowMethods { get; private set; }

        public StringSetting AccessControlRequestHeaders { get; private set; }

        public StringSetting RedirectStudioUrl { get; private set; }

        public BooleanSetting DisableDocumentPreFetching { get; private set; }

        public IntegerSettingWithMin MaxNumberOfItemsToPreFetch { get; private set; }

        public StringSetting WebDir { get; private set; }

        public BooleanSetting DisableClusterDiscovery { get; private set; }
        public BooleanSetting TurnOffDiscoveryClient { get; private set; }

        public StringSetting ServerName { get; private set; }

        public StringSetting PluginsDirectory { get; private set; }

        public StringSetting CompiledIndexCacheDirectory { get; private set; }

        public StringSetting AssembliesDirectory { get; private set; }

        public StringSetting EmbeddedFilesDirectory { get; private set; }

        public StringSetting TaskScheduler { get; private set; }

        public BooleanSetting RejectClientsModeEnabled { get; private set; }

        public TimeSpanSetting MaxIndexCommitPointStoreTimeInterval { get; private set; }

        public TimeSpanSetting MinIndexingTimeIntervalToStoreCommitPoint { get; private set; }

        public IntegerSetting MaxNumberOfStoredCommitPoints { get; private set; }
        public TimeSpanSetting TimeToWaitBeforeRunningIdleIndexes { get; private set; }

        public TimeSpanSetting TimeToWaitBeforeMarkingAutoIndexAsIdle { get; private set; }

        public TimeSpanSetting TimeToWaitBeforeMarkingIdleIndexAsAbandoned { get; private set; }

        public TimeSpanSetting CheckReferenceBecauseOfDocumentUpdateTimeout { get; private set; }

        public TimeSpanSetting TimeToWaitBeforeRunningAbandonedIndexes { get; private set; }

        public IntegerSetting MaxStepsForScript { get; private set; }

        public IntegerSetting AdditionalStepsForScriptBasedOnDocumentSize { get; private set; }

        public IntegerSetting MaxIndexWritesBeforeRecreate { get; private set; }

        public IntegerSetting MaxSimpleIndexOutputsPerDocument { get; private set; }

        public IntegerSetting MaxMapReduceIndexOutputsPerDocument { get; private set; }

        public TimeSpanSetting DatbaseOperationTimeout { get; private set; }

        public StringSetting DefaultStorageTypeName { get; private set; }

        public IntegerSetting FlushIndexToDiskSizeInMb { get; set; }

        public TimeSpanSetting TombstoneRetentionTime { get; private set; }

        public EnumSetting<ImplicitFetchFieldsMode> ImplicitFetchFieldsFromDocumentMode { get; private set; }

        public StringSetting TempPath { get; private set; }

        public BooleanSetting SkipConsistencyCheck { get; set; }
        
        public TimeSpanSetting PutSerialLockDuration { get; set; }

        public class VoronConfiguration
        {
            public IntegerSetting MaxBufferPoolSize { get; set; }

            public NullableIntegerSetting InitialFileSize { get; set; }

            public IntegerSetting MaxScratchBufferSize { get; set; }

            public IntegerSetting MaxSizePerScratchBufferFile { get; set; }

            public IntegerSetting ScratchBufferSizeNotificationThreshold { get; set; }

            public BooleanSetting AllowIncrementalBackups { get; set; }

            public StringSetting TempPath { get; set; }

            public StringSetting JournalsStoragePath { get; set; }

            public BooleanSetting AllowOn32Bits { get; set; }
        }

        public class EsentConfiguration
        {
            public StringSetting JournalsStoragePath { get; set; }

            public IntegerSetting CacheSizeMax { get; set; }

            public IntegerSetting MaxVerPages { get; set; }

            public IntegerSetting PreferredVerPages { get; set; }

            public IntegerSetting DbExtensionSize { get; set; }

            public IntegerSetting LogFileSize { get; set; }

            public IntegerSetting LogBuffers { get; set; }

            public IntegerSetting MaxCursors { get; set; }

            public BooleanSetting CircularLog { get; set; }

            public NullableIntegerSetting MaxSessions { get; set; }

            public NullableIntegerSetting CheckpointDepthMax { get; set; }

            public IntegerSetting MaxInstances { get; set; }
        }

        public class IndexingConfiguration
        {
            public IntegerSetting MaxNumberOfItemsToProcessInTestIndexes { get; set; }

            public IntegerSetting DisableIndexingFreeSpaceThreshold { get; set; }
            public BooleanSetting DisableMapReduceInMemoryTracking { get; set; }
            public IntegerSetting MaxNumberOfStoredIndexingBatchInfoElements { get; set; }
            public BooleanSetting UseLuceneASTParser { get; set; }
            public BooleanSetting SkipRecoveryOnStartup { get; set; }
        }

        public class ClusterConfiguration
        {
            public IntegerSetting ElectionTimeout { get; set; }
            public IntegerSetting HeartbeatTimeout { get; set; }
            public IntegerSetting MaxLogLengthBeforeCompaction { get; set; }
            public TimeSpanSetting MaxStepDownDrainTime { get; set; }
            public IntegerSetting MaxEntriesPerRequest { get; set; }
            public TimeSpanSetting MaxReplicationLatency { get; set; }
        }

        public class PrefetcherConfiguration
        {
            public IntegerSetting FetchingDocumentsFromDiskTimeoutInSeconds { get; set; }

            public IntegerSetting MaximumSizeAllowedToFetchFromStorageInMb { get; set; }
        }

        public class ReplicationConfiguration
        {
            public IntegerSetting FetchingFromDiskTimeoutInSeconds { get; set; }

            public BooleanSetting ForceReplicationRequestBuffering { get; set; }

            public IntegerSetting ReplicationRequestTimeoutInMilliseconds { get; set; }

            public NullableIntegerSettingWithMin MaxNumberOfItemsToReceiveInSingleBatch { get; set; }

            public IntegerSetting ReplicationPropagationDelayInSeconds { get; set; }

            public StringSetting CertificatePath { get; set; }

            public StringSetting CertificatePassword { get; set; }
        }

        public class SqlReplicationConfiguration
        {
            public IntegerSetting CommandTimeoutInSec { get; set; }
        }

        public class StudioConfiguration
        {
            public BooleanSetting AllowNonAdminUsersToSetupPeriodicExport { get; set; }
        }

        public class FileSystemConfiguration
        {
            public TimeSpanSetting MaximumSynchronizationInterval { get; set; }

            public StringSetting DataDir { get; set; }

            public StringSetting IndexStoragePath { get; set; }

            public StringSetting DefaultStorageTypeName { get; set; }

            public BooleanSetting PreventSchemaUpdate { get; set; }

            public BooleanSetting DisableRDC { get; set; }

            public BooleanSetting SynchronizationBatchProcessing { get; set; }
        }

        public class CounterConfiguration
        {
            public StringSetting DataDir { get; set; }

            public TimeSpanSetting TombstoneRetentionTime { get; set; }

            public IntegerSetting DeletedTombstonesInBatch { get; set; }

            public IntegerSetting ReplicationLatencyInMs { get; set; }

            public TimeSpanSetting BatchTimeout { get; set; }
        }

        public class TimeSeriesConfiguration
        {
            public StringSetting DataDir { get; set; }

            public TimeSpanSetting TombstoneRetentionTime { get; set; }

            public IntegerSetting DeletedTombstonesInBatch { get; set; }

            public IntegerSetting ReplicationLatencyInMs { get; set; }
        }

        public class EncryptionConfiguration
        {
            public BooleanSetting UseFips { get; set; }

            public IntegerSetting EncryptionKeyBitsPreference { get; set; }

            public BooleanSetting UseSsl { get; set; }
        }

        public class WebSocketsConfiguration
        {
            public IntegerSetting InitialBufferPoolSize { get; set; }
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
                public BooleanSetting Enabled { get; set; }

                public IntegerSetting Port { get; set; }

                public StringSetting Community { get; set; }
            }
        }
    }

    public enum ImplicitFetchFieldsMode
    {
        Enabled,
        DoNothing,
        Exception
    }
}