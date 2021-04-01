#if !DNXCORE50
// -----------------------------------------------------------------------
//  <copyright file="ConfigurationTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Util;

using Xunit;
using Raven.Abstractions;

namespace Raven.Tests.Core.Configuration
{
    public class ConfigurationTests
    {
        private readonly HashSet<string> propertyPathsToIgnore = new HashSet<string>
                                                                 {
                                                                     "DatabaseName",
                                                                     "FileSystemName",
                                                                     "CounterStorageName",
                                                                     "TimeSeriesName",
                                                                     "Settings",
                                                                     "Container",
                                                                     "Catalog",
                                                                     "RunInUnreliableYetFastModeThatIsNotSuitableForProduction",
                                                                     "CreateAnalyzersDirectoryIfNotExisting",
                                                                     "CreatePluginsDirectoryIfNotExisting",
                                                                     "Port",
                                                                     "IndexingScheduler.LastAmountOfItemsToIndexToRemember",
                                                                     "IndexingScheduler.LastAmountOfItemsToReduceToRemember",
                                                                     "InitialNumberOfItemsToProcessInSingleBatch",
                                                                     "InitialNumberOfItemsToReduceInSingleBatch",
                                                                     "ActiveBundles",
                                                                     "CustomTaskScheduler",
                                                                     "HeadersToIgnore",
                                                                     "UseDefaultOAuthTokenServer",
                                                                     "OAuthTokenServer",
                                                                     "ServerUrl",
                                                                     "AccessControlAllowOrigin",
                                                                     "VirtualDirectory",
                                                                     "OAuthTokenKey"
                                                                 };

        [Fact]
        public void NotChangingWorkingDirectoryShouldNotImpactPaths()
        {
            var inMemoryConfiguration = new InMemoryRavenConfiguration();
            inMemoryConfiguration.Initialize();

            var basePath = FilePathTools.MakeSureEndsWithSlash(AppDomain.CurrentDomain.BaseDirectory.ToFullPath());
            var workingDirectory = inMemoryConfiguration.WorkingDirectory;

            Assert.Equal(basePath, workingDirectory);
            Assert.True(inMemoryConfiguration.AssembliesDirectory.StartsWith(basePath));
            Assert.True(inMemoryConfiguration.CompiledIndexCacheDirectory.StartsWith(basePath));
            Assert.True(inMemoryConfiguration.DataDirectory.StartsWith(basePath));
            Assert.True(inMemoryConfiguration.FileSystem.DataDirectory.StartsWith(basePath));
            Assert.True(inMemoryConfiguration.Counter.DataDirectory.StartsWith(basePath));
            Assert.True(inMemoryConfiguration.TimeSeries.DataDirectory.StartsWith(basePath));
        }

        [Fact]
        public void ChangingWorkingDirectoryShouldImpactPaths()
        {
            string WorkingDirectoryValue = "C:\\Raven\\";
            if (EnvironmentUtils.RunningOnPosix == true)
                WorkingDirectoryValue = Environment.GetEnvironmentVariable("HOME") + @"\";
            
            var inMemoryConfiguration = new InMemoryRavenConfiguration();
            inMemoryConfiguration.Settings["Raven/WorkingDir"] = WorkingDirectoryValue;
            inMemoryConfiguration.Initialize();

            var basePath = FilePathTools.MakeSureEndsWithSlash(AppDomain.CurrentDomain.BaseDirectory.ToFullPath());
            var workingDirectory = inMemoryConfiguration.WorkingDirectory;

            Assert.Equal(WorkingDirectoryValue, inMemoryConfiguration.WorkingDirectory);
            Assert.NotEqual(basePath, workingDirectory);
            Assert.True(inMemoryConfiguration.AssembliesDirectory.StartsWith(WorkingDirectoryValue));
            Assert.True(inMemoryConfiguration.CompiledIndexCacheDirectory.StartsWith(WorkingDirectoryValue));
            Assert.True(inMemoryConfiguration.DataDirectory.StartsWith(WorkingDirectoryValue));
            Assert.True(inMemoryConfiguration.FileSystem.DataDirectory.StartsWith(WorkingDirectoryValue));
            Assert.True(inMemoryConfiguration.Counter.DataDirectory.StartsWith(WorkingDirectoryValue));
            Assert.True(inMemoryConfiguration.TimeSeries.DataDirectory.StartsWith(WorkingDirectoryValue));
        }

        [Fact]
        public void ChangingWorkingDirectoryShouldImpactRelativePaths()
        {
            string WorkingDirectoryValue = "C:\\Raven\\";
            if (EnvironmentUtils.RunningOnPosix == true)
                WorkingDirectoryValue = Environment.GetEnvironmentVariable("HOME") + @"\";
            
            var inMemoryConfiguration = new InMemoryRavenConfiguration();
            inMemoryConfiguration.Settings["Raven/WorkingDir"] = WorkingDirectoryValue;
            inMemoryConfiguration.Settings["Raven/AssembliesDirectory"] = "./my-assemblies";
            inMemoryConfiguration.Settings[Constants.FileSystem.DataDirectory] = "my-files";
            inMemoryConfiguration.Initialize();

            var basePath = FilePathTools.MakeSureEndsWithSlash(AppDomain.CurrentDomain.BaseDirectory.ToFullPath());
            var workingDirectory = inMemoryConfiguration.WorkingDirectory;

            Assert.Equal(WorkingDirectoryValue, inMemoryConfiguration.WorkingDirectory);
            Assert.NotEqual(basePath, workingDirectory);
            Assert.True(inMemoryConfiguration.AssembliesDirectory.StartsWith(WorkingDirectoryValue));
            Assert.True(inMemoryConfiguration.CompiledIndexCacheDirectory.StartsWith(WorkingDirectoryValue));
            Assert.True(inMemoryConfiguration.DataDirectory.StartsWith(WorkingDirectoryValue));
            Assert.True(inMemoryConfiguration.FileSystem.DataDirectory.StartsWith(WorkingDirectoryValue));
            Assert.True(inMemoryConfiguration.Counter.DataDirectory.StartsWith(WorkingDirectoryValue));
            Assert.True(inMemoryConfiguration.TimeSeries.DataDirectory.StartsWith(WorkingDirectoryValue));
        }

        [Fact]
        public void ChangingWorkingDirectoryShouldNotImpactUNCPaths()
        {
            string WorkingDirectoryValue = "C:\\Raven\\";
            if (EnvironmentUtils.RunningOnPosix == true)
                WorkingDirectoryValue = Environment.GetEnvironmentVariable("HOME") + @"\";
            
            var inMemoryConfiguration = new InMemoryRavenConfiguration();
            inMemoryConfiguration.Settings["Raven/WorkingDir"] = WorkingDirectoryValue;
            inMemoryConfiguration.Settings["Raven/DataDir"] = @"\\server1\ravendb\data";
            inMemoryConfiguration.Settings[Constants.FileSystem.DataDirectory] = @"\\server1\ravenfs\data";
            inMemoryConfiguration.Settings[Constants.Counter.DataDirectory] = @"\\server1\ravenfs\data";
            inMemoryConfiguration.Settings[Constants.TimeSeries.DataDirectory] = @"\\server1\ravenfs\data";
            inMemoryConfiguration.Initialize();

            var basePath = FilePathTools.MakeSureEndsWithSlash(AppDomain.CurrentDomain.BaseDirectory.ToFullPath());
            var workingDirectory = inMemoryConfiguration.WorkingDirectory;

            Assert.Equal(WorkingDirectoryValue, inMemoryConfiguration.WorkingDirectory);
            Assert.NotEqual(basePath, workingDirectory);
            if (EnvironmentUtils.RunningOnPosix)
            {
                Assert.True(inMemoryConfiguration.DataDirectory.StartsWith(@"/"));
                Assert.True(inMemoryConfiguration.FileSystem.DataDirectory.StartsWith(@"/"));
                Assert.True(inMemoryConfiguration.Counter.DataDirectory.StartsWith(@"/"));
                Assert.True(inMemoryConfiguration.TimeSeries.DataDirectory.StartsWith(@"/"));
            }
            else
            {
            Assert.True(inMemoryConfiguration.DataDirectory.StartsWith(@"\\"));
            Assert.True(inMemoryConfiguration.FileSystem.DataDirectory.StartsWith(@"\\"));
                Assert.True(inMemoryConfiguration.Counter.DataDirectory.StartsWith(@"\\"));
                Assert.True(inMemoryConfiguration.TimeSeries.DataDirectory.StartsWith(@"\\"));
        }
        }

        [Fact]
        public void CanUseAppDrivePrefixInWorkingDirectoryForAutoDriveLetterCalculations()
        {
            const string WorkingDirectoryValue = "appDrive:\\Raven\\";
            var inMemoryConfiguration = new InMemoryRavenConfiguration();
            inMemoryConfiguration.Settings["Raven/WorkingDir"] = WorkingDirectoryValue;
            inMemoryConfiguration.Initialize();

            var basePath = FilePathTools.MakeSureEndsWithSlash(AppDomain.CurrentDomain.BaseDirectory.ToFullPath());
            var rootPath = Path.GetPathRoot(basePath);
            var workingDirectory = inMemoryConfiguration.WorkingDirectory;

            Assert.NotEqual(basePath, workingDirectory);
            Assert.True(workingDirectory.StartsWith(rootPath));
        }

        [Fact]
        public void DefaultInMemoryRavenConfigurationShouldBeInitializedCorrectly()
        {
            var inMemoryConfiguration = new InMemoryRavenConfiguration();
            inMemoryConfiguration.Initialize();

            int defaultMaxNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 128 * 1024 : 16 * 1024;
            int defaultInitialNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 512 : 256;

            var stronglyTypedConfiguration = new StronglyTypedRavenSettings(inMemoryConfiguration.Settings);
            stronglyTypedConfiguration.Setup(defaultMaxNumberOfItemsToIndexInSingleBatch, defaultInitialNumberOfItemsToIndexInSingleBatch);

            var configurationComparer = new ConfigurationComparer(inMemoryConfiguration, stronglyTypedConfiguration, propertyPathsToIgnore);
            configurationComparer.Ignore(x => x.EnableResponseLoggingForEmbeddedDatabases);
            configurationComparer.Ignore(x => x.DynamicMemoryLimitForProcessing);
            configurationComparer.Ignore(x => x.EmbeddedResponseStreamMaxCachedBlocks);
            configurationComparer.Assert(expected => expected.MaxPrecomputedBatchSizeForNewIndex.Value, actual => actual.MaxPrecomputedBatchSizeForNewIndex);
            configurationComparer.Assert(expected => expected.MaxPrecomputedBatchTotalDocumentSizeInBytes.Value, actual => actual.MaxPrecomputedBatchTotalDocumentSizeInBytes);			
            configurationComparer.Assert(expected => expected.RejectClientsModeEnabled.Value, actual => actual.RejectClientsMode);
            configurationComparer.Assert(expected => expected.MaxSecondsForTaskToWaitForDatabaseToLoad.Value, actual => actual.MaxSecondsForTaskToWaitForDatabaseToLoad);
            configurationComparer.Assert(expected => expected.NewIndexInMemoryMaxTime.Value, actual => actual.NewIndexInMemoryMaxTime);
            configurationComparer.Assert(expected => expected.Replication.FetchingFromDiskTimeoutInSeconds.Value, actual => actual.Replication.FetchingFromDiskTimeoutInSeconds);
            configurationComparer.Assert(expected => expected.ConcurrentResourceLoadTimeout.Value, actual => actual.ConcurrentResourceLoadTimeout);
            configurationComparer.Assert(expected => expected.MaxConcurrentResourceLoads.Value, actual => actual.MaxConcurrentResourceLoads);
            configurationComparer.Assert(expected => expected.SkipConsistencyCheck.Value, actual => actual.Storage.SkipConsistencyCheck);
            configurationComparer.Assert(expected => expected.PutSerialLockDuration.Value, actual => actual.Storage.PutSerialLockDuration);
            configurationComparer.Assert(expected => expected.Prefetcher.MaximumSizeAllowedToFetchFromStorageInMb.Value, actual => actual.Prefetcher.MaximumSizeAllowedToFetchFromStorageInMb);
            configurationComparer.Assert(expected => expected.Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds.Value, actual => actual.Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds);
            configurationComparer.Assert(expected => expected.Voron.AllowIncrementalBackups.Value, actual => actual.Storage.Voron.AllowIncrementalBackups);
            configurationComparer.Assert(expected => expected.Voron.AllowOn32Bits.Value, actual => actual.Storage.Voron.AllowOn32Bits);
            configurationComparer.Assert(expected => expected.Voron.InitialFileSize.Value, actual => actual.Storage.Voron.InitialFileSize);
            configurationComparer.Assert(expected => expected.Voron.ScratchBufferSizeNotificationThreshold.Value, actual => actual.Storage.Voron.ScratchBufferSizeNotificationThreshold);
            configurationComparer.Assert(expected => expected.Voron.MaxBufferPoolSize.Value, actual => actual.Storage.Voron.MaxBufferPoolSize);
            configurationComparer.Assert(expected => expected.Voron.MaxScratchBufferSize.Value, actual => actual.Storage.Voron.MaxScratchBufferSize);
            configurationComparer.Assert(expected => expected.Voron.MaxSizePerScratchBufferFile.Value, actual => actual.Storage.Voron.MaxSizePerScratchBufferFile);
            configurationComparer.Assert(expected => expected.Voron.TempPath.Value, actual => actual.Storage.Voron.TempPath);
            configurationComparer.Assert(expected => expected.Esent.CacheSizeMax.Value, actual => actual.Storage.Esent.CacheSizeMax);
            configurationComparer.Assert(expected => expected.Esent.MaxVerPages.Value, actual => actual.Storage.Esent.MaxVerPages);
            configurationComparer.Assert(expected => expected.Esent.PreferredVerPages.Value, actual => actual.Storage.Esent.PreferredVerPages);
            configurationComparer.Assert(expected => expected.Esent.DbExtensionSize.Value, actual => actual.Storage.Esent.DbExtensionSize);
            configurationComparer.Assert(expected => expected.Esent.LogFileSize.Value, actual => actual.Storage.Esent.LogFileSize);
            configurationComparer.Assert(expected => expected.Esent.LogBuffers.Value, actual => actual.Storage.Esent.LogBuffers);
            configurationComparer.Assert(expected => expected.Esent.MaxCursors.Value, actual => actual.Storage.Esent.MaxCursors);
            configurationComparer.Assert(expected => expected.Esent.CircularLog.Value, actual => actual.Storage.Esent.CircularLog);
            configurationComparer.Assert(expected => expected.Esent.MaxSessions.Value, actual => actual.Storage.Esent.MaxSessions);
            configurationComparer.Assert(expected => expected.Esent.CheckpointDepthMax.Value, actual => actual.Storage.Esent.CheckpointDepthMax);
            configurationComparer.Assert(expected => expected.Esent.MaxInstances.Value, actual => actual.Storage.Esent.MaxInstances);
            configurationComparer.Assert(expected => expected.FileSystem.MaximumSynchronizationInterval.Value, actual => actual.FileSystem.MaximumSynchronizationInterval);
            configurationComparer.Assert(expected => expected.Encryption.EncryptionKeyBitsPreference.Value, actual => actual.Encryption.EncryptionKeyBitsPreference);
            configurationComparer.Assert(expected => expected.Encryption.UseFips.Value, actual => actual.Encryption.UseFips);
            configurationComparer.Assert(expected => expected.Encryption.UseSsl.Value, actual => actual.Encryption.UseSsl);
            configurationComparer.Assert(expected => expected.MaxConcurrentServerRequests.Value, actual => actual.MaxConcurrentServerRequests);
            configurationComparer.Assert(expected => expected.PrefetchingDurationLimit.Value, actual => actual.PrefetchingDurationLimit);
            configurationComparer.Assert(expected => expected.BulkImportBatchTimeout.Value, actual => actual.BulkImportBatchTimeout);
            configurationComparer.Assert(expected => expected.DatbaseOperationTimeout.Value, actual => actual.DatabaseOperationTimeout);
            configurationComparer.Assert(expected => expected.TimeToWaitBeforeRunningIdleIndexes.Value, actual => actual.TimeToWaitBeforeRunningIdleIndexes);
            configurationComparer.Assert(expected => expected.TimeToWaitBeforeRunningAbandonedIndexes.Value, actual => actual.TimeToWaitBeforeRunningAbandonedIndexes);
            configurationComparer.Assert(expected => expected.TimeToWaitBeforeMarkingIdleIndexAsAbandoned.Value, actual => actual.TimeToWaitBeforeMarkingIdleIndexAsAbandoned);

            configurationComparer.Assert(expected => expected.WebSockets.InitialBufferPoolSize.Value, actual => actual.WebSockets.InitialBufferPoolSize);

            configurationComparer.Assert(expected => expected.Indexing.DisableIndexingFreeSpaceThreshold.Value, actual => actual.Indexing.DisableIndexingFreeSpaceThreshold);
            configurationComparer.Assert(expected => expected.Indexing.DisableMapReduceInMemoryTracking.Value, actual => actual.Indexing.DisableMapReduceInMemoryTracking);
            configurationComparer.Assert(expected => expected.Indexing.SkipRecoveryOnStartup.Value, actual => actual.Indexing.SkipRecoveryOnStartup);

            configurationComparer.Assert(expected => expected.Monitoring.Snmp.Port.Value, actual => actual.Monitoring.Snmp.Port);
            configurationComparer.Assert(expected => expected.Monitoring.Snmp.Community.Value, actual => actual.Monitoring.Snmp.Community);
            configurationComparer.Assert(expected => expected.Monitoring.Snmp.Enabled.Value, actual => actual.Monitoring.Snmp.Enabled);

            configurationComparer.Assert(expected => expected.LowMemoryLimitForLinuxDetectionInMB.Value, actual => actual.LowMemoryForLinuxDetectionInMB);
            
            configurationComparer.Assert(expected => expected.TimeToWaitBeforeMarkingAutoIndexAsIdle.Value, actual => actual.TimeToWaitBeforeMarkingAutoIndexAsIdle);
            configurationComparer.Assert(expected => expected.RedirectStudioUrl.Value, actual => actual.RedirectStudioUrl);
            configurationComparer.Assert(expected => expected.ResetIndexOnUncleanShutdown.Value, actual => actual.ResetIndexOnUncleanShutdown);
            configurationComparer.Assert(expected => expected.MaxPageSize.Value, actual => actual.MaxPageSize);
            configurationComparer.Assert(expected => expected.MemoryCacheLimitPercentage.Value, actual => actual.MemoryCacheLimitPercentage);
            configurationComparer.Assert(expected => expected.MemoryCacheLimitMegabytes.Value, actual => actual.MemoryCacheLimitMegabytes);
            configurationComparer.Assert(expected => expected.MemoryCacheLimitCheckInterval.Value, actual => actual.MemoryCacheLimitCheckInterval);
            configurationComparer.Assert(expected => expected.MaxNumberOfItemsToProcessInSingleBatch.Value, actual => actual.MaxNumberOfItemsToProcessInSingleBatch);
            configurationComparer.Assert(expected => expected.MaxNumberOfItemsToReduceInSingleBatch.Value, actual => actual.MaxNumberOfItemsToReduceInSingleBatch);
            configurationComparer.Assert(expected => expected.NumberOfItemsToExecuteReduceInSingleStep.Value, actual => actual.NumberOfItemsToExecuteReduceInSingleStep);
            configurationComparer.Assert(expected => expected.NewIndexInMemoryMaxMb.Value, actual => actual.NewIndexInMemoryMaxBytes);
            configurationComparer.Assert(expected => expected.HostName.Value, actual => actual.HostName);
            configurationComparer.Assert(expected => expected.ExposeConfigOverTheWire.Value, actual => actual.ExposeConfigOverTheWire);
            configurationComparer.Assert(expected => expected.AccessControlMaxAge.Value, actual => actual.AccessControlMaxAge);
            configurationComparer.Assert(expected => expected.AccessControlAllowMethods.Value, actual => actual.AccessControlAllowMethods);
            configurationComparer.Assert(expected => expected.AccessControlRequestHeaders.Value, actual => actual.AccessControlRequestHeaders);
            configurationComparer.Assert(expected => expected.HttpCompression.Value, actual => actual.HttpCompression);
            configurationComparer.Assert(expected => expected.RunInMemory.Value, actual => actual.RunInMemory);
            configurationComparer.Assert(expected => expected.DisableInMemoryIndexing.Value, actual => actual.DisableInMemoryIndexing);
            configurationComparer.Assert(expected => expected.WebDir.Value, actual => actual.WebDir);
            configurationComparer.Assert(expected => expected.DisableDocumentPreFetching.Value, actual => actual.DisableDocumentPreFetching);
            configurationComparer.Assert(expected => expected.MaxNumberOfItemsToPreFetch.Value, actual => actual.MaxNumberOfItemsToPreFetch);
            configurationComparer.Assert(expected => expected.MemoryCacheExpiration.Value, actual => actual.MemoryCacheExpiration);
            configurationComparer.Assert(expected => expected.CreateAutoIndexesForAdHocQueriesIfNeeded.Value, actual => actual.CreateAutoIndexesForAdHocQueriesIfNeeded);
            configurationComparer.Assert(expected => expected.MaxIndexCommitPointStoreTimeInterval.Value, actual => actual.MaxIndexCommitPointStoreTimeInterval);
            configurationComparer.Assert(expected => expected.MinIndexingTimeIntervalToStoreCommitPoint.Value, actual => actual.MinIndexingTimeIntervalToStoreCommitPoint);
            configurationComparer.Assert(expected => expected.MaxNumberOfStoredCommitPoints.Value, actual => actual.MaxNumberOfStoredCommitPoints);
            // allow +- 16MB tolerance in memory during the test (can happen in slow machines / debug):
            configurationComparer.AssertInRange(expected => expected.MemoryLimitForProcessing.Value, actual => actual.MemoryLimitForProcessingInMb, 16);
            configurationComparer.Assert(expected => expected.AvailableMemoryForRaisingBatchSizeLimit.Value, actual => actual.AvailableMemoryForRaisingBatchSizeLimit);
            configurationComparer.Assert(expected => expected.MaxProcessingRunLatency.Value, actual => actual.MaxProcessingRunLatency);
            configurationComparer.Assert(expected => expected.DisableClusterDiscovery.Value, actual => actual.DisableClusterDiscovery);
            configurationComparer.Assert(expected => expected.TurnOffDiscoveryClient.Value, actual => actual.TurnOffDiscoveryClient);
            configurationComparer.Assert(expected => expected.ServerName.Value, actual => actual.ServerName);
            configurationComparer.Assert(expected => expected.MaxStepsForScript.Value, actual => actual.MaxStepsForScript);
            configurationComparer.Assert(expected => expected.AdditionalStepsForScriptBasedOnDocumentSize.Value, actual => actual.AdditionalStepsForScriptBasedOnDocumentSize);
            configurationComparer.Assert(expected => expected.MaxIndexWritesBeforeRecreate.Value, actual => actual.MaxIndexWritesBeforeRecreate);
            configurationComparer.Assert(expected => expected.MaxSimpleIndexOutputsPerDocument.Value, actual => actual.MaxSimpleIndexOutputsPerDocument);
            configurationComparer.Assert(expected => expected.MaxMapReduceIndexOutputsPerDocument.Value, actual => actual.MaxMapReduceIndexOutputsPerDocument);
            configurationComparer.Assert(expected => expected.PrewarmFacetsOnIndexingMaxAge.Value, actual => actual.PrewarmFacetsOnIndexingMaxAge);
            configurationComparer.Assert(expected => expected.PrewarmFacetsSyncronousWaitTime.Value, actual => actual.PrewarmFacetsSyncronousWaitTime);
            configurationComparer.Assert(expected => expected.MaxNumberOfParallelProcessingTasks.Value, actual => actual.MaxNumberOfParallelProcessingTasks);
            configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.DataDir.Value.ToFullPath(null)), actual => actual.DataDirectory);
            configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.Counter.DataDir.Value.ToFullPath(null)), actual => actual.Counter.DataDirectory);
            configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.TimeSeries.DataDir.Value.ToFullPath(null)), actual => actual.TimeSeries.DataDirectory);
            configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.PluginsDirectory.Value.ToFullPath(null)), actual => actual.PluginsDirectory);
            configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.AssembliesDirectory.Value.ToFullPath(null)), actual => actual.AssembliesDirectory);
            configurationComparer.Assert(expected => expected.EmbeddedFilesDirectory.Value.ToFullPath(null), actual => actual.EmbeddedFilesDirectory);
            configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.FileSystem.DataDir.Value.ToFullPath(null)), actual => actual.FileSystem.DataDirectory);
            configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.FileSystem.DataDir.Value.ToFullPath(null)) + @"Indexes", actual => actual.FileSystem.IndexStoragePath);
            configurationComparer.Assert(expected => expected.FileSystem.DefaultStorageTypeName.Value, actual => actual.FileSystem.DefaultStorageTypeName);
            configurationComparer.Assert(expected => expected.MaxConcurrentMultiGetRequests.Value, actual => actual.MaxConcurrentMultiGetRequests);
            configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.DataDir.Value.ToFullPath(null)) + @"Indexes", actual => actual.IndexStoragePath);
            configurationComparer.Assert(expected => expected.DefaultStorageTypeName.Value, actual => actual.DefaultStorageTypeName);
            configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.CompiledIndexCacheDirectory.Value.ToFullPath(null)), actual => actual.CompiledIndexCacheDirectory);
            configurationComparer.Assert(expected => expected.Studio.AllowNonAdminUsersToSetupPeriodicExport.Value, actual => actual.Studio.AllowNonAdminUsersToSetupPeriodicExport);
            configurationComparer.Assert(expected => expected.FlushIndexToDiskSizeInMb.Value, actual => actual.FlushIndexToDiskSizeInMb);
            configurationComparer.Assert(expected => expected.TombstoneRetentionTime.Value, actual => actual.TombstoneRetentionTime);
            configurationComparer.Assert(expected => expected.Counter.ReplicationLatencyInMs.Value, actual => actual.Counter.ReplicationLatencyInMs);
            configurationComparer.Assert(expected => expected.Counter.TombstoneRetentionTime.Value, actual => actual.Counter.TombstoneRetentionTime);
            configurationComparer.Assert(expected => expected.Counter.DeletedTombstonesInBatch.Value, actual => actual.Counter.DeletedTombstonesInBatch);
            configurationComparer.Assert(expected => expected.Counter.BatchTimeout.Value, actual => actual.Counter.BatchTimeout);
            configurationComparer.Assert(expected => expected.TimeSeries.TombstoneRetentionTime.Value, actual => actual.TimeSeries.TombstoneRetentionTime);
            configurationComparer.Assert(expected => expected.TimeSeries.DeletedTombstonesInBatch.Value, actual => actual.TimeSeries.DeletedTombstonesInBatch);
            configurationComparer.Assert(expected => expected.TimeSeries.ReplicationLatencyInMs.Value, actual => actual.TimeSeries.ReplicationLatencyInMs);
            configurationComparer.Assert(expected => expected.Replication.ReplicationRequestTimeoutInMilliseconds.Value, actual => actual.Replication.ReplicationRequestTimeoutInMilliseconds);
            configurationComparer.Assert(expected => expected.Replication.ForceReplicationRequestBuffering.Value, actual => actual.Replication.ForceReplicationRequestBuffering);
            configurationComparer.Assert(expected => expected.Indexing.MaxNumberOfItemsToProcessInTestIndexes.Value, actual => actual.Indexing.MaxNumberOfItemsToProcessInTestIndexes);
            configurationComparer.Assert(expected => expected.Indexing.MaxNumberOfStoredIndexingBatchInfoElements.Value, actual => actual.Indexing.MaxNumberOfStoredIndexingBatchInfoElements);
            configurationComparer.Assert(expected => expected.Indexing.UseLuceneASTParser.Value, actual => actual.Indexing.UseLuceneASTParser);
            configurationComparer.Assert(expected => expected.IndexAndTransformerReplicationLatencyInSec.Value, actual => actual.IndexAndTransformerReplicationLatencyInSec);
            configurationComparer.Assert(expected => expected.MaxConcurrentRequestsForDatabaseDuringLoad.Value, actual => actual.MaxConcurrentRequestsForDatabaseDuringLoad);
            configurationComparer.Assert(expected => expected.Replication.MaxNumberOfItemsToReceiveInSingleBatch.Value, actual => actual.Replication.MaxNumberOfItemsToReceiveInSingleBatch);
            configurationComparer.Assert(expected => expected.Replication.ReplicationPropagationDelayInSeconds.Value, actual => actual.Replication.ReplicationPropagationDelayInSeconds);        
            configurationComparer.Assert(expected => expected.Replication.CertificatePath.Value, actual => actual.Replication.CertificatePath);        
            configurationComparer.Assert(expected => expected.Replication.CertificatePassword.Value, actual => actual.Replication.CertificatePassword);        
            configurationComparer.Assert(expected => expected.ImplicitFetchFieldsFromDocumentMode.Value, actual => actual.ImplicitFetchFieldsFromDocumentMode);
            configurationComparer.Assert(expected => expected.AllowScriptsToAdjustNumberOfSteps.Value, actual => actual.AllowScriptsToAdjustNumberOfSteps);
            configurationComparer.Assert(expected => expected.FileSystem.PreventSchemaUpdate.Value, actual => actual.Storage.PreventSchemaUpdate);
            configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.WorkingDir.Value.ToFullPath(null)), actual => actual.WorkingDirectory);
            configurationComparer.Assert(expected => expected.MaxClauseCount.Value, actual => actual.MaxClauseCount);

            configurationComparer.Assert(expected => expected.CacheDocumentsInMemory.Value, actual => actual.CacheDocumentsInMemory);
            configurationComparer.Assert(expected => expected.MinThreadPoolWorkerThreads.Value, actual => actual.MinThreadPoolWorkerThreads);
            configurationComparer.Assert(expected => expected.MinThreadPoolCompletionThreads.Value, actual => actual.MinThreadPoolCompletionThreads);

            configurationComparer.Assert(expected => expected.SqlReplication.CommandTimeoutInSec.Value, actual => actual.SqlReplication.CommandTimeoutInSec);

            configurationComparer.Assert(expected => expected.Cluster.HeartbeatTimeout.Value, actual => actual.Cluster.HeartbeatTimeout);
            configurationComparer.Assert(expected => expected.Cluster.ElectionTimeout.Value, actual => actual.Cluster.ElectionTimeout);
            configurationComparer.Assert(expected => expected.Cluster.MaxEntriesPerRequest.Value, actual => actual.Cluster.MaxEntriesPerRequest);
            configurationComparer.Assert(expected => expected.Cluster.MaxStepDownDrainTime.Value, actual => actual.Cluster.MaxStepDownDrainTime);
            configurationComparer.Assert(expected => expected.Cluster.MaxLogLengthBeforeCompaction.Value, actual => actual.Cluster.MaxLogLengthBeforeCompaction);
            configurationComparer.Assert(expected => expected.TempPath.Value, actual => actual.TempPath);
            configurationComparer.Assert(expected => expected.Cluster.MaxReplicationLatency.Value, actual => actual.Cluster.MaxReplicationLatency);
            configurationComparer.Assert(expected => expected.FileSystem.DisableRDC.Value, actual => actual.FileSystem.DisableRDC);
            configurationComparer.Assert(expected => expected.FileSystem.SynchronizationBatchProcessing.Value, actual => actual.FileSystem.SynchronizationBatchProcessing);

            configurationComparer.Ignore(x => x.Storage.Esent.JournalsStoragePath);
            configurationComparer.Ignore(x => x.Storage.Voron.JournalsStoragePath);
            configurationComparer.Ignore(x => x.Storage.SkipConsistencyCheck);
            configurationComparer.Ignore(x => x.IgnoreSslCertificateErrors);
            configurationComparer.Ignore(x => x.AnonymousUserAccessMode);
            configurationComparer.Ignore(x => x.TransactionMode);
            configurationComparer.Ignore(x => x.CustomMemoryCacher);

            Assert.NotNull(inMemoryConfiguration.OAuthTokenKey);
            Assert.Equal("/", inMemoryConfiguration.VirtualDirectory);
            Assert.Empty(inMemoryConfiguration.AccessControlAllowOrigin);
            Assert.NotNull(inMemoryConfiguration.ServerUrl);
            Assert.NotNull(inMemoryConfiguration.OAuthTokenServer);
            Assert.True(inMemoryConfiguration.UseDefaultOAuthTokenServer);
            Assert.Empty(inMemoryConfiguration.HeadersToIgnore);
            Assert.Equal(null, inMemoryConfiguration.CustomTaskScheduler);
            Assert.Empty(inMemoryConfiguration.ActiveBundles);
            Assert.Equal("*", stronglyTypedConfiguration.Port.Value);
            Assert.True(inMemoryConfiguration.Port >= 8080);
            Assert.Equal("Open", inMemoryConfiguration.ExposeConfigOverTheWire);
            Assert.True(inMemoryConfiguration.CreateAnalyzersDirectoryIfNotExisting);
            Assert.True(inMemoryConfiguration.CreatePluginsDirectoryIfNotExisting);
            Assert.Equal(null, inMemoryConfiguration.Storage.Esent.JournalsStoragePath);
            Assert.Equal(null, inMemoryConfiguration.Storage.Voron.JournalsStoragePath);
            Assert.Equal(false, inMemoryConfiguration.Storage.SkipConsistencyCheck);

            configurationComparer.Validate();
        }

        private class ConfigurationComparer
        {
            private readonly InMemoryRavenConfiguration inMemoryConfiguration;

            private readonly StronglyTypedRavenSettings stronglyTypedConfiguration;

            private readonly HashSet<string> assertedPropertyPaths;

            private readonly List<string> propertyPathsToCheck;

            public ConfigurationComparer(InMemoryRavenConfiguration inMemoryConfiguration, StronglyTypedRavenSettings stronglyTypedConfiguration, HashSet<string> propertyPathsToIgnore)
            {
                this.inMemoryConfiguration = inMemoryConfiguration;
                this.stronglyTypedConfiguration = stronglyTypedConfiguration;

                assertedPropertyPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                propertyPathsToCheck = GetPropertyPathsToCheck(inMemoryConfiguration).Where(x => propertyPathsToIgnore.Contains(x) == false).ToList();
            }

            public void Ignore(Expression<Func<InMemoryRavenConfiguration, object>> actual)
            {
                var propertyPath = actual.ToPropertyPath();
                assertedPropertyPaths.Add(propertyPath);
            }

            public void Assert<T>(Expression<Func<StronglyTypedRavenSettings, T>> expected, Expression<Func<InMemoryRavenConfiguration, T>> actual)
            {
                var propertyPath = actual.ToPropertyPath();
                if (assertedPropertyPaths.Add(propertyPath) == false)
                    throw new InvalidOperationException("Cannot assert one property more than once. Path: " + propertyPath);

                if (propertyPathsToCheck.Contains(propertyPath) == false)
                    throw new InvalidOperationException("Cannot assert property that is not on a list of properties to assert. Path: " + propertyPath);

                Func<StronglyTypedRavenSettings, T> e;
                try
                {
                    e = expected.Compile();
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failure when compiling " + expected, ex);
                }
                T expectedValue;
                try
                {
                    expectedValue = e(stronglyTypedConfiguration);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failure when running " + expected, ex);

                }

                var a = actual.Compile();
                var actualValue = a(inMemoryConfiguration);

                Xunit.Assert.Equal(expectedValue, actualValue);
            }

            public void AssertInRange<T>(Expression<Func<StronglyTypedRavenSettings, T>> expected, Expression<Func<InMemoryRavenConfiguration, T>> actual, int range)
            {
                var propertyPath = actual.ToPropertyPath();
                if (assertedPropertyPaths.Add(propertyPath) == false)
                    throw new InvalidOperationException("Cannot assert one property more than once. Path: " + propertyPath);

                if (propertyPathsToCheck.Contains(propertyPath) == false)
                    throw new InvalidOperationException("Cannot assert property that is not on a list of properties to assert. Path: " + propertyPath);

                var e = expected.Compile();
                var expectedValue = e(stronglyTypedConfiguration);

                var a = actual.Compile();
                var actualValue = a(inMemoryConfiguration);

                int low    = Convert.ToInt32(expectedValue) - range;
                int high   = Convert.ToInt32(expectedValue) + range;
                int value = Convert.ToInt32(actualValue);

                Xunit.Assert.InRange(value, low, high);
            }

            public void Validate()
            {
                var except = propertyPathsToCheck
                    .Except(assertedPropertyPaths)
                    .ToList();

                if (except.Count == 0)
                    return;

                var message = new StringBuilder();
                message.AppendLine("There are some properties that were not asserted:");
                foreach (var e in except)
                {
                    message.AppendLine(e);
                }

                throw new InvalidOperationException(message.ToString());
            }

            private static IEnumerable<string> GetPropertyPathsToCheck(object o, string parentPropertyPath = null)
            {
                var type = o.GetType();
                var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                foreach (var property in properties)
                {
                    if (property.Name == "ImplicitFetchFieldsFromDocumentMode")
                    {

                    }
                    if (property.PropertyType.IsEnum == false &&
                        property.PropertyType.FullName.StartsWith("System") == false)
                    {
                        var propertyPath = parentPropertyPath == null ? property.Name : parentPropertyPath + "." + property.Name;
                        foreach (var info in GetPropertyPathsToCheck(property.GetValue(o), propertyPath))
                            yield return info;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(parentPropertyPath))
                            yield return property.Name;
                        else
                            yield return parentPropertyPath + "." + property.Name;
                    }
                }
            }
        }
    }
}
#endif