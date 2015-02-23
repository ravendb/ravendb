// -----------------------------------------------------------------------
//  <copyright file="ConfigurationTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Util;

using Xunit;

namespace Raven.Tests.Core.Configuration
{
	public class ConfigurationTests
	{
		private readonly HashSet<string> propertyPathsToIgnore = new HashSet<string>
		                                                         {
			                                                         "DatabaseName",
																	 "CountersDatabaseName",
																	 "FileSystemName",
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
		public void DefaultInMemoryRavenConfigurationShouldBeInitializedCorrectly()
		{
			var inMemoryConfiguration = new InMemoryRavenConfiguration();
			inMemoryConfiguration.Initialize();

			int defaultMaxNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 128 * 1024 : 16 * 1024;
			int defaultInitialNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 512 : 256;

			var stronglyTypedConfiguration = new StronglyTypedRavenSettings(inMemoryConfiguration.Settings);
			stronglyTypedConfiguration.Setup(defaultMaxNumberOfItemsToIndexInSingleBatch, defaultInitialNumberOfItemsToIndexInSingleBatch);

			var configurationComparer = new ConfigurationComparer(inMemoryConfiguration, stronglyTypedConfiguration, propertyPathsToIgnore);
			configurationComparer.Ignore(x=>x.EnableResponseLoggingForEmbeddedDatabases);
			configurationComparer.Assert(expected => expected.RejectClientsModeEnabled.Value, actual => actual.RejectClientsMode);
			configurationComparer.Assert(expected => expected.MaxSecondsForTaskToWaitForDatabaseToLoad.Value, actual => actual.MaxSecondsForTaskToWaitForDatabaseToLoad);
			configurationComparer.Assert(expected => expected.NewIndexInMemoryMaxTime.Value, actual => actual.NewIndexInMemoryMaxTime);
			configurationComparer.Assert(expected => expected.Replication.FetchingFromDiskTimeoutInSeconds.Value, actual => actual.Replication.FetchingFromDiskTimeoutInSeconds);
			configurationComparer.Assert(expected => expected.Prefetcher.MaximumSizeAllowedToFetchFromStorageInMb.Value, actual => actual.Prefetcher.MaximumSizeAllowedToFetchFromStorageInMb);
			configurationComparer.Assert(expected => expected.Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds.Value, actual => actual.Prefetcher.FetchingDocumentsFromDiskTimeoutInSeconds);
			configurationComparer.Assert(expected => expected.Voron.AllowIncrementalBackups.Value, actual => actual.Storage.Voron.AllowIncrementalBackups);
			configurationComparer.Assert(expected => expected.Voron.InitialFileSize.Value, actual => actual.Storage.Voron.InitialFileSize);
			configurationComparer.Assert(expected => expected.Voron.MaxBufferPoolSize.Value, actual => actual.Storage.Voron.MaxBufferPoolSize);
			configurationComparer.Assert(expected => expected.Voron.MaxScratchBufferSize.Value, actual => actual.Storage.Voron.MaxScratchBufferSize);
			configurationComparer.Assert(expected => expected.Voron.TempPath.Value, actual => actual.Storage.Voron.TempPath);
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
			configurationComparer.Assert(expected => expected.AllowLocalAccessWithoutAuthorization.Value, actual => actual.AllowLocalAccessWithoutAuthorization);
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
			configurationComparer.Assert(expected => expected.MemoryLimitForProcessing.Value, actual => actual.MemoryLimitForProcessingInMb);
			configurationComparer.Assert(expected => expected.AvailableMemoryForRaisingBatchSizeLimit.Value, actual => actual.AvailableMemoryForRaisingBatchSizeLimit);
			configurationComparer.Assert(expected => expected.MaxProcessingRunLatency.Value, actual => actual.MaxProcessingRunLatency);
			configurationComparer.Assert(expected => expected.DisableClusterDiscovery.Value, actual => actual.DisableClusterDiscovery);
			configurationComparer.Assert(expected => expected.ServerName.Value, actual => actual.ServerName);
			configurationComparer.Assert(expected => expected.MaxStepsForScript.Value, actual => actual.MaxStepsForScript);
			configurationComparer.Assert(expected => expected.MaxRecentTouchesToRemember.Value, actual => actual.MaxRecentTouchesToRemember);
			configurationComparer.Assert(expected => expected.AdditionalStepsForScriptBasedOnDocumentSize.Value, actual => actual.AdditionalStepsForScriptBasedOnDocumentSize);
			configurationComparer.Assert(expected => expected.MaxIndexWritesBeforeRecreate.Value, actual => actual.MaxIndexWritesBeforeRecreate);
			configurationComparer.Assert(expected => expected.MaxSimpleIndexOutputsPerDocument.Value, actual => actual.MaxSimpleIndexOutputsPerDocument);
			configurationComparer.Assert(expected => expected.MaxMapReduceIndexOutputsPerDocument.Value, actual => actual.MaxMapReduceIndexOutputsPerDocument);
			configurationComparer.Assert(expected => expected.PrewarmFacetsOnIndexingMaxAge.Value, actual => actual.PrewarmFacetsOnIndexingMaxAge);
			configurationComparer.Assert(expected => expected.PrewarmFacetsSyncronousWaitTime.Value, actual => actual.PrewarmFacetsSyncronousWaitTime);
			configurationComparer.Assert(expected => expected.MaxNumberOfParallelProcessingTasks.Value, actual => actual.MaxNumberOfParallelProcessingTasks);
			configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.DataDir.Value.ToFullPath(null)), actual => actual.DataDirectory);
			configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.CountersDataDir.Value.ToFullPath(null)), actual => actual.CountersDataDirectory);
			configurationComparer.Assert(expected => expected.PluginsDirectory.Value.ToFullPath(null), actual => actual.PluginsDirectory);
            configurationComparer.Assert(expected => expected.AssembliesDirectory.Value.ToFullPath(null), actual => actual.AssembliesDirectory);
            configurationComparer.Assert(expected => expected.EmbeddedFilesDirectory.Value.ToFullPath(null), actual => actual.EmbeddedFilesDirectory);
			configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.FileSystem.DataDir.Value.ToFullPath(null)), actual => actual.FileSystem.DataDirectory);
			configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.FileSystem.DataDir.Value.ToFullPath(null)) + @"Indexes", actual => actual.FileSystem.IndexStoragePath);
			configurationComparer.Assert(expected => expected.FileSystem.DefaultStorageTypeName.Value, actual => actual.FileSystem.DefaultStorageTypeName);
			configurationComparer.Assert(expected => expected.MaxConcurrentMultiGetRequests.Value, actual => actual.MaxConcurrentMultiGetRequests);
			configurationComparer.Assert(expected => FilePathTools.MakeSureEndsWithSlash(expected.DataDir.Value.ToFullPath(null)) + @"Indexes", actual => actual.IndexStoragePath);
			configurationComparer.Assert(expected => expected.DefaultStorageTypeName.Value, actual => actual.DefaultStorageTypeName);
			configurationComparer.Assert(expected => expected.CompiledIndexCacheDirectory.Value.ToFullTempPath(), actual => actual.CompiledIndexCacheDirectory);
			configurationComparer.Assert(expected => expected.FlushIndexToDiskSizeInMb.Value, actual => actual.FlushIndexToDiskSizeInMb);
			configurationComparer.Assert(expected => expected.TombstoneRetentionTime.Value, actual => actual.TombstoneRetentionTime);
			configurationComparer.Assert(expected => expected.Replication.ReplicationRequestTimeoutInMilliseconds.Value, actual => actual.Replication.ReplicationRequestTimeoutInMilliseconds);
			configurationComparer.Assert(expected => expected.Indexing.MaxNumberOfItemsToProcessInTestIndexes.Value, actual => actual.Indexing.MaxNumberOfItemsToProcessInTestIndexes);
			configurationComparer.Assert(expected => expected.IndexAndTransformerReplicationLatencyInSec.Value, actual => actual.IndexAndTransformerReplicationLatencyInSec);
			configurationComparer.Assert(expected => expected.MaxConcurrentRequestsForDatabaseDuringLoad.Value, actual => actual.MaxConcurrentRequestsForDatabaseDuringLoad);
			configurationComparer.Assert(expected => expected.Replication.MaxNumberOfItemsToReceiveInSingleBatch.Value, actual => actual.Replication.MaxNumberOfItemsToReceiveInSingleBatch);
			configurationComparer.Ignore(x => x.Storage.Esent.JournalsStoragePath);
			configurationComparer.Ignore(x => x.Storage.Voron.JournalsStoragePath);

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

				var e = expected.Compile();
				var expectedValue = e(stronglyTypedConfiguration);

				var a = actual.Compile();
				var actualValue = a(inMemoryConfiguration);

				Xunit.Assert.Equal(expectedValue, actualValue);
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
					if (property.PropertyType.FullName.StartsWith("System") == false)
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