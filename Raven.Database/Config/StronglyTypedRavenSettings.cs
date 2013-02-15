// -----------------------------------------------------------------------
//  <copyright file="StronglyTypedRavenSettings.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.IO;
using System.Runtime.Caching;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config
{
	public class StronglyTypedRavenSettings
	{
		private readonly NameValueCollection settings;

		public StronglyTypedRavenSettings(NameValueCollection settings)
		{
			this.settings = settings;
		}

		public void Setup(int defaultMaxNumberOfItemsToIndexInSingleBatch, int defaultInitialNumberOfItemsToIndexInSingleBatch)
		{
			MaxPageSize =
				new IntegerSettingWithMin(settings["Raven/MaxPageSize"], 1024, 10);
			MemoryCacheLimitMegabytes =
				new IntegerSetting(settings["Raven/MemoryCacheLimitMegabytes"], GetDefaultMemoryCacheLimitMegabytes);
			MemoryCacheExpiration =
				new TimeSpanSetting(settings["Raven/MemoryCacheExpiration"], TimeSpan.FromMinutes(5),
				                    TimeSpanArgumentType.FromSeconds);
			MemoryCacheLimitPercentage =
				new IntegerSetting(settings["Raven/MemoryCacheLimitPercentage"], 0 /* auto size */);
			MemoryCacheLimitCheckInterval =
				new TimeSpanSetting(settings["Raven/MemoryCacheLimitCheckInterval"], MemoryCache.Default.PollingInterval,
				                    TimeSpanArgumentType.FromParse);
			MaxIndexingRunLatency =
				new TimeSpanSetting(settings["Raven/MaxIndexingRunLatency"], TimeSpan.FromMinutes(5),
				                    TimeSpanArgumentType.FromParse);
			MaxNumberOfItemsToIndexInSingleBatch =
				new IntegerSettingWithMin(settings["Raven/MaxNumberOfItemsToIndexInSingleBatch"],
				                          defaultMaxNumberOfItemsToIndexInSingleBatch, 128);
			AvailableMemoryForRaisingIndexBatchSizeLimit =
				new IntegerSetting(settings["Raven/AvailableMemoryForRaisingIndexBatchSizeLimit"],
				                   Math.Min(768, MemoryStatistics.TotalPhysicalMemory/2));
			MaxNumberOfItemsToReduceInSingleBatch =
				new IntegerSettingWithMin(settings["Raven/MaxNumberOfItemsToReduceInSingleBatch"],
				                          defaultMaxNumberOfItemsToIndexInSingleBatch/2, 128);
			NumberOfItemsToExecuteReduceInSingleStep =
				new IntegerSetting(settings["Raven/NumberOfItemsToExecuteReduceInSingleStep"], 1024);
			MaxNumberOfParallelIndexTasks =
				new IntegerSettingWithMin(settings["Raven/MaxNumberOfParallelIndexTasks"], Environment.ProcessorCount, 1);
			TempIndexPromotionMinimumQueryCount =
				new IntegerSetting(settings["Raven/TempIndexPromotionMinimumQueryCount"], 100);
			TempIndexPromotionThreshold =
				new IntegerSetting(settings["Raven/TempIndexPromotionThreshold"], 60000 /* once a minute */);
			TempIndexCleanupPeriod =
				new TimeSpanSetting(settings["Raven/TempIndexCleanupPeriod"], TimeSpan.FromMinutes(10),
				                    TimeSpanArgumentType.FromSeconds);
			TempIndexCleanupThreshold =
				new TimeSpanSetting(settings["Raven/TempIndexCleanupThreshold"], TimeSpan.FromMinutes(20),
				                    TimeSpanArgumentType.FromSeconds);
			TempIndexInMemoryMaxMb =
				new MultipliedIntegerSetting(new IntegerSettingWithMin(settings["Raven/TempIndexInMemoryMaxMB"], 25, 1), 1024*1024);
			RunInMemory =
				new BooleanSetting(settings["Raven/RunInMemory"], false);
			CreateTemporaryIndexesForAdHocQueriesIfNeeded =
				new BooleanSetting(settings["Raven/CreateTemporaryIndexesForAdHocQueriesIfNeeded"], true);
			ResetIndexOnUncleanShutdown =
				new BooleanSetting(settings["Raven/ResetIndexOnUncleanShutdown"], false);
			DataDir =
				new StringSetting(settings["Raven/DataDir"], @"~\Data");
			IndexStoragePath =
				new StringSetting(settings["Raven/IndexStoragePath"], (string) null);
			HostName =
				new StringSetting(settings["Raven/HostName"], (string) null);
			Port =
				new StringSetting(settings["Raven/Port"], (string) null);
			HttpCompression =
				new BooleanSetting(settings["Raven/HttpCompression"], true);
			AccessControlAllowOrigin =
				new StringSetting(settings["Raven/AccessControlAllowOrigin"], (string) null);
			AccessControlMaxAge =
				new StringSetting(settings["Raven/AccessControlMaxAge"], "1728000" /* 20 days */);
			AccessControlAllowMethods =
				new StringSetting(settings["Raven/AccessControlAllowMethods"], "PUT,PATCH,GET,DELETE,POST");
			AccessControlRequestHeaders =
				new StringSetting(settings["Raven/AccessControlRequestHeaders"], (string) null);
			RedirectStudioUrl =
				new StringSetting(settings["Raven/RedirectStudioUrl"], (string) null);
			DisableDocumentPreFetchingForIndexing =
				new BooleanSetting(settings["Raven/DisableDocumentPreFetchingForIndexing"], false);
			WebDir =
				new StringSetting(settings["Raven/WebDir"], GetDefaultWebDir);
			PluginsDirectory =
				new StringSetting(settings["Raven/PluginsDirectory"], @"~\Plugins");
			TaskScheduler =
				new StringSetting(settings["Raven/TaskScheduler"], (string) null);
			AllowLocalAccessWithoutAuthorization =
				new BooleanSetting(settings["Raven/AllowLocalAccessWithoutAuthorization"], false);
			MaxIndexCommitPointStoreTimeInterval =
				new TimeSpanSetting(settings["Raven/MaxIndexCommitPointStoreTimeInterval"], TimeSpan.FromMinutes(5),
				                    TimeSpanArgumentType.FromParse);
			MaxNumberOfStoredCommitPoints =
				new IntegerSetting(settings["Raven/MaxNumberOfStoredCommitPoints"], 5);
			MinIndexingTimeIntervalToStoreCommitPoint =
				new TimeSpanSetting(settings["Raven/MinIndexingTimeIntervalToStoreCommitPoint"], TimeSpan.FromMinutes(1),
				                    TimeSpanArgumentType.FromParse);
		}

		private string GetDefaultWebDir()
		{
			return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"Raven/WebUI");
		}

		private int GetDefaultMemoryCacheLimitMegabytes()
		{
			var cacheSizeMaxSetting = new IntegerSetting(settings["Raven/Esent/CacheSizeMax"], 1024);

			// we need to leave ( a lot ) of room for other things as well, so we min the cache size
			var val = (MemoryStatistics.TotalPhysicalMemory/2) -
			          // reduce the unmanaged cache size from the default min
									cacheSizeMaxSetting.Value;

			if (val < 0)
				return 128; // if machine has less than 1024 MB, then only use 128 MB 

			return val;
		}

		public IntegerSettingWithMin MaxPageSize { get; private set; }

		public IntegerSetting MemoryCacheLimitMegabytes { get; private set; }

		public TimeSpanSetting MemoryCacheExpiration { get; private set; }

		public IntegerSetting MemoryCacheLimitPercentage { get; private set; }

		public TimeSpanSetting MemoryCacheLimitCheckInterval { get; private set; }

		public TimeSpanSetting MaxIndexingRunLatency { get; private set; }

		public IntegerSettingWithMin MaxNumberOfItemsToIndexInSingleBatch { get; private set; }

		public IntegerSetting AvailableMemoryForRaisingIndexBatchSizeLimit { get; private set; }

		public IntegerSettingWithMin MaxNumberOfItemsToReduceInSingleBatch { get; private set; }

		public IntegerSetting NumberOfItemsToExecuteReduceInSingleStep { get; private set; }

		public IntegerSettingWithMin MaxNumberOfParallelIndexTasks { get; private set; }

		public IntegerSetting TempIndexPromotionMinimumQueryCount { get; private set; }

		public IntegerSetting TempIndexPromotionThreshold { get; private set; }

		public TimeSpanSetting TempIndexCleanupPeriod { get; private set; }

		public TimeSpanSetting TempIndexCleanupThreshold { get; private set; }

		public MultipliedIntegerSetting TempIndexInMemoryMaxMb { get; private set; }

		public BooleanSetting RunInMemory { get; private set; }

		public BooleanSetting CreateTemporaryIndexesForAdHocQueriesIfNeeded { get; private set; }

		public BooleanSetting ResetIndexOnUncleanShutdown { get; private set; }

		public StringSetting DataDir { get; private set; }

		public StringSetting IndexStoragePath { get; private set; }

		public StringSetting HostName { get; private set; }

		public StringSetting Port { get; private set; }

		public BooleanSetting HttpCompression { get; private set; }

		public StringSetting AccessControlAllowOrigin { get; private set; }

		public StringSetting AccessControlMaxAge { get; private set; }

		public StringSetting AccessControlAllowMethods { get; private set; }

		public StringSetting AccessControlRequestHeaders { get; private set; }

		public StringSetting RedirectStudioUrl { get; private set; }

		public BooleanSetting DisableDocumentPreFetchingForIndexing { get; private set; }

		public StringSetting WebDir { get; private set; }

		public StringSetting PluginsDirectory { get; private set; }

		public StringSetting TaskScheduler { get; private set; }

		public BooleanSetting AllowLocalAccessWithoutAuthorization { get; private set; }

		public TimeSpanSetting MaxIndexCommitPointStoreTimeInterval { get; private set; }

		public TimeSpanSetting MinIndexingTimeIntervalToStoreCommitPoint { get; private set; }

		public IntegerSetting MaxNumberOfStoredCommitPoints { get; private set; }
	}
}