// -----------------------------------------------------------------------
//  <copyright file="RavenSettings.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections;
using System.Collections.Generic;
using Raven.Database.Config.Settings;

namespace Raven.Database.Config
{
	public class RavenSettings
	{

		public IntergerSettingWithMin MaxPageSize { get; set; }

		public IntegerSetting MemoryCacheLimitMegabytes { get; set; }

		public TimeSpanSetting MemoryCacheExpiration { get; set; }

		public IntegerSetting MemoryCacheLimitPercentage { get; set; }

		public TimeSpanSetting MemoryCacheLimitCheckInterval { get; set; }

		public TimeSpanSetting MaxIndexingRunLatency { get; set; }

		public IntergerSettingWithMin MaxNumberOfItemsToIndexInSingleBatch { get; set; }

		public IntegerSetting AvailableMemoryForRaisingIndexBatchSizeLimit { get; set; }

		public IntergerSettingWithMin MaxNumberOfItemsToReduceInSingleBatch { get; set; }

		public IntegerSetting NumberOfItemsToExecuteReduceInSingleStep { get; set; }

		public IntergerSettingWithMin MaxNumberOfParallelIndexTasks { get; set; }

		public IntegerSetting TempIndexPromotionMinimumQueryCount { get; set; }
	}
}