// -----------------------------------------------------------------------
//  <copyright file="ConfigAccessor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Storage;

namespace Raven.Database.FileSystem.Synchronization
{
	public static class SynchronizationConfigAccessor
	{
		private static readonly ILog Log = LogManager.GetCurrentClassLogger();

		public static SynchronizationConfig GetOrDefault(IStorageActionsAccessor accessor)
		{
			try
			{
				if (accessor.ConfigExists(SynchronizationConstants.RavenSynchronizationConfig) == false)
					return new SynchronizationConfig(); // return a default one

				return accessor.GetConfig(SynchronizationConstants.RavenSynchronizationConfig).JsonDeserialization<SynchronizationConfig>();
			}
			catch (Exception e)
			{
				Log.Warn("Could not deserialize a synchronization configuration", e);
				return new SynchronizationConfig(); // return a default one
			}
		}

		public static SynchronizationConfig GetOrDefault(ITransactionalStorage storage)
		{
			SynchronizationConfig result = null;

			storage.Batch(accessor => result = GetOrDefault(accessor));

			return result ?? new SynchronizationConfig();
		}
	}
}