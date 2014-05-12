using System;
using System.IO;
using NLog;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.RavenFS;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Util;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class FileLockManager
	{
		private readonly TimeSpan defaultTimeout = TimeSpan.FromMinutes(10);
		private readonly Logger log = LogManager.GetCurrentClassLogger();

		private TimeSpan SynchronizationTimeout(IStorageActionsAccessor accessor)
		{
            string timeoutConfigKey = string.Empty;
            accessor.TryGetConfigurationValue<string>(SynchronizationConstants.RavenSynchronizationLockTimeout, out timeoutConfigKey);

            TimeSpan timeoutConfiguration;
            if (TimeSpan.TryParse(timeoutConfigKey, out timeoutConfiguration))
                return timeoutConfiguration;

            return defaultTimeout;
		}

		public void LockByCreatingSyncConfiguration(string fileName, ServerInfo sourceServer, IStorageActionsAccessor accessor)
		{
			var syncLock = new SynchronizationLock
			{
				SourceServer = sourceServer,
				FileLockedAt = DateTime.UtcNow
			};

            accessor.SetConfig(RavenFileNameHelper.SyncLockNameForFile(fileName), JsonExtensions.ToJObject(syncLock));

            log.Debug("File '{0}' was locked", fileName);
		}

		public void UnlockByDeletingSyncConfiguration(string fileName, IStorageActionsAccessor accessor)
		{
			accessor.DeleteConfig(RavenFileNameHelper.SyncLockNameForFile(fileName));
			log.Debug("File '{0}' was unlocked", fileName);
		}

		public bool TimeoutExceeded(string fileName, IStorageActionsAccessor accessor)
		{
			SynchronizationLock syncLock;

			try
			{
                syncLock = accessor.GetConfig(RavenFileNameHelper.SyncLockNameForFile(fileName)).JsonDeserialization<SynchronizationLock>();				
			}
			catch (FileNotFoundException)
			{
				return true;
			}

			return DateTime.UtcNow - syncLock.FileLockedAt > SynchronizationTimeout(accessor);
		}

		public bool TimeoutExceeded(string fileName, ITransactionalStorage storage)
		{
			var result = false;

			storage.Batch(accessor => result = TimeoutExceeded(fileName, accessor));

			return result;
		}
	}
}
