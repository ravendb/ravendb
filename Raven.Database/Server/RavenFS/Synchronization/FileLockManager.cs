using System;
using System.IO;
using NLog;
using Raven.Abstractions.RavenFS;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Util;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class FileLockManager
	{
		private readonly TimeSpan defaultTimeout = TimeSpan.FromMinutes(10);
		private readonly Logger log = LogManager.GetCurrentClassLogger();
		private TimeSpan configuredTimeout;

		private TimeSpan SynchronizationTimeout(IStorageActionsAccessor accessor)
		{
			var timeoutConfigExists = accessor.TryGetConfigurationValue(
				SynchronizationConstants.RavenSynchronizationLockTimeout, out configuredTimeout);

			return timeoutConfigExists ? configuredTimeout : defaultTimeout;
		}

		public void LockByCreatingSyncConfiguration(string fileName, ServerInfo sourceServer, IStorageActionsAccessor accessor)
		{
			var syncLock = new SynchronizationLock
			{
				SourceServer = sourceServer,
				FileLockedAt = DateTime.UtcNow
			};

			accessor.SetConfig(RavenFileNameHelper.SyncLockNameForFile(fileName), syncLock.AsConfig());

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
				syncLock =
					accessor.GetConfig(RavenFileNameHelper.SyncLockNameForFile(fileName)).AsObject<SynchronizationLock>();
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
