using System;
using System.IO;
using NLog;
using Raven.Abstractions.Extensions;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Util;
using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Synchronization
{
	public class FileLockManager
	{
		private readonly Logger log = LogManager.GetCurrentClassLogger();

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

			return (DateTime.UtcNow - syncLock.FileLockedAt).TotalMilliseconds > SynchronizationConfigAccessor.GetOrDefault(accessor).SynchronizationLockTimeoutMiliseconds;
		}

		public bool TimeoutExceeded(string fileName, ITransactionalStorage storage)
		{
			var result = false;

			storage.Batch(accessor => result = TimeoutExceeded(fileName, accessor));

			return result;
		}
	}
}
