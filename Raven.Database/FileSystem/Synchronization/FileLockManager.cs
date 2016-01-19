using System;
using System.IO;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Util;
using FileSystemInfo = Raven.Abstractions.FileSystem.FileSystemInfo;

namespace Raven.Database.FileSystem.Synchronization
{
    public class FileLockManager
    {
        private readonly ILog log = LogManager.GetCurrentClassLogger();

        public void LockByCreatingSyncConfiguration(string fileName, FileSystemInfo sourceFileSystem, IStorageActionsAccessor accessor)
        {
            var syncLock = new SynchronizationLock
            {
                SourceFileSystem = sourceFileSystem,
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
