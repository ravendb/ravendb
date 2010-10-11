using System;
using Raven.Database;
using Raven.Database.Storage;
using Raven.Storage.Managed.Impl;

namespace Raven.Storage.Managed
{
    public class RemoteManagedStorage : IRemoteStorage
    {
        private readonly IPersistentSource persistentSource;

        public RemoteManagedStorage(RemoteManagedStorageState state)
        {
            if (state.Path != null)
            {
                persistentSource = new ReadOnlyFileBasedPersistentSource(state.Path, state.Prefix);
            }
            else
            {
                persistentSource = new MemoryPersistentSource(state.Data, state.Log);
            }
        }
        public void Batch(Action<IStorageActionsAccessor> action)
        {
            var tableStorage = new TableStorage(persistentSource);
            tableStorage.Initialze();
            var accessor = new StorageActionsAccessor(tableStorage);
            action(accessor);
        }

        public void Dispose()
        {
            persistentSource.Dispose();
        }
    }
}