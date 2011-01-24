//-----------------------------------------------------------------------
// <copyright file="RemoteManagedStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Database;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Munin;
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
                persistentSource = new MemoryPersistentSource(state.Log);
            }
        }
        public void Batch(Action<IStorageActionsAccessor> action)
        {
			using (var tableStorage = new TableStorage(persistentSource))
			{
				tableStorage.Initialze();
				var accessor = new StorageActionsAccessor(tableStorage, new DummyUuidGenerator(), new AbstractDocumentCodec[0]);
				action(accessor);
			}
        }

        public void Dispose()
        {
            persistentSource.Dispose();
        }
    }
}