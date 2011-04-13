//-----------------------------------------------------------------------
// <copyright file="RemoteEsentStorage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Runtime.Caching;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.MEF;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Raven.Storage.Esent.StorageActions;

namespace Raven.Storage.Esent
{
    public class RemoteEsentStorage : IRemoteStorage
    {
        private readonly JET_INSTANCE instance;
        private readonly TableColumnsCache tableColumnsCache;
        private readonly string database;
        private readonly IDocumentCacher documentCacher;

        public RemoteEsentStorage(RemoteEsentStorageState state)
        {
            instance = state.Instance;
            database = state.Database;
            tableColumnsCache = new TableColumnsCache();
            tableColumnsCache.InitColumDictionaries(instance, database);
            documentCacher = new DocumentCacher();
        }

        public void Batch(Action<IStorageActionsAccessor> action)
        {
            using (var pht = new DocumentStorageActions(instance, database, tableColumnsCache, new OrderedPartCollection<AbstractDocumentCodec>(), new DummyUuidGenerator(), documentCacher))
            {
                action(new StorageActionsAccessor(pht));
            }
        }

        public void Dispose()
        {
            documentCacher.Dispose();
        }
    }
}
