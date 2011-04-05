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
using Raven.Storage.Esent.StorageActions;

namespace Raven.Storage.Esent
{
    public class RemoteEsentStorage : IRemoteStorage, IDocumentCacher
    {
        private readonly JET_INSTANCE instance;
        private readonly TableColumnsCache tableColumnsCache;
        private readonly string database;

		private readonly ObjectCache cachedSerializedDocuments = new MemoryCache(typeof(RemoteEsentStorage).FullName + ".Cache");

		public Tuple<JObject, JObject> GetCachedDocument(string key, Guid etag)
		{
			var cachedDocument = (Tuple<JObject, JObject>)cachedSerializedDocuments.Get("Doc/" + key + "/" + etag);
			if (cachedDocument != null)
				return Tuple.Create(new JObject(cachedDocument.Item1), new JObject(cachedDocument.Item2));
			return null;
		}

		public void SetCachedDocument(string key, Guid etag, Tuple<JObject, JObject> doc)
		{
			cachedSerializedDocuments["Doc/" + key + "/" + etag] = doc;
		}

        public RemoteEsentStorage(RemoteEsentStorageState state)
        {
            instance = state.Instance;
            database = state.Database;
            tableColumnsCache = new TableColumnsCache();
            tableColumnsCache.InitColumDictionaries(instance, database);
        }

        public void Batch(Action<IStorageActionsAccessor> action)
        {
            using (var pht = new DocumentStorageActions(instance, database, tableColumnsCache, new OrderedPartCollection<AbstractDocumentCodec>(), new DummyUuidGenerator(), this))
            {
                action(new StorageActionsAccessor(pht));
            }
        }

        public void Dispose()
        {
            
        }
    }
}
