namespace Raven.Client.Silverlight.Document
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Newtonsoft.Json.Linq;
    using Raven.Client.Silverlight.Common;
    using Raven.Client.Silverlight.Common.Helpers;
    using Raven.Client.Silverlight.Data;

    public class DocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSession
    {
        private readonly IDocumentRepository documentRepository;

        public DocumentSession(Uri databaseAddress)
        {
            this.documentRepository = new DocumentRepository(databaseAddress);
        }

        public void Load<T>(string key, CallbackFunction.Load<T> callback) where T : JsonDocument
        {
            StoredDocument existingEntity;
            if (StoredEntities.TryGetValue(key, out existingEntity))
            {
                SynchronizationContext.Current.Post(
                    delegate
                    {
                        callback.Invoke((T)existingEntity.CurrentState);
                    },
                    null);

                return;
            }

            this.documentRepository.Get(key, callback, this.Store);
        }

        public void LoadMany<T>(string[] keys, CallbackFunction.Load<IList<T>> callback) where T : JsonDocument
        {
            Guard.Assert(() => keys != null);
            Guard.Assert(() => keys.Length > 0);

            StoredDocument existingEntity;

            var existingEntities = new List<T>();

            foreach (var id in keys)
            {
                StoredEntities.TryGetValue(id, out existingEntity);
                if (existingEntity != null)
                {
                    existingEntities.Add((T)existingEntity.CurrentState);
                }
            }

            if (existingEntities.Count == keys.Length)
            {
                SynchronizationContext.Current.Post(
                    delegate
                    {
                        callback.Invoke(existingEntities);
                    },
                    null);

                return;
            }

            this.documentRepository.GetMany(keys, existingEntities.Select(x => x.Id).ToArray(), callback, this.StoreMany);
        }

        public void LoadMany<T>(CallbackFunction.Load<IList<T>> callback) where T : JsonDocument
        {
            this.documentRepository.GetMany(null, StoredEntities.Select(x => (x.Value.CurrentState as JsonDocument).Key).ToArray(), callback, this.StoreMany);
        }

        public void StoreEntity<T>(T entity) where T : JsonDocument
        {
            this.Store(entity);
        }

        public void Delete<T>(T entity) where T : JsonDocument
        {
            bool generated;
            Guard.Assert(() => StoredEntities.ContainsKey(GetOrGenerateDocumentKey(entity, out generated)));

            DeletedEntities.Add(entity);
        }

        public void SaveChanges(CallbackFunction.Save callback)
        {
            foreach (var deletedEntity in DeletedEntities)
            {
                if (StoredEntities.Any(x => x.Value.CurrentState == deletedEntity))
                {
                    StoredEntities.Remove(StoredEntities.Where(x => x.Value.CurrentState == deletedEntity).FirstOrDefault());
                }

                this.documentRepository.Delete(deletedEntity as JsonDocument, callback);
            }

            this.DeletedEntities.Clear();

            var dirtyEntities = StoredEntities.Where(x => x.Value.IsDirty).ToDictionary(x => x.Key, y => y.Value);
            foreach (var storedDocument in dirtyEntities)
            {
                if (storedDocument.Value.IsNew)
                {
                    if ((storedDocument.Value.CurrentState as JsonDocument).Key == "auto-generated")
                    {
                        this.documentRepository.Post(storedDocument.Value.CurrentState as JsonDocument, callback, this.Store);
                    }
                    else
                    {
                        this.documentRepository.Put(storedDocument.Value.CurrentState as JsonDocument, callback, this.Store);
                    }
                }
                else
                {
                    // PATCH
                }

                StoredEntities.Remove(storedDocument);
            }
        }

        public class StoredDocument
        {
            public object CurrentState { get; set; }

            public JObject BaseState { get; set; }

            public bool IsDirty
            {
                get { return this.IsNew || (this.CurrentState as Entity).ToJson().ToString() != this.BaseState.ToString(); }
            }

            public bool IsNew { get; set; }
        }
    }
}
