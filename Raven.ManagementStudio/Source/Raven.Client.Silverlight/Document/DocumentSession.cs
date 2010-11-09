namespace Raven.Client.Silverlight.Document
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using Raven.Client.Silverlight.Common;
    using Raven.Client.Silverlight.Common.Helpers;
    using Raven.Client.Silverlight.Data;

    public class DocumentSession : InMemorySessionOperations<JsonDocument>, IAsyncDocumentSession
    {
        private readonly IDocumentRepository documentRepository;

        public DocumentSession(Uri databaseAddress)
        {
            this.documentRepository = new DocumentRepository(databaseAddress);
        }

        public void Load<T>(string key, CallbackFunction.Load<T> callback) where T : JsonDocument
        {
            StoredEntity existingEntity;
            if (StoredEntities.TryGetValue(key, out existingEntity))
            {
                SynchronizationContext.Current.Post(
                    delegate
                    {
                        callback.Invoke(new LoadResponse<T>()
                                            {
                                                Data = existingEntity.CurrentState as T,
                                                StatusCode = HttpStatusCode.OK
                                            });
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

            StoredEntity existingEntity;

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
                            callback.Invoke(
                                new LoadResponse<IList<T>>()
                                    {
                                        Data = existingEntities,
                                        StatusCode = HttpStatusCode.OK
                                    });
                        },
                    null);

                return;
            }

            this.documentRepository.GetMany(keys, existingEntities, callback, this.StoreMany);
        }

        public void LoadMany<T>(CallbackFunction.Load<IList<T>> callback) where T : JsonDocument
        {
            this.documentRepository.GetMany(null, null, callback, this.StoreMany);
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

        public void SaveChanges(CallbackFunction.Save<JsonDocument> callback)
        {
            foreach (var deletedEntity in DeletedEntities)
            {
                if (StoredEntities.Any(x => x.Value.CurrentState == deletedEntity))
                {
                    StoredEntities.Remove(StoredEntities.Where(x => x.Value.CurrentState == deletedEntity).FirstOrDefault());
                }

                this.documentRepository.Delete(deletedEntity, callback);
            }

            this.DeletedEntities.Clear();

            var dirtyEntities = StoredEntities.Where(x => x.Value.IsDirty).ToDictionary(x => x.Key, y => y.Value);
            foreach (var storedDocument in dirtyEntities)
            {
                if (storedDocument.Value.IsNew)
                {
                    if (storedDocument.Value.CurrentState.Key == "auto-generated")
                    {
                        this.documentRepository.Post(storedDocument.Value.CurrentState, callback, this.Store);
                    }
                    else
                    {
                        this.documentRepository.Put(storedDocument.Value.CurrentState, callback, this.Store);
                    }
                }
                else
                {
                    // PATCH
                }

                StoredEntities.Remove(storedDocument);
            }
        }
    }
}
