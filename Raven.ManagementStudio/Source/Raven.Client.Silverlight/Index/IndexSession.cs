namespace Raven.Client.Silverlight.Index
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using Raven.Client.Silverlight.Common;
    using Raven.Client.Silverlight.Common.Helpers;
    using Raven.Client.Silverlight.Data;

    public class IndexSession : InMemorySessionOperations<JsonIndex>, IAsyncIndexSession
    {
        private readonly IIndexRepository indexRepository;

        public IndexSession(Uri databaseAddress)
        {
            this.indexRepository = new IndexRepository(databaseAddress);
        }

        public void Load<T>(string name, CallbackFunction.Load<T> callback) where T : JsonIndex
        {
            StoredEntity existingEntity;
            if (StoredEntities.TryGetValue(name, out existingEntity))
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

            this.indexRepository.Get(name, callback, this.Store);
        }

        public void LoadMany<T>(string[] names, CallbackFunction.Load<IList<T>> callback) where T : JsonIndex
        {
            this.indexRepository.GetMany(names, StoredEntities.Select(x => (x.Value.CurrentState as T)).ToList(), callback, this.StoreMany);
        }

        public void LoadMany<T>(CallbackFunction.Load<IList<T>> callback) where T : JsonIndex
        {
            this.indexRepository.GetMany(null, null, callback, this.StoreMany);
        }

        public void StoreEntity<T>(T entity) where T : JsonIndex
        {
            this.Store(entity);
        }

        public void Delete<T>(T entity) where T : JsonIndex
        {
            bool generated;
            Guard.Assert(() => StoredEntities.ContainsKey(GetOrGenerateDocumentKey(entity, out generated)));

            DeletedEntities.Add(entity);
        }

        public void SaveChanges(CallbackFunction.Save<JsonIndex> callback)
        {
            foreach (var deletedEntity in DeletedEntities)
            {
                if (StoredEntities.Any(x => x.Value.CurrentState == deletedEntity))
                {
                    StoredEntities.Remove(StoredEntities.Where(x => x.Value.CurrentState == deletedEntity).FirstOrDefault());
                }

                this.indexRepository.Delete(deletedEntity, callback);
            }

            this.DeletedEntities.Clear();

            var dirtyEntities = this.StoredEntities.Where(x => x.Value.IsDirty).ToDictionary(x => x.Key, y => y.Value);
            foreach (var storedIndex in dirtyEntities)
            {
                if (storedIndex.Value.IsNew)
                {
                    this.indexRepository.Put(storedIndex.Value.CurrentState, callback, this.Store);
                }
                else
                {
                    // PATCH
                }

                this.StoredEntities.Remove(storedIndex);
            }
        }

        public void Refresh<T>(T entity, CallbackFunction.Load<T> callback) where T : JsonIndex
        {
            Guard.Assert(() => !string.IsNullOrEmpty(entity.Id));

            if (this.StoredEntities.ContainsKey(entity.Id))
            {
                this.StoredEntities.Remove(entity.Id);
            }

            if (this.DeletedEntities.Contains(entity))
            {
                this.DeletedEntities.Remove(entity);
            }

            this.indexRepository.Get(entity.Name, callback, this.Store);
        }
    }
}
