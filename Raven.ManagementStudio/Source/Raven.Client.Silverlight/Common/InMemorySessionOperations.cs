namespace Raven.Client.Silverlight.Common
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Newtonsoft.Json.Linq;
    using Raven.Client.Silverlight.Common.Helpers;
    using Raven.Client.Silverlight.Data;

    public class InMemorySessionOperations<T> : IDisposable where T : Entity
    {
        protected readonly HashSet<T> DeletedEntities;

        protected readonly IDictionary<string, StoredEntity> StoredEntities;

        public InMemorySessionOperations()
        {
            this.StoredEntities = new Dictionary<string, StoredEntity>(StringComparer.InvariantCultureIgnoreCase);
            this.DeletedEntities = new HashSet<T>();
        }

        public void Dispose()
        {
        }

        public void Store(T entity)
        {
            Guard.Assert(() => entity != null);

            bool generated;

            string id = this.GetOrGenerateDocumentKey(entity, out generated);

            if (this.StoredEntities.ContainsKey(id))
            {
                // Guard.Assert(() => ReferenceEquals(storedEntities[id], entity));
                this.StoredEntities[id].CurrentState = entity;
            }
            else
            {
                var storedDocument = new StoredEntity()
                                         {
                                             CurrentState = entity,
                                             BaseState = entity.ToJson(),
                                             IsNew = generated
                                         };

                this.StoredEntities.Add(id, storedDocument);
            }
        }

        public void StoreMany<T1>(IList<T1> entities) where T1 : T
        {
            foreach (var entity in entities)
            {
                this.Store(entity);
            }
        }

        public void Clear()
        {
            this.DeletedEntities.Clear();
            this.StoredEntities.Clear();
        }

        protected string GetOrGenerateDocumentKey(object entity, out bool generated)
        {
            generated = false;

            var identityProperty = entity.GetType().GetProperties().FirstOrDefault(x => x.Name == "Id");

            Guard.Assert(() => identityProperty != null);

            var value = identityProperty.GetValue(entity, null);
            var id = value as string;

            if (id == null)
            {
                id = Guid.NewGuid().ToString();
                identityProperty.SetValue(entity, id, null);
                generated = true;
            }

            return id;
        }

        protected class StoredEntity
        {
            public T CurrentState { get; set; }

            public JObject BaseState { get; set; }

            public bool IsDirty
            {
                get { return this.IsNew || this.CurrentState.ToJson().ToString() != this.BaseState.ToString(); }
            }

            public bool IsNew { get; set; }
        }
    }
}
