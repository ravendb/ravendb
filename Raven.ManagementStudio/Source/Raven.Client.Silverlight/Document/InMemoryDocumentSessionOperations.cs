namespace Raven.Client.Silverlight.Document
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Raven.Client.Silverlight.Common.Helpers;
    using Raven.Client.Silverlight.Data;

    public class InMemoryDocumentSessionOperations : IDisposable
    {
        protected readonly HashSet<object> DeletedEntities;

        protected readonly IDictionary<string, DocumentSession.StoredDocument> StoredEntities;

        public InMemoryDocumentSessionOperations()
        {
            this.StoredEntities = new Dictionary<string, DocumentSession.StoredDocument>(StringComparer.InvariantCultureIgnoreCase);
            this.DeletedEntities = new HashSet<object>();
        }

        public void Dispose()
        {
        }

        public void Store(object entity)
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
                var storedDocument = new DocumentSession.StoredDocument()
                                         {
                                             CurrentState = entity,
                                             BaseState = (entity as Entity).ToJson(),
                                             IsNew = generated
                                         };

                this.StoredEntities.Add(id, storedDocument);
            }
        }

        public void StoreMany<T>(IList<T> entities)
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
    }
}
