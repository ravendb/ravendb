namespace Raven.Management.Client.Silverlight.Document
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Client;
    using Common.Converters;
    using Common.Exceptions;
    using Database;
    using Database.Data;
    using Newtonsoft.Json.Linq;
    using Raven.Client;
    using Raven.Client.Document;
    using Raven.Management.Client.Silverlight.Common;

    /// <summary>
    /// Abstract implementation for in memory session operations
    /// </summary>
    public abstract class InMemoryDocumentSessionOperations : IDisposable
    {
        protected T Convert<T>(object entity)
        {
            T result;

            try
            {
                result = (T)entity;
            }
            catch (Exception)
            {
                throw new NotSupportedException("We do not support this type of casting");
            }

            return result;
        }

        public object ConvertTo<T>(JsonDocument document)
        {
            T entity = default(T);
            EnsureNotReadVetoed(document.Metadata);
            var documentType = document.Metadata.Value<string>("Raven-Clr-Type");
            if (documentType != null)
            {
                Type type = Type.GetType(documentType);
                if (type != null)
                    entity = (T)document.DataAsJson.Deserialize(type, Conventions);
            }

            if (Equals(entity, default(T)))
            {
                entity = document.DataAsJson.Deserialize<T>(Conventions);
            }

            TrySetIdentity(entity, document.Key);
            return entity;
        }

        public object ConvertToJsonDocument(object entity)
        {
            DocumentMetadata documentMetadata = entitiesAndMetadata[entity];

            Type entityType = entity.GetType();
            PropertyInfo identityProperty = documentStore.Conventions.GetIdentityProperty(entityType);

            JObject objectAsJson = GetObjectAsJson(entity);
            if (identityProperty != null)
            {
                objectAsJson.Remove(identityProperty.Name);
            }

            documentMetadata.Metadata["Raven-Clr-Type"] = JToken.FromObject(ReflectionUtil.GetFullNameWithoutVersionInformation(entityType));

            return new JsonDocument
                       {
                           DataAsJson = objectAsJson,
                           Key = documentMetadata.Key,
                           Metadata = documentMetadata.Metadata,
                           Etag = documentMetadata.ETag.HasValue ? documentMetadata.ETag.Value : Guid.Empty
                       };
        }

        private const string RavenEntityName = "Raven-Entity-Name";

        /// <summary>
        /// The entities waiting to be deleted
        /// </summary>
        protected readonly HashSet<object> deletedEntities = new HashSet<object>();

        /// <summary>
        /// hold the data required to manage the data for RavenDB's Unit of Work
        /// </summary>
        protected readonly Dictionary<object, DocumentMetadata> entitiesAndMetadata = new Dictionary<object, DocumentMetadata>();

        /// <summary>
        /// Translate between a key and its associated entity
        /// </summary>
        protected readonly Dictionary<string, object> entitiesByKey = new Dictionary<string, object>(StringComparer.InvariantCultureIgnoreCase);

        protected readonly IDictionary<string, JsonDocument> documentByKey = new Dictionary<string, JsonDocument>(StringComparer.InvariantCultureIgnoreCase);

        /// <summary>
        /// The document store associated with this session
        /// </summary>
        protected DocumentStore documentStore;

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryDocumentSessionOperations"/> class.
        /// </summary>
        /// <param name="documentStore">The document store.</param>
        /// <param name="storeListeners">The store listeners.</param>
        /// <param name="deleteListeners">The delete listeners.</param>
        protected InMemoryDocumentSessionOperations(DocumentStore documentStore)
        {
            this.documentStore = documentStore;
            ResourceManagerId = documentStore.ResourceManagerId;
            UseOptimisticConcurrency = false;
            AllowNonAuthoritiveInformation = true;
            NonAuthoritiveInformationTimeout = TimeSpan.FromSeconds(15);
            MaxNumberOfRequestsPerSession = documentStore.Conventions.MaxNumberOfRequestsPerSession;
        }

        /// <summary>
        /// Gets the number of requests for this session
        /// </summary>
        /// <value></value>
        public int NumberOfRequests { get; private set; }

        /// <summary>
        /// Gets or sets the timeout to wait for authoritive information if encountered non authoritive document.
        /// </summary>
        /// <value></value>
        public TimeSpan NonAuthoritiveInformationTimeout { get; set; }

        /// <summary>
        /// Gets the conventions used by this session
        /// </summary>
        /// <value>The conventions.</value>
        /// <remarks>
        /// This instance is shared among all sessions, changes to the <see cref="DocumentConvention"/> should be done
        /// via the <see cref="IDocumentStore"/> instance, not on a single session.
        /// </remarks>
        public DocumentConvention Conventions
        {
            get { return documentStore.Conventions; }
        }

        /// <summary>
        /// The transaction resource manager identifier
        /// </summary>
        public Guid ResourceManagerId { get; private set; }

        /// <summary>
        /// Gets or sets the max number of requests per session.
        /// If the <see cref="NumberOfRequests"/> rise above <see cref="MaxNumberOfRequestsPerSession"/>, an exception will be thrown.
        /// </summary>
        /// <value>The max number of requests per session.</value>
        public int MaxNumberOfRequestsPerSession { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the session should use optimistic concurrency.
        /// When set to <c>true</c>, a check is made so that a change made behind the session back would fail
        /// and raise <see cref="Raven.Http.Exceptions.ConcurrencyException"/>.
        /// </summary>
        /// <value></value>
        public bool UseOptimisticConcurrency { get; set; }

        /// <summary>
        /// Gets a value indicating whether any of the entities tracked by the session has changes.
        /// </summary>
        /// <value></value>
        public bool HasChanges
        {
            get
            {
                return deletedEntities.Count > 0 ||
                       entitiesAndMetadata.Where(pair => EntityChanged(pair.Key, pair.Value)).Any();
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether non authoritive information is allowed.
        /// Non authoritive information is document that has been modified by a transaction that hasn't been committed.
        /// The server provides the latest committed version, but it is known that attempting to write to a non authoritive document
        /// will fail, because it is already modified.
        /// If set to <c>false</c>, the session will wait <see cref="NonAuthoritiveInformationTimeout"/> for the transaction to commit to get an
        /// authoritive information. If the wait is longer than <see cref="NonAuthoritiveInformationTimeout"/>, <see cref="NonAuthoritiveInformationException"/> is thrown.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if non authoritive information is allowed; otherwise, <c>false</c>.
        /// </value>
        public bool AllowNonAuthoritiveInformation { get; set; }

        #region IDisposable Members

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public virtual void Dispose()
        {
        }

        #endregion

        /// <summary>
        /// Occurs when an entity is stored in the session
        /// </summary>
        public virtual event EntityStored Stored;

        /// <summary>
        /// Occurs when an entity is converted to a document and metadata.
        /// Changes made to the document / metadata instances passed to this event will be persisted.
        /// </summary>
        public virtual event EntityToDocument OnEntityConverted;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="document"></param>
        /// <returns></returns>
        public string GetDocumentId(JsonDocument document)
        {
            DocumentMetadata value;
            return entitiesAndMetadata.TryGetValue(document, out value) == false ? null : value.Key;
        }

        /// <summary>
        /// Determines whether the specified entity has changed.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>
        /// 	<c>true</c> if the specified entity has changed; otherwise, <c>false</c>.
        /// </returns>
        public bool HasChanged(JsonDocument entity)
        {
            DocumentMetadata value;
            return entitiesAndMetadata.TryGetValue(entity, out value) && EntityChanged(entity, value);
        }

        /// <summary>
        /// Tracks the entity inside the unit of work
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="document">The document found.</param>
        /// <returns></returns>
        protected T TrackEntity<T>(JsonDocument document)
        {
            Guard.Assert(() => document != null);

            if (document.Metadata.Property("@etag") == null)
            {
                document.Metadata.Add("@etag", new JValue(document.Etag.ToString()));
            }

            if (document.NonAuthoritiveInformation && AllowNonAuthoritiveInformation == false)
            {
                throw new NonAuthoritiveInformationException("Document " + document.Key + " returned Non Authoritive Information (probably modified by a transaction in progress) and AllowNonAuthoritiveInformation  is set to false");
            }

            object entity;
            if (!entitiesByKey.TryGetValue(document.Key, out entity))
            {
                entitiesAndMetadata[document] = new DocumentMetadata
                {
                    OriginalValue = document.ToJson(),
                    Metadata = document.Metadata,
                    OriginalMetadata = new JObject(document.Metadata),
                    ETag = new Guid(document.Metadata.Value<string>("@etag")),
                    Key = document.Key
                };

                entitiesByKey[document.Key] = document;

                entity = document;
            }

            if (typeof(JsonDocument) == typeof(T))
            {
                return (T)entity;
            }

            return (T)ConvertToEntity<T>(document.Key, document.DataAsJson, document.Metadata);
        }

        ///// <summary>
        ///// Tracks the entity.
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="key">The key.</param>
        ///// <param name="document">The document.</param>
        ///// <param name="metadata">The metadata.</param>
        ///// <returns></returns>
        //public T TrackEntity<T>(string key, JObject document, JObject metadata)
        //{
        //    object entity;
        //    if (entitiesByKey.TryGetValue(key, out entity) == false)
        //    {
        //        entity = ConvertToEntity<T>(key, document, metadata);
        //    }
        //    else
        //    {
        //        // the local instnace may have been changed, we adhere to the current Unit of Work
        //        // instance, and return that, ignoring anything new.
        //        return (T)entity;
        //    }
        //    var etag = metadata.Value<string>("@etag");
        //    document.Remove("@metadata");
        //    if (metadata.Value<bool>("Non-Authoritive-Information") &&
        //        AllowNonAuthoritiveInformation == false)
        //    {
        //        throw new NonAuthoritiveInformationException("Document " + key +
        //                                                     " returned Non Authoritive Information (probably modified by a transaction in progress) and AllowNonAuthoritiveInformation  is set to false");
        //    }
        //    entitiesAndMetadata[entity] = new DocumentMetadata
        //                                      {
        //                                          OriginalValue = document,
        //                                          Metadata = metadata,
        //                                          OriginalMetadata = new JObject(metadata),
        //                                          ETag = new Guid(etag),
        //                                          Key = key
        //                                      };
        //    entitiesByKey[key] = entity;
        //    return (T)entity;
        //}

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be deleted when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The entity.</param>
        public void Delete<T>(T entity)
        {
            Guard.Assert(() => entitiesAndMetadata.ContainsKey(entity));

            deletedEntities.Add(entity);
        }

        /// <summary>
        /// Converts the json document to an entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <param name="documentFound">The document found.</param>
        /// <param name="metadata">The metadata.</param>
        /// <returns></returns>
        protected object ConvertToEntity<T>(string id, JObject documentFound, JObject metadata)
        {
            T entity = default(T);
            EnsureNotReadVetoed(metadata);
            var documentType = metadata.Value<string>("Raven-Clr-Type");
            if (documentType != null)
            {
                Type type = Type.GetType(documentType);
                if (type != null)
                    entity = (T)documentFound.Deserialize(type, Conventions);
            }

            if (Equals(entity, default(T)))
            {
                entity = documentFound.Deserialize<T>(Conventions);
            }

            TrySetIdentity(entity, id);
            return entity;
        }

        /// <summary>
        /// Tries to set the identity property
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="id">The id.</param>
        protected internal void TrySetIdentity<T>(T entity, string id)
        {
            PropertyInfo identityProperty = documentStore.Conventions.GetIdentityProperty(entity.GetType());
            if (identityProperty != null && identityProperty.CanWrite)
            {
                if (identityProperty.PropertyType == typeof(string))
                {
                    identityProperty.SetValue(entity, id, null);
                }
                else // need converting
                {
                    ITypeConverter converter =
                        Conventions.IdentityTypeConvertors.FirstOrDefault(
                            x => x.CanConvertFrom(identityProperty.PropertyType));
                    if (converter == null)
                        throw new ArgumentException("Could not convert identity to type " +
                                                    identityProperty.PropertyType +
                                                    " because there is not matching type converter registered in the conventions' IdentityTypeConvertors");

                    identityProperty.SetValue(entity, converter.ConvertTo(id), null);
                }
            }
        }

        private static void EnsureNotReadVetoed(JObject metadata)
        {
            var readVetoAsString = metadata.Value<string>("Raven-Read-Veto");
            if (readVetoAsString == null)
                return;

            JObject readVeto = JObject.Parse(readVetoAsString);

            var s = readVeto.Value<string>("Reason");
            throw new ReadVetoException(
                "Document could not be read because of a read veto." + Environment.NewLine +
                "The read was vetoed by: " + readVeto.Value<string>("Trigger") + Environment.NewLine +
                "Veto reason: " + s
                );
        }

        /// <summary>
        /// Stores the specified entity in the session. The entity will be saved when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// </summary>
        /// <param name="entity">The entity.</param>
        public void Store(object entity)
        {
            Guard.Assert(() => entity != null);

            string id = entity as JsonDocument == null ? GetOrGenerateDocumentKey(entity) : ((JsonDocument)entity).Key;

            TrySetIdentity(entity, id);

            // we make the check here even if we just generated the key
            // users can override the key generation behavior, and we need
            // to detect if they generate duplicates.
            if (id != null &&
                id.EndsWith("/") == false // not a prefix id
                && entitiesByKey.ContainsKey(id))
            {
                if (ReferenceEquals(entitiesByKey[id], entity))
                    return; // calling Store twice on the same reference is a no-op

                //if (entity as JsonDocument == null)
                //{
                //    throw new NonUniqueObjectException("Attempted to associated a different object with id '" + id + "'.");
                //}

                //entitiesAndMetadata.Remove(entitiesByKey[id]);
            }

            string tag = documentStore.Conventions.GetTypeTagName(entity.GetType());

            entitiesAndMetadata.Add(entity, new DocumentMetadata
                                                {
                                                    Key = id,
                                                    Metadata = new JObject(new JProperty(RavenEntityName, new JValue(tag))),
                                                    OriginalMetadata = new JObject(),
                                                    ETag = UseOptimisticConcurrency ? (Guid?)Guid.Empty : null,
                                                    OriginalValue = new JObject()
                                                });

            if (id != null)
                entitiesByKey[id] = entity;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entities"></param>
        public void StoreMany(IList<object> entities)
        {
            foreach (object entity in entities)
            {
                TrackEntity<JsonDocument>(entity as JsonDocument);
            }
        }

        /// <summary>
        /// Tries to get the identity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        protected string GetOrGenerateDocumentKey(object entity)
        {
            string id;
            TryGetIdFromInstance(entity, out id);

            return id ?? Conventions.GenerateDocumentKey(entity);
        }

        private bool TryGetIdFromInstance(object entity, out string id)
        {
            PropertyInfo identityProperty = GetIdentityProperty(entity.GetType());
            if (identityProperty != null)
            {
                object value = identityProperty.GetValue(entity, null);
                id = value as string;
                if (id == null && value != null) // need convertion
                {
                    ITypeConverter converter =
                        Conventions.IdentityTypeConvertors.FirstOrDefault(x => x.CanConvertFrom(value.GetType()));
                    if (converter == null)
                        throw new ArgumentException("Cannot use type " + value.GetType() +
                                                    " as an identity without having a type converter registered for it in the conventions' IdentityTypeConvertors");
                    id = converter.ConvertFrom(value);
                }
                return true;
            }
            id = null;
            return false;
        }

        /// <summary>
        /// Creates the put entity command.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <param name="documentMetadata">The document metadata.</param>
        /// <returns></returns>
        protected ICommandData CreatePutEntityCommand(object entity, DocumentMetadata documentMetadata)
        {
            if (entity as JsonDocument != null)
            {
                var document = (JsonDocument)entity;

                return new PutCommandData
                           {
                               Document = document.DataAsJson,
                               Etag = UseOptimisticConcurrency ? documentMetadata.ETag : null,
                               Key = document.Key,
                               Metadata = document.Metadata,
                           };
            }

            JObject json = ConvertEntityToJson(entity, documentMetadata.Metadata);

            Guid? etag = UseOptimisticConcurrency ? documentMetadata.ETag : null;

            return new PutCommandData
                       {
                           Document = json,
                           Etag = etag,
                           Key = documentMetadata.Key,
                           Metadata = documentMetadata.Metadata,
                       };
        }

        private PropertyInfo GetIdentityProperty(Type entityType)
        {
            return documentStore.Conventions.GetIdentityProperty(entityType);
        }

        /// <summary>
        /// Updates the batch results.
        /// </summary>
        /// <param name="batchResults">The batch results.</param>
        /// <param name="entities">The entities.</param>
        protected void UpdateBatchResults(IList<BatchResult> batchResults, IList<object> entities)
        {
            EntityStored stored = Stored;
            for (int i = 0; i < batchResults.Count; i++)
            {
                BatchResult batchResult = batchResults[i];
                if (batchResult.Method != "PUT")
                    continue;

                object entity = entities[i];
                DocumentMetadata documentMetadata;
                if (entitiesAndMetadata.TryGetValue(entity, out documentMetadata) == false)
                    continue;

                batchResult.Metadata["@etag"] = new JValue(batchResult.Etag.ToString());
                entitiesByKey[batchResult.Key] = entity;
                documentMetadata.ETag = batchResult.Etag;
                documentMetadata.Key = batchResult.Key;
                documentMetadata.OriginalMetadata = new JObject(batchResult.Metadata);
                documentMetadata.Metadata = batchResult.Metadata;

                if(entity as JsonDocument != null)
                {
                    var document = (JsonDocument) entity;
                    if (batchResult.Etag != null)
                    {
                        document.Etag = batchResult.Etag.Value;
                    }

                    documentMetadata.OriginalValue = document.ToJson();
                }
                else
                {
                    documentMetadata.OriginalValue = ConvertEntityToJson(entity, documentMetadata.Metadata);
                }
                

                TrySetIdentity(entity, batchResult.Key);

                if (stored != null)
                    stored(entity);
            }
        }

        /// <summary>
        /// Prepares for save changes.
        /// </summary>
        /// <returns></returns>
        protected SaveChangesData PrepareForSaveChanges()
        {
            var result = new SaveChangesData
                             {
                                 Entities = new List<object>(),
                                 Commands = new List<ICommandData>()
                             };

            DocumentMetadata value = null;
            foreach (string key in (from deletedEntity in deletedEntities
                                    where entitiesAndMetadata.TryGetValue(deletedEntity, out value)
                                    select value.Key))
            {
                Guid? etag = null;
                object existingEntity;
                DocumentMetadata metadata = null;
                if (entitiesByKey.TryGetValue(key, out existingEntity))
                {
                    if (entitiesAndMetadata.TryGetValue(existingEntity, out metadata))
                        etag = metadata.ETag;
                    entitiesAndMetadata.Remove(existingEntity);
                    entitiesByKey.Remove(key);
                }

                etag = UseOptimisticConcurrency ? etag : null;
                result.Entities.Add(existingEntity);

                result.Commands.Add(new DeleteCommandData
                                        {
                                            Etag = etag,
                                            Key = key,
                                        });
            }
            deletedEntities.Clear();
            foreach (var entity in entitiesAndMetadata.Where(pair => EntityChanged(pair.Key, pair.Value)))
            {
                result.Entities.Add(entity.Key);
                if (entity.Value.Key != null)
                    entitiesByKey.Remove(entity.Value.Key);
                result.Commands.Add(CreatePutEntityCommand(entity.Key, entity.Value));
            }

            return result;
        }

        /// <summary>
        /// Determines if the entity have changed.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <param name="documentMetadata">The document metadata.</param>
        /// <returns></returns>
        protected bool EntityChanged(object entity, DocumentMetadata documentMetadata)
        {
            if (documentMetadata == null)
                return true;

            var equalityComparer = new JTokenEqualityComparer();

            if(entity as JsonDocument != null)
            {
                var document = (JsonDocument) entity;

                return equalityComparer.Equals(document.ToJson(), documentMetadata.OriginalValue) == false ||
                       equalityComparer.Equals(documentMetadata.Metadata, documentMetadata.OriginalMetadata) == false;
            }

            JObject newObj = ConvertEntityToJson(entity, documentMetadata.Metadata);

            return equalityComparer.Equals(newObj, documentMetadata.OriginalValue) == false ||
                   equalityComparer.Equals(documentMetadata.Metadata, documentMetadata.OriginalMetadata) == false;
        }

        private JObject ConvertEntityToJson(object entity, JObject metadata)
        {
            Type entityType = entity.GetType();
            PropertyInfo identityProperty = documentStore.Conventions.GetIdentityProperty(entityType);

            JObject objectAsJson = GetObjectAsJson(entity);
            if (identityProperty != null)
            {
                objectAsJson.Remove(identityProperty.Name);
            }

            metadata["Raven-Clr-Type"] = JToken.FromObject(ReflectionUtil.GetFullNameWithoutVersionInformation(entityType));

            EntityToDocument entityConverted = OnEntityConverted;
            if (entityConverted != null)
                entityConverted(entity, objectAsJson, metadata);

            return objectAsJson;
        }

        private JObject GetObjectAsJson(object entity)
        {
            return JObject.FromObject(entity, Conventions.CreateSerializer());
        }


        /// <summary>
        /// Evicts the specified entity from the session.
        /// Remove the entity from the delete queue and stops tracking changes for this entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        public void Evict(JsonDocument entity)
        {
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value))
            {
                entitiesAndMetadata.Remove(entity);
                entitiesByKey.Remove(value.Key);
            }
            deletedEntities.Remove(entity);
        }

        /// <summary>
        /// Clears this instance.
        /// Remove all entities from the delete queue and stops tracking changes for all entities.
        /// </summary>
        public void Clear()
        {
            entitiesAndMetadata.Clear();
            deletedEntities.Clear();
            entitiesByKey.Clear();
        }
    }
}