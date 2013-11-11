//-----------------------------------------------------------------------
// <copyright file="InMemoryDocumentSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
#if !SILVERLIGHT && !NETFX_CORE
using System.Transactions;
#endif
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Linq;
using Raven.Client.Connection;
using Raven.Client.Document.DTC;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    /// <summary>
    /// Abstract implementation for in memory session operations
    /// </summary>
    public abstract class InMemoryDocumentSessionOperations : IDisposable
    {
        private static int counter;

        private readonly int hash = Interlocked.Increment(ref counter);

        protected bool GenerateDocumentKeysOnStore = true;
        /// <summary>
        /// The session id 
        /// </summary>
        public Guid Id { get; private set; }

        /// <summary>
        /// The database name for this session
        /// </summary>
        public string DatabaseName { get; internal set; }

        protected static readonly ILog log = LogManager.GetCurrentClassLogger();

        /// <summary>
        /// The entities waiting to be deleted
        /// </summary>
        protected readonly HashSet<object> deletedEntities = new HashSet<object>(ObjectReferenceEqualityComparer<object>.Default);

        /// <summary>
        /// Entities whose id we already know do not exists, because they are a missing include, or a missing load, etc.
        /// </summary>
        protected readonly HashSet<string> knownMissingIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private Dictionary<string, object> externalState;

        public IDictionary<string, object> ExternalState
        {
            get { return externalState ?? (externalState = new Dictionary<string, object>()); }
        }

#if !SILVERLIGHT
        private bool hasEnlisted;
        [ThreadStatic]
        private static Dictionary<string, HashSet<string>> _registeredStoresInTransaction;

        private static Dictionary<string, HashSet<string>> RegisteredStoresInTransaction
        {
            get { return (_registeredStoresInTransaction ?? (_registeredStoresInTransaction = new Dictionary<string, HashSet<string>>())); }
        }
#endif

        /// <summary>
        /// hold the data required to manage the data for RavenDB's Unit of Work
        /// </summary>
        protected readonly Dictionary<object, DocumentMetadata> entitiesAndMetadata =
            new Dictionary<object, DocumentMetadata>(ObjectReferenceEqualityComparer<object>.Default);

        /// <summary>
        /// Translate between a key and its associated entity
        /// </summary>
        protected readonly Dictionary<string, object> entitiesByKey = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        protected readonly string dbName;
        private readonly DocumentStoreBase documentStore;

        /// <summary>
        /// all the listeners for this session
        /// </summary>
        protected readonly DocumentSessionListeners listeners;

        ///<summary>
        /// The document store associated with this session
        ///</summary>
        public IDocumentStore DocumentStore
        {
            get { return documentStore; }
        }


        /// <summary>
        /// Gets the number of requests for this session
        /// </summary>
        /// <value></value>
        public int NumberOfRequests { get; private set; }

        /// <summary>
        /// Gets the number of entities held in memory to manage Unit of Work
        /// </summary>
        public int NumberOfEntitiesInUnitOfWork
        {
            get
            {
                return entitiesAndMetadata.Count;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryDocumentSessionOperations"/> class.
        /// </summary>
        protected InMemoryDocumentSessionOperations(
            string dbName,
            DocumentStoreBase documentStore,
            DocumentSessionListeners listeners,
            Guid id)
        {
            Id = id;
            this.dbName = dbName;
            this.documentStore = documentStore;
            this.listeners = listeners;
            ResourceManagerId = documentStore.ResourceManagerId;
            UseOptimisticConcurrency = false;
            AllowNonAuthoritativeInformation = true;
            NonAuthoritativeInformationTimeout = TimeSpan.FromSeconds(15);
            MaxNumberOfRequestsPerSession = documentStore.Conventions.MaxNumberOfRequestsPerSession;
            GenerateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(documentStore, GenerateKey);
            EntityToJson = new EntityToJson(documentStore, listeners);
        }

        /// <summary>
        /// Gets or sets the timeout to wait for authoritative information if encountered non authoritative document.
        /// </summary>
        /// <value></value>
        public TimeSpan NonAuthoritativeInformationTimeout { get; set; }

        /// <summary>
        /// Gets the store identifier for this session.
        /// The store identifier is the identifier for the particular RavenDB instance.
        /// </summary>
        /// <value>The store identifier.</value>
        public string StoreIdentifier
        {
            get { return documentStore.Identifier + ";" + DatabaseName; }
        }

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
        /// and raise <see cref="ConcurrencyException"/>.
        /// </summary>
        /// <value></value>
        public bool UseOptimisticConcurrency { get; set; }

        /// <summary>
        /// Gets the ETag for the specified entity.
        /// If the entity is transient, it will load the etag from the store
        /// and associate the current state of the entity with the etag from the server.
        /// </summary>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        public Etag GetEtagFor<T>(T instance)
        {
            return GetDocumentMetadata(instance).ETag;
        }

        /// <summary>
        /// Gets the metadata for the specified entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        public RavenJObject GetMetadataFor<T>(T instance)
        {
            return GetDocumentMetadata(instance).Metadata;
        }

        private DocumentMetadata GetDocumentMetadata<T>(T instance)
        {
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(instance, out value) == false)
            {
                string id;
                if (GenerateEntityIdOnTheClient.TryGetIdFromInstance(instance, out id)
                    || (instance is IDynamicMetaObjectProvider &&
                       GenerateEntityIdOnTheClient.TryGetIdFromDynamic(instance, out id))
)
                {
                    AssertNoNonUniqueInstance(instance, id);

                    var jsonDocument = GetJsonDocument(id);
                    entitiesByKey[id] = instance;
                    entitiesAndMetadata[instance] = value = new DocumentMetadata
                    {
                        ETag = UseOptimisticConcurrency ? Etag.Empty : null,
                        Key = id,
                        OriginalMetadata = jsonDocument.Metadata,
                        Metadata = (RavenJObject)jsonDocument.Metadata.CloneToken(),
                        OriginalValue = new RavenJObject()
                    };
                }
                else
                {
                    throw new InvalidOperationException("Could not find the document key for " + instance);
                }
            }
            return value;
        }

        /// <summary>
        /// Get the json document by key from the store
        /// </summary>
        protected abstract JsonDocument GetJsonDocument(string documentKey);

        /// <summary>
        /// Returns whatever a document with the specified id is loaded in the 
        /// current session
        /// </summary>
        public bool IsLoaded(string id)
        {
            return entitiesByKey.ContainsKey(id);
        }

        /// <summary>
        /// Returns whatever a document with the specified id is deleted 
        /// or known to be missing
        /// </summary>
        public bool IsDeleted(string id)
        {
            return knownMissingIds.Contains(id);
        }


        /// <summary>
        /// Gets the document id.
        /// </summary>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        public string GetDocumentId(object instance)
        {
            if (instance == null)
                return null;
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(instance, out value) == false)
                return null;
            return value.Key;
        }
        /// <summary>
        /// Gets a value indicating whether any of the entities tracked by the session has changes.
        /// </summary>
        /// <value></value>
        public bool HasChanges
        {
            get
            {
                return deletedEntities.Count > 0 ||
                        entitiesAndMetadata.Any(pair => EntityChanged(pair.Key, pair.Value));
            }
        }



        /// <summary>
        /// Determines whether the specified entity has changed.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns>
        /// 	<c>true</c> if the specified entity has changed; otherwise, <c>false</c>.
        /// </returns>
        public bool HasChanged(object entity)
        {
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
                return false;
            return EntityChanged(entity, value);
        }

        public void IncrementRequestCount()
        {
            if (++NumberOfRequests > MaxNumberOfRequestsPerSession)
                throw new InvalidOperationException(
                    string.Format(
                        @"The maximum number of requests ({0}) allowed for this session has been reached.
Raven limits the number of remote calls that a session is allowed to make as an early warning system. Sessions are expected to be short lived, and 
Raven provides facilities like Load(string[] keys) to load multiple documents at once and batch saves (call SaveChanges() only once).
You can increase the limit by setting DocumentConvention.MaxNumberOfRequestsPerSession or MaxNumberOfRequestsPerSession, but it is
advisable that you'll look into reducing the number of remote calls first, since that will speed up your application significantly and result in a 
more responsive application.
",
                        MaxNumberOfRequestsPerSession));
        }

        /// <summary>
        /// Tracks the entity inside the unit of work
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="documentFound">The document found.</param>
        /// <returns></returns>
        public T TrackEntity<T>(JsonDocument documentFound)
        {
            return (T)TrackEntity(typeof(T), documentFound);
        }

        /// <summary>
        /// Tracks the entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="document">The document.</param>
        /// <param name="metadata">The metadata.</param>'
        /// <param name="noTracking"></param>
        /// <returns></returns>
        public T TrackEntity<T>(string key, RavenJObject document, RavenJObject metadata, bool noTracking)
        {
            var entity = TrackEntity(typeof(T), key, document, metadata, noTracking);
            try
            {
                return (T)entity;
            }
            catch (InvalidCastException e)
            {
                var actual = typeof(T).Name;
                var expected = entity.GetType().Name;
                var message = string.Format("The query results type is '{0}' but you expected to get results of type '{1}'. If you want to return a projection, you should use .AsProjection<{1}>() before calling to .ToList().", expected, actual);
                throw new InvalidOperationException(message, e);
            }
        }

        /// <summary>
        /// Tracks the entity inside the unit of work
        /// </summary>
        /// <param name="entityType"></param>
        /// <param name="documentFound">The document found.</param>
        /// <returns></returns>
        public object TrackEntity(Type entityType, JsonDocument documentFound)
        {
            if (documentFound.NonAuthoritativeInformation.HasValue
                && documentFound.NonAuthoritativeInformation.Value
                && AllowNonAuthoritativeInformation == false)
            {
                throw new NonAuthoritativeInformationException("Document " + documentFound.Key +
                " returned Non Authoritative Information (probably modified by a transaction in progress) and AllowNonAuthoritativeInformation  is set to false");
            }
            if (documentFound.Metadata.Value<bool?>(Constants.RavenDocumentDoesNotExists) == true)
            {
                return GetDefaultValue(entityType); // document is not really there.
            }
            if (documentFound.Etag != null && !documentFound.Metadata.ContainsKey("@etag"))
            {
                documentFound.Metadata["@etag"] = documentFound.Etag.ToString();
            }
            if (!documentFound.Metadata.ContainsKey(Constants.LastModified))
            {
                documentFound.Metadata[Constants.LastModified] = documentFound.LastModified;
            }

            return TrackEntity(entityType, documentFound.Key, documentFound.DataAsJson, documentFound.Metadata, noTracking: false);
        }

        /// <summary>
        /// Tracks the entity.
        /// </summary>
		/// <param name="entityType">The entity type.</param>
        /// <param name="key">The key.</param>
        /// <param name="document">The document.</param>
        /// <param name="metadata">The metadata.</param>
        /// <returns></returns>
        object TrackEntity(Type entityType, string key, RavenJObject document, RavenJObject metadata, bool noTracking)
        {
            if (string.IsNullOrEmpty(key))
            {
                return JsonObjectToClrInstancesWithoutTracking(entityType, document);
            }
            document.Remove("@metadata");
            object entity;
            if (entitiesByKey.TryGetValue(key, out entity) == false)
            {
                entity = ConvertToEntity(entityType, key, document, metadata);
            }
            else
            {
                // the local instance may have been changed, we adhere to the current Unit of Work
                // instance, and return that, ignoring anything new.
                return entity;
            }
            var etag = metadata.Value<string>("@etag");
            if (metadata.Value<bool>("Non-Authoritative-Information") &&
                AllowNonAuthoritativeInformation == false)
            {
                throw new NonAuthoritativeInformationException("Document " + key +
                    " returned Non Authoritative Information (probably modified by a transaction in progress) and AllowNonAuthoritativeInformation  is set to false");
            }

            if (noTracking == false)
            {
                entitiesAndMetadata[entity] = new DocumentMetadata
                {
                    OriginalValue = document,
                    Metadata = metadata,
                    OriginalMetadata = (RavenJObject)metadata.CloneToken(),
                    ETag = HttpExtensions.EtagHeaderToEtag(etag),
                    Key = key
                };
                entitiesByKey[key] = entity;
            }

            return entity;
        }

        /// <summary>
        /// Converts the json document to an entity.
        /// </summary>
        /// <param name="entityType"></param>
        /// <param name="id">The id.</param>
        /// <param name="documentFound">The document found.</param>
        /// <param name="metadata">The metadata.</param>
        /// <returns></returns>
        object ConvertToEntity(Type entityType, string id, RavenJObject documentFound, RavenJObject metadata)
        {
            try
            {
                if (entityType == typeof (RavenJObject))
                    return documentFound.CloneToken();

                var defaultValue = GetDefaultValue(entityType);
                var entity = defaultValue;
                EnsureNotReadVetoed(metadata);

		        IDisposable disposable = null;
		        var defaultRavenContractResolver = Conventions.JsonContractResolver as DefaultRavenContractResolver;
		        if (defaultRavenContractResolver != null)
		        {
			        disposable = defaultRavenContractResolver.RegisterForExtensionData(RegisterMissingProperties);
		        }

		        using (disposable)
		        {
                var documentType = Conventions.GetClrType(id, documentFound, metadata);
                if (documentType != null)
                {
                    var type = Type.GetType(documentType);
                    if (type != null)
                        entity = documentFound.Deserialize(type, Conventions);
                }

                if (Equals(entity, defaultValue))
                {
                    entity = documentFound.Deserialize(entityType, Conventions);
                    var document = entity as RavenJObject;
                    if (document != null)
                    {
                        entity = (object) (new DynamicJsonObject(document));
                    }
                }
                GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);

                foreach (var documentConversionListener in listeners.ConversionListeners)
                {
                    documentConversionListener.DocumentToEntity(id, entity, documentFound, metadata);
                }

                return entity;
            }
	        }
            catch (ReadVetoException)
            {
                throw;
            }
            catch (Exception ex)
            {
		        throw new InvalidOperationException("Could not convert document " + id + " to entity of type " + entityType,
		                                            ex);
	        }
            }

		private void RegisterMissingProperties(object o, string key, JToken value)
		{
			Dictionary<string, JToken> dictionary;
			if (EntityToJson.MissingDictionary.TryGetValue(o, out dictionary) == false)
			{
				EntityToJson.MissingDictionary[o] = dictionary = new Dictionary<string, JToken>();
        }

			dictionary[key] = value;
		}

        /// <summary>
        /// Gets the default value of the specified type.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        static object GetDefaultValue(Type type)
        {
#if !NETFX_CORE
            return type.IsValueType ? Activator.CreateInstance(type) : null;
#else
            return null;
#endif
        }

        /// <summary>
        /// Gets or sets a value indicating whether non authoritative information is allowed.
        /// Non authoritative information is document that has been modified by a transaction that hasn't been committed.
        /// The server provides the latest committed version, but it is known that attempting to write to a non authoritative document
        /// will fail, because it is already modified.
        /// If set to <c>false</c>, the session will wait <see cref="NonAuthoritativeInformationTimeout"/> for the transaction to commit to get an
        /// authoritative information. If the wait is longer than <see cref="NonAuthoritativeInformationTimeout"/>, <see cref="NonAuthoritativeInformationException"/> is thrown.
        /// </summary>
        /// <value>
        /// 	<c>true</c> if non authoritative information is allowed; otherwise, <c>false</c>.
        /// </value>
        public bool AllowNonAuthoritativeInformation { get; set; }

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be deleted when SaveChanges is called.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The entity.</param>
        public void Delete<T>(T entity)
        {
            if (ReferenceEquals(entity, null)) throw new ArgumentNullException("entity");
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
                throw new InvalidOperationException(entity + " is not associated with the session, cannot delete unknown entity instance");
            if (value.OriginalMetadata.ContainsKey(Constants.RavenReadOnly) && value.OriginalMetadata.Value<bool>(Constants.RavenReadOnly))
                throw new InvalidOperationException(entity + " is marked as read only and cannot be deleted");
            deletedEntities.Add(entity);
            knownMissingIds.Add(value.Key);
        }


        /// <summary>
        /// Converts the json document to an entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <param name="documentFound">The document found.</param>
        /// <param name="metadata">The metadata.</param>
        /// <returns></returns>
        protected object ConvertToEntity<T>(string id, RavenJObject documentFound, RavenJObject metadata)
        {
            try
            {
		        if (typeof (T) == typeof (RavenJObject))
			        return (T) (object) documentFound.CloneToken();

                foreach (var extendedDocumentConversionListener in listeners.ExtendedConversionListeners)
                {
                    extendedDocumentConversionListener.BeforeConversionToEntity(id, documentFound, metadata);
                }

                var entity = default(T);
                EnsureNotReadVetoed(metadata);

		        IDisposable disposable = null;
		        var defaultRavenContractResolver = Conventions.JsonContractResolver as DefaultRavenContractResolver;
		        if (defaultRavenContractResolver != null)
		        {
			        disposable = defaultRavenContractResolver.RegisterForExtensionData(RegisterMissingProperties);
		        }
		        using (disposable)
		        {

                var documentType = Conventions.GetClrType(id, documentFound, metadata);
                if (documentType != null)
                {
                    var type = Type.GetType(documentType);
                    if (type != null)
					        entity = (T) documentFound.Deserialize(type, Conventions);
                }
                if (Equals(entity, default(T)))
                {
                    entity = documentFound.Deserialize<T>(Conventions);
                    var document = entity as RavenJObject;
                    if (document != null)
                    {
					        entity = (T) (object) (new DynamicJsonObject(document));
                    }
                }
                GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);

                foreach (var documentConversionListener in listeners.ConversionListeners)
                {
                    documentConversionListener.DocumentToEntity(id, entity, documentFound, metadata);
                }

                foreach (var extendedDocumentConversionListener in listeners.ExtendedConversionListeners)
                {
                    extendedDocumentConversionListener.AfterConversionToEntity(id, documentFound, metadata, entity);
                }

                return entity;
            }
	        }
            catch (Exception ex)
            {
		        throw new InvalidOperationException("Could not convert document " + id + " to entity of type " + typeof (T),
		                                            ex);
            }
        }

        private static void EnsureNotReadVetoed(RavenJObject metadata)
        {
            var readVeto = metadata["Raven-Read-Veto"] as RavenJObject;
            if (readVeto == null)
                return;

            var s = readVeto.Value<string>("Reason");
            throw new ReadVetoException(
                "Document could not be read because of a read veto." + Environment.NewLine +
                "The read was vetoed by: " + readVeto.Value<string>("Trigger") + Environment.NewLine +
                "Veto reason: " + s
                );
        }

        /// <summary>
        /// Stores the specified entity in the session. The entity will be saved when SaveChanges is called.
        /// </summary>
        public void Store(object entity)
        {
            string id;
            var hasId = GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id);
            StoreInternal(entity, null, null, forceConcurrencyCheck: hasId == false);
        }

        /// <summary>
        /// Stores the specified entity in the session. The entity will be saved when SaveChanges is called.
        /// </summary>
        public void Store(object entity, Etag etag)
        {
            StoreInternal(entity, etag, null, forceConcurrencyCheck: true);
        }

        /// <summary>
        /// Stores the specified entity in the session, explicitly specifying its Id. The entity will be saved when SaveChanges is called.
        /// </summary>
        public void Store(object entity, string id)
        {
            StoreInternal(entity, null, id, forceConcurrencyCheck: false);
        }

        /// <summary>
        /// Stores the specified entity in the session, explicitly specifying its Id. The entity will be saved when SaveChanges is called.
        /// </summary>
        public void Store(object entity, Etag etag, string id)
        {
            StoreInternal(entity, etag, id, forceConcurrencyCheck: true);
        }

        private void StoreInternal(object entity, Etag etag, string id, bool forceConcurrencyCheck)
        {
            if (null == entity)
                throw new ArgumentNullException("entity");

            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value))
            {
                value.ETag = etag ?? value.ETag;
                value.ForceConcurrencyCheck = forceConcurrencyCheck;
                return;
            }

            if (id == null)
            {
                if (GenerateDocumentKeysOnStore)
                {
                    id = GenerateEntityIdOnTheClient.GenerateDocumentKeyForStorage(entity);
                }
                else
                {
                    RememberEntityForDocumentKeyGeneration(entity);
                }
            }
            else
            {
                // Store it back into the Id field so the client has access to to it                    
                GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);
            }

            // we make the check here even if we just generated the key
            // users can override the key generation behavior, and we need
            // to detect if they generate duplicates.
            AssertNoNonUniqueInstance(entity, id);

            var metadata = new RavenJObject();
            var tag = documentStore.Conventions.GetTypeTagName(entity.GetType());
            if (tag != null)
                metadata.Add(Constants.RavenEntityName, tag);
            if (id != null)
                knownMissingIds.Remove(id);
            StoreEntityInUnitOfWork(id, entity, etag, metadata, forceConcurrencyCheck);
        }

        public Task StoreAsync(object entity)
        {
            string id;
            var hasId = GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id);

            return StoreAsyncInternal(entity, null, null, forceConcurrencyCheck: hasId == false);
        }

        public Task StoreAsync(object entity, Etag etag)
        {
            return StoreAsyncInternal(entity, etag, null, forceConcurrencyCheck: true);
        }

        public Task StoreAsync(object entity, Etag etag, string id)
        {
            return StoreAsyncInternal(entity, etag, id, forceConcurrencyCheck: true);
        }

        public Task StoreAsync(object entity, string id)
        {
            return StoreAsyncInternal(entity, null, id, forceConcurrencyCheck: false);
        }

		private async Task StoreAsyncInternal(object entity, Etag etag, string id, bool forceConcurrencyCheck)
        {
            if (null == entity)
                throw new ArgumentNullException("entity");

            if (id == null)
            {
				id = await GenerateDocumentKeyForStorageAsync(entity);
            }

            StoreInternal(entity, etag, id, forceConcurrencyCheck);
        }

        protected abstract string GenerateKey(object entity);

        protected virtual void RememberEntityForDocumentKeyGeneration(object entity)
        {
            throw new NotImplementedException("You cannot set GenerateDocumentKeysOnStore to false without implementing RememberEntityForDocumentKeyGeneration");
        }

		protected internal async Task<string> GenerateDocumentKeyForStorageAsync(object entity)
        {
            if (entity is IDynamicMetaObjectProvider)
            {
                string id;
                if (GenerateEntityIdOnTheClient.TryGetIdFromDynamic(entity, out id) || id == null)
					return id;

				var key = await GenerateKeyAsync(entity);
                            // If we generated a new id, store it back into the Id field so the client has access to to it                    
				if (key != null)
					GenerateEntityIdOnTheClient.TrySetIdOnDynamic(entity, key);
				return key;
            }

			var result = await GetOrGenerateDocumentKeyAsync(entity);
			GenerateEntityIdOnTheClient.TrySetIdentity(entity, result);
			return result;
        }

        protected abstract Task<string> GenerateKeyAsync(object entity);

        protected virtual void StoreEntityInUnitOfWork(string id, object entity, Etag etag, RavenJObject metadata, bool forceConcurrencyCheck)
        {
            entitiesAndMetadata.Add(entity, new DocumentMetadata
            {
                Key = id,
                Metadata = metadata,
                OriginalMetadata = new RavenJObject(),
                ETag = etag,
                OriginalValue = new RavenJObject(),
                ForceConcurrencyCheck = forceConcurrencyCheck
            });
            if (id != null)
                entitiesByKey[id] = entity;
        }

        protected virtual void AssertNoNonUniqueInstance(object entity, string id)
        {
            if (id == null || id.EndsWith("/") || !entitiesByKey.ContainsKey(id) || ReferenceEquals(entitiesByKey[id], entity))
                return;

            throw new NonUniqueObjectException("Attempted to associate a different object with id '" + id + "'.");
        }



		protected async Task<string> GetOrGenerateDocumentKeyAsync(object entity)
        {
            string id;
            GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id);

            Task<string> generator =
                id != null
                ? CompletedTask.With(id)
                : GenerateKeyAsync(entity);

			var result = await generator;
			if (result != null && result.StartsWith("/"))
                    throw new InvalidOperationException("Cannot use value '" + id + "' as a document id because it begins with a '/'");

			return result;
        }

        /// <summary>
        /// Creates the put entity command.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <param name="documentMetadata">The document metadata.</param>
        /// <returns></returns>
        protected ICommandData CreatePutEntityCommand(object entity, DocumentMetadata documentMetadata)
        {
            string id;
            if (GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id) &&
                documentMetadata.Key != null &&
                documentMetadata.Key.Equals(id, StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException("Entity " + entity.GetType().FullName + " had document key '" +
                                                    documentMetadata.Key + "' but now has document key property '" + id + "'." +
                                                    Environment.NewLine +
                                                    "You cannot change the document key property of a entity loaded into the session");
            }

            var json = EntityToJson.ConvertEntityToJson(documentMetadata.Key, entity, documentMetadata.Metadata);

            var etag = UseOptimisticConcurrency || documentMetadata.ForceConcurrencyCheck
                           ? (documentMetadata.ETag ?? Etag.Empty)
                           : null;

            return new PutCommandData
            {
                Document = json,
                Etag = etag,
                Key = documentMetadata.Key,
                Metadata = (RavenJObject)documentMetadata.Metadata.CloneToken(),
            };
        }

        /// <summary>
        /// Updates the batch results.
        /// </summary>
        protected void UpdateBatchResults(IList<BatchResult> batchResults, SaveChangesData saveChangesData)
        {
#if !SILVERLIGHT && !NETFX_CORE
            if (documentStore.HasJsonRequestFactory && Conventions.ShouldSaveChangesForceAggressiveCacheCheck && batchResults.Count != 0)
            {
                documentStore.JsonRequestFactory.ExpireItemsFromCache(DatabaseName ?? Constants.SystemDatabase);
            }
#endif
            for (var i = saveChangesData.DeferredCommandsCount; i < batchResults.Count; i++)
            {
                var batchResult = batchResults[i];
                if (batchResult.Method != "PUT")
                    continue;

                var entity = saveChangesData.Entities[i - saveChangesData.DeferredCommandsCount];
                DocumentMetadata documentMetadata;
                if (entitiesAndMetadata.TryGetValue(entity, out documentMetadata) == false)
                    continue;

				batchResult.Metadata["@etag"] = new RavenJValue((string)batchResult.Etag);
				entitiesByKey[batchResult.Key] = entity;
				documentMetadata.ETag = batchResult.Etag;
				documentMetadata.Key = batchResult.Key;
				documentMetadata.OriginalMetadata = (RavenJObject)batchResult.Metadata.CloneToken();
				documentMetadata.Metadata = batchResult.Metadata;
				documentMetadata.OriginalValue = EntityToJson.ConvertEntityToJson(documentMetadata.Key, entity, documentMetadata.Metadata);

                GenerateEntityIdOnTheClient.TrySetIdentity(entity, batchResult.Key);

                foreach (var documentStoreListener in listeners.StoreListeners)
                {
                    documentStoreListener.AfterStore(batchResult.Key, entity, batchResult.Metadata);
                }
            }

            var lastPut = batchResults.LastOrDefault(x => x.Method == "PUT");
            if (lastPut == null)
                return;

            documentStore.LastEtagHolder.UpdateLastWrittenEtag(lastPut.Etag);
        }

        /// <summary>
        /// Prepares for save changes.
        /// </summary>
        /// <returns></returns>
        protected SaveChangesData PrepareForSaveChanges()
        {
            EntityToJson.CachedJsonDocs.Clear();
            var result = new SaveChangesData
            {
                Entities = new List<object>(),
                Commands = new List<ICommandData>(deferedCommands),
                DeferredCommandsCount = deferedCommands.Count
            };
            deferedCommands.Clear();

#if !SILVERLIGHT && !NETFX_CORE
            if (documentStore.EnlistInDistributedTransactions)
                TryEnlistInAmbientTransaction();
#endif
            PrepareForEntitiesDeletion(result);
            PrepareForEntitiesPuts(result);

            return result;
        }

        private void PrepareForEntitiesPuts(SaveChangesData result)
        {
            foreach (var entity in entitiesAndMetadata.Where(pair => EntityChanged(pair.Key, pair.Value)).ToArray())
            {
                foreach (var documentStoreListener in listeners.StoreListeners)
                {
                    if (documentStoreListener.BeforeStore(entity.Value.Key, entity.Key, entity.Value.Metadata, entity.Value.OriginalValue))
                        EntityToJson.CachedJsonDocs.Remove(entity.Key);
                }
                result.Entities.Add(entity.Key);
                if (entity.Value.Key != null)
                    entitiesByKey.Remove(entity.Value.Key);
                result.Commands.Add(CreatePutEntityCommand(entity.Key, entity.Value));
            }
        }

        private void PrepareForEntitiesDeletion(SaveChangesData result)
        {
            DocumentMetadata value = null;

            var keysToDelete = (from deletedEntity in deletedEntities
                                where entitiesAndMetadata.TryGetValue(deletedEntity, out value)
                                // skip deleting read only entities
                                where !value.OriginalMetadata.ContainsKey(Constants.RavenReadOnly) ||
                                      !value.OriginalMetadata.Value<bool>(Constants.RavenReadOnly)
                                select value.Key).ToList();

            foreach (var key in keysToDelete)
            {
                Etag etag = null;
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

                foreach (var deleteListener in listeners.DeleteListeners)
                {
                    deleteListener.BeforeDelete(key, existingEntity, metadata != null ? metadata.Metadata : null);
                }

                result.Commands.Add(new DeleteCommandData
                {
                    Etag = etag,
                    Key = key,
                });
            }
            deletedEntities.Clear();
        }

#if !SILVERLIGHT && !NETFX_CORE
        protected virtual void TryEnlistInAmbientTransaction()
        {

            if (hasEnlisted || Transaction.Current == null)
                return;

            HashSet<string> registered;
            var localIdentifier = Transaction.Current.TransactionInformation.LocalIdentifier;
            if (RegisteredStoresInTransaction.TryGetValue(localIdentifier, out registered) == false)
            {
                RegisteredStoresInTransaction[localIdentifier] =
                    registered = new HashSet<string>();
            }

            if (registered.Add(StoreIdentifier))
            {
                var transactionalSession = (ITransactionalDocumentSession)this;
                var ravenClientEnlistment = new RavenClientEnlistment(documentStore, transactionalSession, () =>
                    {
                        RegisteredStoresInTransaction.Remove(localIdentifier);
                        if (documentStore.WasDisposed)
                            throw new ObjectDisposedException("RavenDB Session");
                    });
                if (documentStore.TransactionRecoveryStorage is VolatileOnlyTransactionRecoveryStorage)
                    Transaction.Current.EnlistVolatile(ravenClientEnlistment, EnlistmentOptions.None);
                else
                    Transaction.Current.EnlistDurable(ResourceManagerId, ravenClientEnlistment, EnlistmentOptions.None);
            }
            hasEnlisted = true;
        }
#endif


        /// <summary>
        /// Mark the entity as read only, change tracking won't apply 
        /// to such an entity. This can be done as an optimization step, so 
        /// we don't need to check the entity for changes.
        /// This flag is persisted in the document metadata and subsequent modifications of the document will not be possible.
        /// If you want the session to ignore this entity, consider using the Evict() method.
        /// </summary>
        public void MarkReadOnly(object entity)
        {
            GetMetadataFor(entity)[Constants.RavenReadOnly] = true;
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

            string id;
            if (GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id) &&
                string.Equals(documentMetadata.Key, id, StringComparison.OrdinalIgnoreCase) == false)
                return true;

            // prevent saves of a modified read only entity
            if (documentMetadata.OriginalMetadata.ContainsKey(Constants.RavenReadOnly) &&
                documentMetadata.OriginalMetadata.Value<bool>(Constants.RavenReadOnly) &&
                documentMetadata.Metadata.ContainsKey(Constants.RavenReadOnly) &&
                documentMetadata.Metadata.Value<bool>(Constants.RavenReadOnly))
                return false;

            var newObj = EntityToJson.ConvertEntityToJson(documentMetadata.Key, entity, documentMetadata.Metadata);
            return RavenJToken.DeepEquals(newObj, documentMetadata.OriginalValue) == false ||
                RavenJToken.DeepEquals(documentMetadata.Metadata, documentMetadata.OriginalMetadata) == false;
        }

        /// <summary>
        /// Evicts the specified entity from the session.
        /// Remove the entity from the delete queue and stops tracking changes for this entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The entity.</param>
        public void Evict<T>(T entity)
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
            knownMissingIds.Clear();
        }

        private readonly List<ICommandData> deferedCommands = new List<ICommandData>();
        public GenerateEntityIdOnTheClient GenerateEntityIdOnTheClient { get; private set; }
        public EntityToJson EntityToJson { get; private set; }

        /// <summary>
        /// Defer commands to be executed on SaveChanges()
        /// </summary>
        /// <param name="commands">The commands to be executed</param>
        public virtual void Defer(params ICommandData[] commands)
        {
            deferedCommands.AddRange(commands);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public virtual void Dispose()
        {
        }

        /// <summary>
        /// Commits the specified tx id.
        /// </summary>
        /// <param name="txId">The tx id.</param>
        public abstract void Commit(string txId);
        /// <summary>
        /// Rollbacks the specified tx id.
        /// </summary>
        /// <param name="txId">The tx id.</param>
        public abstract void Rollback(string txId);

#if !SILVERLIGHT
        /// <summary>
        /// Clears the enlistment.
        /// </summary>
        protected void ClearEnlistment()
        {
            hasEnlisted = false;
        }
#endif
        /// <summary>
        /// Metadata held about an entity by the session
        /// </summary>
        public class DocumentMetadata
        {
            /// <summary>
            /// Gets or sets the original value.
            /// </summary>
            /// <value>The original value.</value>
            public RavenJObject OriginalValue { get; set; }
            /// <summary>
            /// Gets or sets the metadata.
            /// </summary>
            /// <value>The metadata.</value>
            public RavenJObject Metadata { get; set; }
            /// <summary>
            /// Gets or sets the ETag.
            /// </summary>
            /// <value>The ETag.</value>
            public Etag ETag { get; set; }
            /// <summary>
            /// Gets or sets the key.
            /// </summary>
            /// <value>The key.</value>
            public string Key { get; set; }
            /// <summary>
            /// Gets or sets the original metadata.
            /// </summary>
            /// <value>The original metadata.</value>
            public RavenJObject OriginalMetadata { get; set; }

            /// <summary>
            /// A concurrency check will be forced on this entity 
            /// even if UseOptimisticConcurrency is set to false
            /// </summary>
            public bool ForceConcurrencyCheck { get; set; }
        }

        /// <summary>
        /// Data for a batch command to the server
        /// </summary>
        public class SaveChangesData
        {
            public SaveChangesData()
            {
                Commands = new List<ICommandData>();
                Entities = new List<object>();
            }

            /// <summary>
            /// Gets or sets the commands.
            /// </summary>
            /// <value>The commands.</value>
            public List<ICommandData> Commands { get; set; }

            public int DeferredCommandsCount { get; set; }

            /// <summary>
            /// Gets or sets the entities.
            /// </summary>
            /// <value>The entities.</value>
            public IList<object> Entities { get; set; }
        }

        protected void LogBatch(SaveChangesData data)
        {
            log.Debug(() =>
            {
                var sb = new StringBuilder()
                    .AppendFormat("Saving {0} changes to {1}", data.Commands.Count, StoreIdentifier)
                    .AppendLine();
                foreach (var commandData in data.Commands)
                {
                    sb.AppendFormat("\t{0} {1}", commandData.Method, commandData.Key).AppendLine();
                }
                return sb.ToString();
            });
        }

        public void RegisterMissing(string id)
        {
            knownMissingIds.Add(id);
        }

        public void RegisterMissingIncludes(IEnumerable<RavenJObject> results, ICollection<string> includes)
        {
            if (includes == null || includes.Any() == false)
                return;

            foreach (var result in results)
            {
                foreach (var include in includes)
                {
                    IncludesUtil.Include(result, include, id =>
                    {
                        if (id == null)
                            return false;
                        if (IsLoaded(id) == false)
                        {
                            RegisterMissing(id);
                            return false;
                        }
                        return true;
                    });
                }
            }
        }

        public override int GetHashCode()
        {
            return hash;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(obj, this);
        }

        internal void HandleInternalMetadata(RavenJObject result)
        {
            // Implant a property with "id" value ... if not exists
            var metadata = result.Value<RavenJObject>("@metadata");
            if (metadata == null || string.IsNullOrEmpty(metadata.Value<string>("@id")))
            {
                // if the item has metadata, then nested items will not have it, so we can skip recursing down
                foreach (var nested in result.Select(property => property.Value))
                {
                    var jObject = nested as RavenJObject;
                    if (jObject != null)
                        HandleInternalMetadata(jObject);
                    var jArray = nested as RavenJArray;
                    if (jArray == null)
                        continue;
                    foreach (var item in jArray.OfType<RavenJObject>())
                    {
                        HandleInternalMetadata(item);
                    }
                }
                return;
            }

            var entityName = metadata.Value<string>(Constants.RavenEntityName);

            var idPropName = Conventions.FindIdentityPropertyNameFromEntityName(entityName);
            if (result.ContainsKey(idPropName))
                return;

            result[idPropName] = new RavenJValue(metadata.Value<string>("@id"));
        }

        protected object JsonObjectToClrInstancesWithoutTracking(Type type, RavenJObject val)
        {
            if (val == null)
                return null;
            if (type.IsArray)
            {
                // Returns array, public APIs don't surface that yet though as we only support Transform
                // With a single Id
                var elementType = type.GetElementType();
                var array = val.Value<RavenJArray>("$values").Cast<RavenJObject>()
                               .Where(x => x != null)
                               .Select(y =>
                               {
                                   HandleInternalMetadata(y);

                                   return ProjectionToInstance(y, elementType);
                               })
                               .ToArray();

                var newArray = Array.CreateInstance(elementType, array.Length);
                Array.Copy(array, newArray, array.Length);
                return newArray;
            }

            var items = (val.Value<RavenJArray>("$values") ?? new RavenJArray(val))
                .Select(JsonExtensions.ToJObject)
                .Where(x => x != null)
                .Select(x =>
                {
                    HandleInternalMetadata(x);
                    return ProjectionToInstance(x, type);
                })
                .ToArray();

            if (items.Length == 1)
                return items[0];

            return items;
        }

        private object ProjectionToInstance(RavenJObject y, Type type)
        {
            foreach (var conversionListener in listeners.ExtendedConversionListeners)
            {
                conversionListener.BeforeConversionToEntity(null, y, null);
            }
            var instance = y.Deserialize(type, Conventions);
            foreach (var conversionListener in listeners.ConversionListeners)
            {
                conversionListener.DocumentToEntity(null, instance, y, null);
            }
            foreach (var conversionListener in listeners.ExtendedConversionListeners)
            {
                conversionListener.AfterConversionToEntity(null, y, null, instance);
            }
            return instance;
        }

    }
}
