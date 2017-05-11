//-----------------------------------------------------------------------
// <copyright file="InMemoryDocumentSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.Session;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Abstract implementation for in memory session operations
    /// </summary>
    public abstract class InMemoryDocumentSessionOperations : IDisposable
    {
        protected readonly RequestExecutor _requestExecutor;
        private readonly IDisposable _releaseOperationContext;
        private readonly JsonOperationContext _context;
        protected readonly List<ILazyOperation> PendingLazyOperations = new List<ILazyOperation>();
        protected readonly Dictionary<ILazyOperation, Action<object>> OnEvaluateLazy = new Dictionary<ILazyOperation, Action<object>>();
        private static int _instancesCounter;
        private readonly int _hash = Interlocked.Increment(ref _instancesCounter);
        protected bool GenerateDocumentKeysOnStore = true;
        private BatchOptions _saveChangesOptions;

        /// <summary>
        /// The session id 
        /// </summary>
        public Guid Id { get; private set; }

        /// <summary>
        /// The entities waiting to be deleted
        /// </summary>
        protected readonly HashSet<object> DeletedEntities = new HashSet<object>(ObjectReferenceEqualityComparer<object>.Default);

        public event EventHandler<BeforeStoreEventArgs> OnBeforeStore;
        public event EventHandler<AfterStoreEventArgs> OnAfterStore;
        public event EventHandler<BeforeDeleteEventArgs> OnBeforeDelete;
        public event EventHandler<BeforeQueryExecutedEventArgs> OnBeforeQueryExecuted;

        /// <summary>
        /// Entities whose id we already know do not exists, because they are a missing include, or a missing load, etc.
        /// </summary>
        protected readonly HashSet<string> KnownMissingIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private Dictionary<string, object> _externalState;

        public IDictionary<string, object> ExternalState => _externalState ?? (_externalState = new Dictionary<string, object>());

        /// <summary>
        /// Translate between a key and its associated entity
        /// </summary>
        internal readonly DocumentsById DocumentsById = new DocumentsById();

        /// <summary>
        /// Translate between a key and its associated entity
        /// </summary>
        internal readonly Dictionary<string, DocumentInfo> IncludedDocumentsByKey = new Dictionary<string, DocumentInfo>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// hold the data required to manage the data for RavenDB's Unit of Work
        /// </summary>
        protected internal readonly Dictionary<object, DocumentInfo> DocumentsByEntity = new Dictionary<object, DocumentInfo>(ObjectReferenceEqualityComparer<object>.Default);

        protected readonly DocumentStoreBase _documentStore;

        public string DatabaseName { get; }

        ///<summary>
        /// The document store associated with this session
        ///</summary>
        public IDocumentStore DocumentStore => _documentStore;

        public RequestExecutor RequestExecutor => _requestExecutor;

        public JsonOperationContext Context => _context;
        /// <summary>
        /// Gets the number of requests for this session
        /// </summary>
        /// <value></value>
        public int NumberOfRequests { get; private set; }

        /// <summary>
        /// Gets the number of entities held in memory to manage Unit of Work
        /// </summary>
        public int NumberOfEntitiesInUnitOfWork => DocumentsByEntity.Count;

        /// <summary>
        /// Gets the store identifier for this session.
        /// The store identifier is the identifier for the particular RavenDB instance.
        /// </summary>
        /// <value>The store identifier.</value>
        public string StoreIdentifier => _documentStore.Identifier + ";" + DatabaseName;

        /// <summary>
        /// Gets the conventions used by this session
        /// </summary>
        /// <value>The conventions.</value>
        /// <remarks>
        /// This instance is shared among all sessions, changes to the <see cref="DocumentConventions"/> should be done
        /// via the <see cref="IDocumentStore"/> instance, not on a single session.
        /// </remarks>
        public DocumentConventions Conventions => DocumentStore.Conventions;

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

        protected readonly List<ICommandData> _deferedCommands = new List<ICommandData>();
        public GenerateEntityIdOnTheClient GenerateEntityIdOnTheClient { get; }
        public EntityToBlittable EntityToBlittable { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryDocumentSessionOperations"/> class.
        /// </summary>
        protected InMemoryDocumentSessionOperations(
            string databaseName,
            DocumentStoreBase documentStore,
            RequestExecutor requestExecutor,
            Guid id)
        {
            Id = id;
            DatabaseName = databaseName;
            _documentStore = documentStore;
            _requestExecutor = requestExecutor;
            _releaseOperationContext = requestExecutor.ContextPool.AllocateOperationContext(out _context);
            UseOptimisticConcurrency = documentStore.Conventions.UseOptimisticConcurrency;
            MaxNumberOfRequestsPerSession = documentStore.Conventions.MaxNumberOfRequestsPerSession;
            GenerateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(documentStore.Conventions, GenerateKey);
            EntityToBlittable = new EntityToBlittable(this);
        }

        /// <summary>
        /// Gets the ETag for the specified entity.
        /// If the entity is transient, it will load the etag from the store
        /// and associate the current state of the entity with the etag from the server.
        /// </summary>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        public long? GetEtagFor<T>(T instance)
        {
            return GetDocumentInfo(instance).ETag;
        }

        /// <summary>
        /// Gets the metadata for the specified entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        public IMetadataDictionary GetMetadataFor<T>(T instance)
        {
            if (instance == null)
                throw new ArgumentNullException(nameof(instance));

            var documentInfo = GetDocumentInfo(instance);

            if (documentInfo.MetadataInstance != null)
                return documentInfo.MetadataInstance;

            var metadataAsBlittable = documentInfo.Metadata;
            var metadata = new MetadataAsDictionary(metadataAsBlittable);
            documentInfo.MetadataInstance = metadata;
            return metadata;
        }

        public ChangeVectorEntry[] GetChangeVectorFor<T>(T instance)
        {
            if(instance == null)
                throw new ArgumentNullException(nameof(instance));

            var documentInfo = GetDocumentInfo(instance);
            BlittableJsonReaderArray changeVectorJson;
            if (documentInfo.Metadata.TryGet(Constants.Documents.Metadata.ChangeVector, out changeVectorJson))
                return changeVectorJson.ToVector();

            return new ChangeVectorEntry[0];
        }

        private DocumentInfo GetDocumentInfo<T>(T instance)
        {
            DocumentInfo documentInfo;
            string id;

            if (DocumentsByEntity.TryGetValue(instance, out documentInfo))
                return documentInfo;

            if (GenerateEntityIdOnTheClient.TryGetIdFromInstance(instance, out id) == false && (instance is IDynamicMetaObjectProvider == false || GenerateEntityIdOnTheClient.TryGetIdFromDynamic(instance, out id) == false))
                throw new InvalidOperationException("Could not find the document id for " + instance);

            AssertNoNonUniqueInstance(instance, id);

            throw new ArgumentException("Document " + id + " doesn't exist in the session");
        }

        /// <summary>
        /// Returns whether a document with the specified id is loaded in the 
        /// current session
        /// </summary>
        public bool IsLoaded(string id)
        {
            return IsLoadedOrDeleted(id);
        }

        internal bool IsLoadedOrDeleted(string id)
        {
            return (DocumentsById.TryGetValue(id, out DocumentInfo documentInfo) && documentInfo.Document != null) || 
                IsDeleted(id) || 
                IncludedDocumentsByKey.ContainsKey(id);
        }

        /// <summary>
        /// Returns whether a document with the specified id is deleted 
        /// or known to be missing
        /// </summary>
        public bool IsDeleted(string id)
        {
            return KnownMissingIds.Contains(id);
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
            DocumentInfo value;
            if (DocumentsByEntity.TryGetValue(instance, out value) == false)
                return null;
            return value.Id;
        }

        public void IncrementRequestCount()
        {
            if (++NumberOfRequests > MaxNumberOfRequestsPerSession)
                throw new InvalidOperationException($@"The maximum number of requests ({MaxNumberOfRequestsPerSession}) allowed for this session has been reached.
Raven limits the number of remote calls that a session is allowed to make as an early warning system. Sessions are expected to be short lived, and 
Raven provides facilities like Load(string[] keys) to load multiple documents at once and batch saves (call SaveChanges() only once).
You can increase the limit by setting DocumentConventions.MaxNumberOfRequestsPerSession or MaxNumberOfRequestsPerSession, but it is
advisable that you'll look into reducing the number of remote calls first, since that will speed up your application significantly and result in a 
more responsive application.
");
        }

        /// <summary>
        /// Tracks the entity inside the unit of work
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="documentFound">The document found.</param>
        /// <returns></returns>
        public T TrackEntity<T>(DocumentInfo documentFound)
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
        public T TrackEntity<T>(string key, BlittableJsonReaderObject document, BlittableJsonReaderObject metadata, bool noTracking)
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
                var message = string.Format("The query results type is '{0}' but you expected to get results of type '{1}'. " +
"If you want to return a projection, you should use .ProjectFromIndexFieldsInto<{1}>() (for Query) or .SelectFields<{1}>() (for DocumentQuery) before calling to .ToList().", expected, actual);
                throw new InvalidOperationException(message, e);
            }
        }

        /// <summary>
        /// Tracks the entity inside the unit of work
        /// </summary>
        /// <param name="entityType"></param>
        /// <param name="documentFound">The document found.</param>
        /// <returns></returns>
        public object TrackEntity(Type entityType, DocumentInfo documentFound)
        {
            return TrackEntity(entityType, documentFound.Id, documentFound.Document, documentFound.Metadata, noTracking: false);
        }

        /// <summary>
        /// Tracks the entity.
        /// </summary>
        /// <param name="entityType">The entity type.</param>
        /// <param name="key">The key.</param>
        /// <param name="document">The document.</param>
        /// <param name="metadata">The metadata.</param>
        /// <param name="noTracking">Entity tracking is enabled if true, disabled otherwise.</param>
        /// <returns></returns>
        private object TrackEntity(Type entityType, string key, BlittableJsonReaderObject document, BlittableJsonReaderObject metadata, bool noTracking)
        {
            if (string.IsNullOrEmpty(key))
            {
                return DeserializeFromTransformer(entityType, null, document);
            }

            DocumentInfo docInfo;
            if (DocumentsById.TryGetValue(key, out docInfo))
            {
                // the local instance may have been changed, we adhere to the current Unit of Work
                // instance, and return that, ignoring anything new.
                if (docInfo.Entity == null)
                    docInfo.Entity = ConvertToEntity(entityType, key, document);

                if (noTracking == false)
                {
                    IncludedDocumentsByKey.Remove(key);
                    DocumentsByEntity[docInfo.Entity] = docInfo;
                }
                return docInfo.Entity;
            }

            if (IncludedDocumentsByKey.TryGetValue(key, out docInfo))
            {
                if (docInfo.Entity == null)
                    docInfo.Entity = ConvertToEntity(entityType, key, document);

                if (noTracking == false)
                {
                    IncludedDocumentsByKey.Remove(key);
                    DocumentsById.Add(docInfo);
                    DocumentsByEntity[docInfo.Entity] = docInfo;
                }
                return docInfo.Entity;
            }

            var entity = ConvertToEntity(entityType, key, document);

            long etag;
            if (metadata.TryGet(Constants.Documents.Metadata.Etag, out etag) == false)
                throw new InvalidOperationException("Document must have an ETag");

            if (noTracking == false)
            {
                var newDocumentInfo = new DocumentInfo
                {
                    Id = key,
                    Document = document,
                    Metadata = metadata,
                    Entity = entity,
                    ETag = etag
                };

                DocumentsById.Add(newDocumentInfo);
                DocumentsByEntity[entity] = newDocumentInfo;
            }

            return entity;
        }

        /// <summary>
        /// Converts the json document to an entity.
        /// </summary>
        /// <param name="entityType"></param>
        /// <param name="id">The id.</param>
        /// <param name="documentFound">The document found.</param>
        /// <returns></returns>
        public object ConvertToEntity(Type entityType, string id, BlittableJsonReaderObject documentFound)
        {
            //TODO: consider removing this function entirely, leaving only the EntityToBlittalbe version
            return EntityToBlittable.ConvertToEntity(entityType, id, documentFound);
        }

        private void RegisterMissingProperties(object o, string key, object value)
        {
            Dictionary<string, object> dictionary;
            if (EntityToBlittable.MissingDictionary.TryGetValue(o, out dictionary) == false)
            {
                EntityToBlittable.MissingDictionary[o] = dictionary = new Dictionary<string, object>();
            }

            dictionary[key] = value;
        }

        /// <summary>
        /// Gets the default value of the specified type.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object GetDefaultValue(Type type)
        {
            return type.GetTypeInfo().IsValueType ? Activator.CreateInstance(type) : null;
        }

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be deleted when SaveChanges is called.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The entity.</param>
        public void Delete<T>(T entity)
        {
            if (ReferenceEquals(entity, null))
                throw new ArgumentNullException(nameof(entity));

            DocumentInfo value;
            if (DocumentsByEntity.TryGetValue(entity, out value) == false)
            {
                throw new InvalidOperationException(entity + " is not associated with the session, cannot delete unknown entity instance");
            }
            DeletedEntities.Add(entity);
            IncludedDocumentsByKey.Remove(value.Id);
            KnownMissingIds.Add(value.Id);
        }

        /// <summary>
        /// Marks the specified entity for deletion. The entity will be deleted when <see cref="IDocumentSession.SaveChanges"/> is called.
        /// WARNING: This method will not call beforeDelete listener!
        /// </summary>
        /// <param name="id"></param>
        public void Delete(string id)
        {
            Delete(id, null);
        }

        public void Delete(string id, long? expectedEtag)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            long? etag = null;
            DocumentInfo documentInfo;
            if (DocumentsById.TryGetValue(id, out documentInfo))
            {
                var newObj = EntityToBlittable.ConvertEntityToBlittable(documentInfo.Entity, documentInfo);
                if (documentInfo.Entity != null && EntityChanged(newObj, documentInfo, null))
                {
                    throw new InvalidOperationException(
                        "Can't delete changed entity using identifier. Use Delete<T>(T entity) instead.");
                }
                if (documentInfo.Entity != null)
                {
                    DocumentsByEntity.Remove(documentInfo.Entity);
                }
                DocumentsById.Remove(id);
                etag = documentInfo.ETag;
            }
            
            KnownMissingIds.Add(id);
            etag = UseOptimisticConcurrency ? etag : null;
            Defer(new DeleteCommandData(id, expectedEtag ?? etag));
        }

        //TODO
        /*internal void EnsureNotReadVetoed(RavenJObject metadata)
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
        }*/

        /// <summary>
        /// Stores the specified entity in the session. The entity will be saved when SaveChanges is called.
        /// </summary>
        public void Store(object entity)
        {
            string id;
            var hasId = GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id);
            StoreInternal(entity, null, null, hasId == false ? ConcurrencyCheckMode.Forced : ConcurrencyCheckMode.Auto);
        }

        /// <summary>
        /// Stores the specified entity in the session. The entity will be saved when SaveChanges is called.
        /// </summary>
        public void Store(object entity, long? etag)
        {
            StoreInternal(entity, etag, null, etag == null ? ConcurrencyCheckMode.Disabled : ConcurrencyCheckMode.Forced);
        }

        /// <summary>
        /// Stores the specified entity in the session, explicitly specifying its Id. The entity will be saved when SaveChanges is called.
        /// </summary>
        public void Store(object entity, string id)
        {
            StoreInternal(entity, null, id, ConcurrencyCheckMode.Auto);
        }

        /// <summary>
        /// Stores the specified entity in the session, explicitly specifying its Id. The entity will be saved when SaveChanges is called.
        /// </summary>
        public void Store(object entity, long? etag, string id)
        {
            StoreInternal(entity, etag, id, etag == null ? ConcurrencyCheckMode.Disabled : ConcurrencyCheckMode.Forced);
        }

        private void StoreInternal(object entity, long? etag, string id, ConcurrencyCheckMode forceConcurrencyCheck)
        {
            if (null == entity)
                throw new ArgumentNullException(nameof(entity));

            DocumentInfo value;
            if (DocumentsByEntity.TryGetValue(entity, out value))
            {
                if (etag != null)
                    value.ETag = etag;
                value.ConcurrencyCheckMode = forceConcurrencyCheck;
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

            if (_deferedCommands.Any(c => c.Key == id))
                throw new InvalidOperationException("Can't store document, there is a deferred command registered for this document in the session. Document id: " + id);

            if (DeletedEntities.Contains(entity))
                throw new InvalidOperationException("Can't store object, it was already deleted in this session.  Document id: " + id);

            // we make the check here even if we just generated the key
            // users can override the key generation behavior, and we need
            // to detect if they generate duplicates.
            AssertNoNonUniqueInstance(entity, id);

            var tag = _documentStore.Conventions.GetCollectionName(entity);
            var metadata = new DynamicJsonValue();
            if (tag != null)
                metadata[Constants.Documents.Metadata.Collection] = tag;

            var clrType = _documentStore.Conventions.GetClrTypeName(entity.GetType());
            if (clrType != null)
                metadata[Constants.Documents.Metadata.RavenClrType] = clrType;

            if (id != null)
                KnownMissingIds.Remove(id);
            StoreEntityInUnitOfWork(id, entity, etag, metadata, forceConcurrencyCheck);
        }

        public Task StoreAsync(object entity, CancellationToken token = default(CancellationToken))
        {
            string id;
            var hasId = GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id);

            return StoreAsyncInternal(entity, null, null, hasId == false ? ConcurrencyCheckMode.Forced : ConcurrencyCheckMode.Auto, token: token);
        }

        public Task StoreAsync(object entity, long? etag, CancellationToken token = default(CancellationToken))
        {
            return StoreAsyncInternal(entity, etag, null, etag == null ? ConcurrencyCheckMode.Disabled : ConcurrencyCheckMode.Forced, token: token);
        }

        public Task StoreAsync(object entity, long? etag, string id, CancellationToken token = default(CancellationToken))
        {
            return StoreAsyncInternal(entity, etag, id, etag == null ? ConcurrencyCheckMode.Disabled : ConcurrencyCheckMode.Forced, token: token);
        }

        public Task StoreAsync(object entity, string id, CancellationToken token = default(CancellationToken))
        {
            return StoreAsyncInternal(entity, null, id, ConcurrencyCheckMode.Auto, token: token);
        }

        private async Task StoreAsyncInternal(object entity, long? etag, string id, ConcurrencyCheckMode forceConcurrencyCheck, CancellationToken token = default(CancellationToken))
        {
            if (null == entity)
                throw new ArgumentNullException(nameof(entity));

            if (id == null)
            {
                id = await GenerateDocumentKeyForStorageAsync(entity).WithCancellation(token).ConfigureAwait(false);
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
                if (GenerateEntityIdOnTheClient.TryGetIdFromDynamic(entity, out id))
                    return id;

                var key = await GenerateKeyAsync(entity).ConfigureAwait(false);
                // If we generated a new id, store it back into the Id field so the client has access to to it                    
                if (key != null)
                    GenerateEntityIdOnTheClient.TrySetIdOnDynamic(entity, key);
                return key;
            }

            var result = await GetOrGenerateDocumentKeyAsync(entity).ConfigureAwait(false);
            GenerateEntityIdOnTheClient.TrySetIdentity(entity, result);
            return result;
        }

        protected abstract Task<string> GenerateKeyAsync(object entity);

        protected virtual void StoreEntityInUnitOfWork(string id, object entity, long? etag, DynamicJsonValue metadata, ConcurrencyCheckMode forceConcurrencyCheck)
        {
            DeletedEntities.Remove(entity);
            if (id != null)
                KnownMissingIds.Remove(id);

            var documentInfo = new DocumentInfo
            {
                Id = id,
                Metadata = Context.ReadObject(metadata, id),
                ETag = etag,
                ConcurrencyCheckMode = forceConcurrencyCheck,
                Entity = entity,
                IsNewDocument = true,
                Document = null
            };

            DocumentsByEntity.Add(entity, documentInfo);
            if (id != null)
                DocumentsById.Add(documentInfo);
        }

        protected virtual void AssertNoNonUniqueInstance(object entity, string id)
        {
            DocumentInfo info;
            if (id == null || id.EndsWith("/") || DocumentsById.TryGetValue(id, out info) == false || ReferenceEquals(info.Entity, entity))
                return;

            throw new NonUniqueObjectException("Attempted to associate a different object with id '" + id + "'.");
        }

        protected async Task<string> GetOrGenerateDocumentKeyAsync(object entity)
        {
            string id;
            GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id);

            Task<string> generator =
                id != null
                ? Task.FromResult(id)
                : GenerateKeyAsync(entity);

            var result = await generator.ConfigureAwait(false);
            if (result != null && result.StartsWith("/"))
                throw new InvalidOperationException("Cannot use value '" + id + "' as a document id because it begins with a '/'");

            return result;
        }

        public SaveChangesData PrepareForSaveChanges()
        {
            var result = new SaveChangesData
            {
                DeferedCommands = new List<ICommandData>(_deferedCommands),
                Options = _saveChangesOptions
            };
            _deferedCommands.Clear();

            PrepareForEntitiesDeletion(result, null);
            PrepareForEntitiesPuts(result);

            return result;
        }

        private void UpdateMetadataModifications(DocumentInfo documentInfo)
        {
            if ((documentInfo.MetadataInstance == null) || !((MetadataAsDictionary)documentInfo.MetadataInstance).Changed)
                return;
            if ((documentInfo.Metadata.Modifications == null) || (documentInfo.Metadata.Modifications.Properties.Count == 0))
            {
                documentInfo.Metadata.Modifications = new DynamicJsonValue();
            }
            foreach (var prop in documentInfo.MetadataInstance.Keys)
            {
                documentInfo.Metadata.Modifications[prop] = documentInfo.MetadataInstance[prop];
            }
        }

        private void PrepareForEntitiesDeletion(SaveChangesData result, IDictionary<string, DocumentsChanges[]> changes)
        {
            foreach (var deletedEntity in DeletedEntities)
            {
                if (DocumentsByEntity.TryGetValue(deletedEntity, out DocumentInfo documentInfo) == false)
                    continue;

                if (changes != null)
                {
                    var docChanges = new List<DocumentsChanges>();
                    var change = new DocumentsChanges
                    {
                        FieldNewValue = string.Empty,
                        FieldOldValue = string.Empty,
                        Change = DocumentsChanges.ChangeType.DocumentDeleted
                    };

                    docChanges.Add(change);
                    changes[documentInfo.Id] = docChanges.ToArray();
                }
                else
                {
                    foreach (var resultCommand in result.DeferedCommands)
                    {
                        if (resultCommand.Key == documentInfo.Id)
                            ThrowInvalidDeletedDocumentWithDefferredCommand(resultCommand);
                    }

                    long? etag = null;
                    if (DocumentsById.TryGetValue(documentInfo.Id, out documentInfo))
                    {
                        etag = documentInfo.ETag;

                        if (documentInfo.Entity != null)
                        {
                            var afterStoreEventArgs = new AfterStoreEventArgs(this, documentInfo.Id, documentInfo.Entity);
                            OnAfterStore?.Invoke(this, afterStoreEventArgs);

                            DocumentsByEntity.Remove(documentInfo.Entity);
                            result.Entities.Add(documentInfo.Entity);
                        }

                        DocumentsById.Remove(documentInfo.Id);
                    }
                    etag = UseOptimisticConcurrency ? etag : null;
                    var beforeDeleteEventArgs = new BeforeDeleteEventArgs(this, documentInfo.Id, documentInfo.Entity);
                    OnBeforeDelete?.Invoke(this, beforeDeleteEventArgs);
                    result.SessionCommands.Add(new DeleteCommandData(documentInfo.Id, etag));
                }
            }
            DeletedEntities.Clear();
        }

        private void PrepareForEntitiesPuts(SaveChangesData result)
        {
            foreach (var entity in DocumentsByEntity)
            {
                UpdateMetadataModifications(entity.Value);
                var document = EntityToBlittable.ConvertEntityToBlittable(entity.Key, entity.Value);
                if (entity.Value.IgnoreChanges || EntityChanged(document, entity.Value, null) == false)
                    continue;

                foreach (var resultCommand in result.DeferedCommands)
                {
                    if (resultCommand.Type != CommandType.AttachmentPUT &&
                        resultCommand.Key == entity.Value.Id)
                        ThrowInvalidModifiedDocumentWithDefferredCommand(resultCommand);
                }

                var beforeStoreEventArgs = new BeforeStoreEventArgs(this, entity.Value.Id, entity.Key);
                OnBeforeStore?.Invoke(this, beforeStoreEventArgs);
                if ((OnBeforeStore != null) && EntityChanged(document, entity.Value, null))
                    document = EntityToBlittable.ConvertEntityToBlittable(entity.Key, entity.Value);

                entity.Value.IsNewDocument = false;
                result.Entities.Add(entity.Key);

                if (entity.Value.Id != null)
                    DocumentsById.Remove(entity.Value.Id);

                entity.Value.Document = document;

                var etag = (UseOptimisticConcurrency &&
                            entity.Value.ConcurrencyCheckMode != ConcurrencyCheckMode.Disabled) ||
                           entity.Value.ConcurrencyCheckMode == ConcurrencyCheckMode.Forced
                    ? (long?)(entity.Value.ETag ?? 0)
                    : null;

                result.SessionCommands.Add(new PutCommandDataWithBlittableJson(entity.Value.Id, etag, document));
            }
        }

        private static void ThrowInvalidDeletedDocumentWithDefferredCommand(ICommandData resultCommand)
        {
            throw new InvalidOperationException(
                $"Cannot perfrom save because document {resultCommand.Key} has been deleted by the session and is also taking part in deferred {resultCommand.Type} command");
        }

        private static void ThrowInvalidModifiedDocumentWithDefferredCommand(ICommandData resultCommand)
        {
            throw new InvalidOperationException(
                $"Cannot perfrom save because document {resultCommand.Key} has been modified by the session and is also taking part in deferred {resultCommand.Type} command");
        }

        protected bool EntityChanged(BlittableJsonReaderObject newObj, DocumentInfo documentInfo, IDictionary<string, DocumentsChanges[]> changes)
        {
            return BlittableOperation.EntityChanged(newObj, documentInfo, changes);
        }

        public IDictionary<string, DocumentsChanges[]> WhatChanged()
        {
            var changes = new Dictionary<string, DocumentsChanges[]>();

            PrepareForEntitiesDeletion(null, changes);
            GetAllEntitiesChanges(changes);
            return changes;
        }

        /// <summary>
        /// Gets a value indicating whether any of the entities tracked by the session has changes.
        /// </summary>
        /// <value></value>
        public bool HasChanges
        {
            get
            {
                foreach (var entity in DocumentsByEntity)
                {
                    var document = EntityToBlittable.ConvertEntityToBlittable(entity.Key, entity.Value);
                    if (EntityChanged(document, entity.Value, null))
                    {
                        return true;
                    }
                }
                return DeletedEntities.Count > 0;
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
            DocumentInfo documentInfo;
            if (DocumentsByEntity.TryGetValue(entity, out documentInfo) == false)
                return false;
            var document = EntityToBlittable.ConvertEntityToBlittable(entity, documentInfo);
            return EntityChanged(document, documentInfo, null);
        }

        public void WaitForReplicationAfterSaveChanges(TimeSpan? timeout = null, bool throwOnTimeout = true,
            int replicas = 1, bool majority = false)
        {
            var realTimeout = timeout ?? TimeSpan.FromSeconds(15);
            if (_saveChangesOptions == null)
                _saveChangesOptions = new BatchOptions();
            _saveChangesOptions.WaitForReplicas = true;
            _saveChangesOptions.Majority = majority;
            _saveChangesOptions.NumberOfReplicasToWaitFor = replicas;
            _saveChangesOptions.WaitForReplicasTimeout = realTimeout;
            _saveChangesOptions.ThrowOnTimeoutInWaitForReplicas = throwOnTimeout;
        }

        public void WaitForIndexesAfterSaveChanges(TimeSpan? timeout = null, bool throwOnTimeout = false,
            string[] indexes = null)
        {
            var realTimeout = timeout ?? TimeSpan.FromSeconds(15);
            if (_saveChangesOptions == null)
                _saveChangesOptions = new BatchOptions();
            _saveChangesOptions.WaitForIndexes = true;
            _saveChangesOptions.WaitForIndexesTimeout = realTimeout;
            _saveChangesOptions.ThrowOnTimeoutInWaitForIndexes = throwOnTimeout;
            _saveChangesOptions.WaitForSpecificIndexes = indexes;
        }

        private void GetAllEntitiesChanges(IDictionary<string, DocumentsChanges[]> changes)
        {
            foreach (var pair in DocumentsById)
            {
                UpdateMetadataModifications(pair.Value);
                var newObj = EntityToBlittable.ConvertEntityToBlittable(pair.Value.Entity, pair.Value);
                EntityChanged(newObj, pair.Value, changes);
            }
        }

        /// <summary>
        /// Mark the entity as one that should be ignore for change tracking purposes,
        /// it still takes part in the session, but is ignored for SaveChanges.
        /// </summary>
        public void IgnoreChangesFor(object entity)
        {
            GetDocumentInfo(entity).IgnoreChanges = true;
        }

        /// <summary>
        /// Evicts the specified entity from the session.
        /// Remove the entity from the delete queue and stops tracking changes for this entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The entity.</param>
        public void Evict<T>(T entity)
        {
            DocumentInfo documentInfo;
            if (DocumentsByEntity.TryGetValue(entity, out documentInfo))
            {
                DocumentsByEntity.Remove(entity);
                DocumentsById.Remove(documentInfo.Id);
            }
            DeletedEntities.Remove(entity);
        }

        /// <summary>
        /// Clears this instance.
        /// Remove all entities from the delete queue and stops tracking changes for all entities.
        /// </summary>
        public void Clear()
        {
            DocumentsByEntity.Clear();
            DeletedEntities.Clear();
            DocumentsById.Clear();
            KnownMissingIds.Clear();
            IncludedDocumentsByKey.Clear();
        }

        /// <summary>
        ///     Defer commands to be executed on SaveChanges()
        /// </summary>
        /// <param name="command">Command to be executed</param>
        /// <param name="commands">Array of comands to be executed.</param>
        public void Defer(ICommandData command, params ICommandData[] commands)
        {
            _deferedCommands.Add(command);
            _deferedCommands.AddRange(commands);
        }

        /// <summary>
        /// Defer commands to be executed on SaveChanges()
        /// </summary>
        /// <param name="commands">The commands to be executed</param>
        public void Defer(ICommandData[] commands)
        {
            _deferedCommands.AddRange(commands);
        }

        private void Dispose(bool isDisposing)
        {
            if (isDisposing)
                GC.SuppressFinalize(this);
            _releaseOperationContext.Dispose();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public virtual void Dispose()
        {
            Dispose(true);
        }

        ~InMemoryDocumentSessionOperations()
        {
            Dispose(false);

#if DEBUG
            Debug.WriteLine("Disposing a session for finalizer! It should be disposed by calling session.Dispose()!");
#endif
        }

        public void RegisterMissing(string id)
        {
            KnownMissingIds.Add(id);
        }

        public void UnregisterMissing(string id)
        {
            KnownMissingIds.Remove(id);
        }

        public void RegisterMissingIncludes(BlittableJsonReaderArray results, ICollection<string> includes)
        {
            if (includes == null || includes.Count == 0)
                return;

            foreach (BlittableJsonReaderObject result in results)
            {
                foreach (var include in includes)
                {
                    if(include == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                        continue;

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
            return _hash;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(obj, this);
        }

        internal void HandleInternalMetadata(BlittableJsonReaderObject result)
        {
            // Implant a property with "id" value ... if it doesn't exist
            BlittableJsonReaderObject metadata;
            string id;
            if (result.TryGet(Constants.Documents.Metadata.Key, out metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Id, out id) == false)
            {
                // if the item doesn't have meta data, then nested items might have, so we need to check them
                var propDetail = new BlittableJsonReaderObject.PropertyDetails();
                for (int index = 0; index < result.Count; index++)
                {
                    result.GetPropertyByIndex(index, ref propDetail, addObjectToCache: true);
                    var jsonObj = propDetail.Value as BlittableJsonReaderObject;
                    if (jsonObj != null)
                    {
                        HandleInternalMetadata(jsonObj);
                        continue;
                    }

                    var jsonArray = propDetail.Value as BlittableJsonReaderArray;
                    if (jsonArray != null)
                    {
                        HandleInternalMetadata(jsonArray);
                    }
                }
                return;
            }

            string entityName;
            if (metadata.TryGet(Constants.Documents.Metadata.Collection, out entityName) == false)
                return;

            var idPropName = Conventions.FindIdentityPropertyNameFromEntityName(entityName);

            result.Modifications = new DynamicJsonValue
            {
                [idPropName] = id // this is being read by BlittableJsonReader for additional properties on the object
            };
        }

        internal void HandleInternalMetadata(BlittableJsonReaderArray values)
        {
            foreach (var nested in values)
            {
                var bObject = nested as BlittableJsonReaderObject;
                if (bObject != null)
                    HandleInternalMetadata(bObject);
                var bArray = nested as BlittableJsonReaderArray;
                if (bArray == null)
                    continue;
                HandleInternalMetadata(bArray);
            }
        }

        public object DeserializeFromTransformer(Type entityType, string id, BlittableJsonReaderObject document)
        {
            HandleInternalMetadata(document);
            return EntityToBlittable.ConvertToEntity(entityType, id, document);
        }

        public string CreateDynamicIndexName<T>()
        {
            var indexName = "dynamic";
            if (typeof(T) != typeof(object))
            {
                indexName += "/" + Conventions.GetCollectionName(typeof(T));
            }
            return indexName;
        }

        public bool CheckIfIdAlreadyIncluded(string[] ids, KeyValuePair<string, Type>[] includes)
        {
            return CheckIfIdAlreadyIncluded(ids, includes.Select(x => x.Key));
        }

        public bool CheckIfIdAlreadyIncluded(string[] ids, IEnumerable<string> includes)
        {
            foreach (var id in ids)
            {
                if (KnownMissingIds.Contains(id))
                    continue;

                DocumentInfo documentInfo;

                // Check if document was already loaded, the check if we've received it through include
                if (DocumentsById.TryGetValue(id, out documentInfo) == false && IncludedDocumentsByKey.TryGetValue(id, out documentInfo) == false)
                    return false;

                if (documentInfo.Entity == null)
                    return false;

                if (includes == null)
                    continue;

                foreach (var include in includes)
                {
                    var hasAll = true;
                    IncludesUtil.Include(documentInfo.Document, include, s =>
                    {
                        hasAll &= IsLoaded(s);
                        return true;
                    });
                    if (hasAll == false)
                        return false;
                }
            }
            return true;
        }

        protected void RefreshInternal<T>(T entity, RavenCommand<GetDocumentResult> cmd, DocumentInfo documentInfo)
        {
            var document = (BlittableJsonReaderObject)cmd.Result.Results[0];
            if (document == null)
                throw new InvalidOperationException("Document '" + documentInfo.Id +
                                                    "' no longer exists and was probably deleted");

            object value;
            document.TryGetMember(Constants.Documents.Metadata.Key, out value);
            documentInfo.Metadata = value as BlittableJsonReaderObject;

            object etag;
            document.TryGetMember(Constants.Documents.Metadata.Etag, out etag);
            documentInfo.ETag = etag as long?;

            documentInfo.Document = document;

            documentInfo.Entity = ConvertToEntity(typeof(T), documentInfo.Id, document);

            var type = entity.GetType();
            foreach (var property in ReflectionUtil.GetPropertiesAndFieldsFor(type, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
            {
                var prop = property;
                if (prop.DeclaringType != type && prop.DeclaringType != null)
                    prop = prop.DeclaringType.GetProperty(prop.Name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic) ?? property;

                if (!prop.CanWrite() || !prop.CanRead() || prop.GetIndexParameters().Length != 0)
                    continue;
                prop.SetValue(entity, prop.GetValue(documentInfo.Entity));
            }
        }

        protected static T GetOperationResult<T>(object result)
        {
            if (result == null)
                return default(T);

            if (result is T)
                return (T)result;

            var resultsArray = result as T[];
            if (resultsArray != null && resultsArray.Length > 0)
                return resultsArray[0];

            var resultsDictionary = result as Dictionary<string, T>;
            if (resultsDictionary != null)
            {
                if (resultsDictionary.Count == 0)
                    return default(T);

                if (resultsDictionary.Count == 1)
                    return resultsDictionary.Values.FirstOrDefault();
            }

            throw new InvalidCastException($"Unable to cast {result.GetType().Name} to {typeof(T).Name}");
        }

        public enum ConcurrencyCheckMode
        {
            /// <summary>
            /// Automatic optimistic concurrency check depending on UseOptimisticConcurrency setting or provided ETag
            /// </summary>
            Auto,

            /// <summary>
            /// Force optimistic concurrency check even if UseOptimisticConcurrency is not set
            /// </summary>
            Forced,

            /// <summary>
            /// Disable optimistic concurrency check even if UseOptimisticConcurrency is set
            /// </summary>
            Disabled
        }

        /// <summary>
        /// Data for a batch command to the server
        /// </summary>
        public class SaveChangesData
        {
            public List<ICommandData> DeferedCommands;
            public readonly List<ICommandData> SessionCommands = new List<ICommandData>();
            public readonly List<object> Entities = new List<object>();
            public BatchOptions Options;
        }

        public void OnAfterStoreInvoke(AfterStoreEventArgs afterStoreEventArgs)
        {
            OnAfterStore?.Invoke(this, afterStoreEventArgs);
        }

        public void OnBeforeQueryExecutedInvoke(BeforeQueryExecutedEventArgs beforeQueryExecutedEventArgs)
        {
            OnBeforeQueryExecuted?.Invoke(this, beforeQueryExecutedEventArgs);
        }
    }

    /// <summary>
    /// Information held about an entity by the session
    /// </summary>
    public class DocumentInfo
    {
        /// <summary>
        /// Gets or sets the id.
        /// </summary>
        /// <value>The id.</value>
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets the ETag.
        /// </summary>
        /// <value>The ETag.</value>
        public long? ETag { get; set; }

        /// <summary>
        /// A concurrency check will be forced on this entity 
        /// even if UseOptimisticConcurrency is set to false
        /// </summary>
        public InMemoryDocumentSessionOperations.ConcurrencyCheckMode ConcurrencyCheckMode { get; set; }

        /// <summary>
        /// If set to true, the session will ignore this document
        /// when SaveChanges() is called, and won't perform and change tracking
        /// </summary>
        public bool IgnoreChanges { get; set; }

        public BlittableJsonReaderObject Metadata { get; set; }

        public BlittableJsonReaderObject Document { get; set; }

        public IMetadataDictionary MetadataInstance { get; set; }

        public object Entity { get; set; }

        public bool IsNewDocument { get; set; }

        public string Collection { get; set; }

        public static DocumentInfo GetNewDocumentInfo(BlittableJsonReaderObject document)
        {
            BlittableJsonReaderObject metadata;
            string id;
            long etag;

            if (document.TryGet(Constants.Documents.Metadata.Key, out metadata) == false)
                throw new InvalidOperationException("Document must have a metadata");
            if (metadata.TryGet(Constants.Documents.Metadata.Id, out id) == false)
                throw new InvalidOperationException("Document must have an id");
            if (metadata.TryGet(Constants.Documents.Metadata.Etag, out etag) == false)
                throw new InvalidOperationException("Document must have an ETag");

            var newDocumentInfo = new DocumentInfo
            {
                Id = id,
                Document = document,
                Metadata = metadata,
                Entity = null,
                ETag = etag
            };
            return newDocumentInfo;
        }
    }
}