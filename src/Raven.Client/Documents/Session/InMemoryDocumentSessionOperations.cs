//-----------------------------------------------------------------------
// <copyright file="InMemoryDocumentSessionOperations.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Lambda2Js;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Documents.Session.Tokens;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Abstract implementation for in memory session operations
    /// </summary>
    public abstract partial class InMemoryDocumentSessionOperations : IDisposable
    {
        internal long _asyncTasksCounter;
        internal int _maxDocsCountOnCachedRenewSession = 16 * 1024;
        protected readonly RequestExecutor _requestExecutor;
        private OperationExecutor _operationExecutor;
        private readonly IDisposable _releaseOperationContext;
        private readonly JsonOperationContext _context;
        protected readonly List<ILazyOperation> PendingLazyOperations = new List<ILazyOperation>();
        protected readonly Dictionary<ILazyOperation, Action<object>> OnEvaluateLazy = new Dictionary<ILazyOperation, Action<object>>();
        private static int _instancesCounter;
        private readonly int _hash = Interlocked.Increment(ref _instancesCounter);
        protected bool GenerateDocumentIdsOnStore = true;
        protected internal readonly SessionInfo _sessionInfo;

        private BatchOptions _saveChangesOptions;

        internal readonly bool? DisableAtomicDocumentWritesInClusterWideTransaction;

        public TransactionMode TransactionMode;

        private bool _isDisposed;
        private IJsonSerializer _jsonSerializer;

        /// <summary>
        /// The session id
        /// </summary>
        public Guid Id { get; }

        public event EventHandler<BeforeStoreEventArgs> OnBeforeStore;

        public event EventHandler<AfterSaveChangesEventArgs> OnAfterSaveChanges;

        public event EventHandler<BeforeDeleteEventArgs> OnBeforeDelete;

        public event EventHandler<BeforeQueryEventArgs> OnBeforeQuery;

        public event EventHandler<BeforeConversionToDocumentEventArgs> OnBeforeConversionToDocument;

        public event EventHandler<AfterConversionToDocumentEventArgs> OnAfterConversionToDocument;

        public event EventHandler<BeforeConversionToEntityEventArgs> OnBeforeConversionToEntity;

        public event EventHandler<AfterConversionToEntityEventArgs> OnAfterConversionToEntity;

        public event EventHandler<SessionDisposingEventArgs> OnSessionDisposing;

        /// <summary>
        /// Entities whose id we already know do not exists, because they are a missing include, or a missing load, etc.
        /// </summary>
        protected readonly HashSet<string> _knownMissingIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private Dictionary<string, object> _externalState;

        public IDictionary<string, object> ExternalState => _externalState ?? (_externalState = new Dictionary<string, object>());

        public async Task<ServerNode> GetCurrentSessionNode()
        {
            using (AsyncTaskHolder())
                return await SessionInfo.GetCurrentSessionNode(_requestExecutor).ConfigureAwait(false);
        }

        /// <summary>
        /// Translate between an ID and its associated entity
        /// </summary>
        internal readonly DocumentsById DocumentsById = new DocumentsById();

        /// <summary>
        /// Translate between an ID and its associated entity
        /// </summary>
        internal readonly Dictionary<string, DocumentInfo> IncludedDocumentsById = new Dictionary<string, DocumentInfo>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// hold the data required to manage the data for RavenDB's Unit of Work
        /// </summary>
        internal readonly DocumentsByEntityHolder DocumentsByEntity = new DocumentsByEntityHolder();

        /// <summary>
        /// The entities waiting to be deleted
        /// </summary>
        internal readonly DeletedEntitiesHolder DeletedEntities = new DeletedEntitiesHolder();

        /// <summary>
        /// hold the data required to manage Counters tracking for RavenDB's Unit of Work
        /// </summary>
        protected internal Dictionary<string, (bool GotAll, Dictionary<string, long?> Values)> CountersByDocId =>
            _countersByDocId ?? (_countersByDocId = new Dictionary<string, (bool GotAll, Dictionary<string, long?> Values)>(StringComparer.OrdinalIgnoreCase));

        private Dictionary<string, (bool GotAll, Dictionary<string, long?> Values)> _countersByDocId;

        protected internal Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> TimeSeriesByDocId =>
            _timeSeriesByDocId ?? (_timeSeriesByDocId = new Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>>(StringComparer.OrdinalIgnoreCase));

        private Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> _timeSeriesByDocId;

        protected readonly DocumentStoreBase _documentStore;

        public string DatabaseName { get; }

        ///<summary>
        /// The document store associated with this session
        ///</summary>
        public IDocumentStore DocumentStore => _documentStore;

        public RequestExecutor RequestExecutor => _requestExecutor;

        public SessionInfo SessionInfo => _sessionInfo;

        internal OperationExecutor Operations => _operationExecutor ??= new SessionOperationExecutor(this);

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
        public DocumentConventions Conventions => _requestExecutor.Conventions;

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

        protected readonly List<ICommandData> DeferredCommands = new List<ICommandData>();

        protected internal readonly Dictionary<(string, CommandType, string), ICommandData> DeferredCommandsDictionary =
            new Dictionary<(string, CommandType, string), ICommandData>();

        public readonly bool NoTracking;

        internal Dictionary<string, ForceRevisionStrategy> IdsForCreatingForcedRevisions = new Dictionary<string, ForceRevisionStrategy>(StringComparer.OrdinalIgnoreCase);

        public int DeferredCommandsCount => DeferredCommands.Count;

        public GenerateEntityIdOnTheClient GenerateEntityIdOnTheClient { get; }
        public ISessionBlittableJsonConverter JsonConverter { get; }

        protected internal IJsonSerializer JsonSerializer => _jsonSerializer ?? (_jsonSerializer = RequestExecutor.Conventions.Serialization.CreateSerializer());

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryDocumentSessionOperations"/> class.
        /// </summary>
        protected InMemoryDocumentSessionOperations(
            DocumentStoreBase documentStore,
            Guid id,
            SessionOptions options)
        {
            Id = id;
            DatabaseName = options.Database ?? documentStore.Database;
            if (string.IsNullOrWhiteSpace(DatabaseName))
                ThrowNoDatabase();

            _documentStore = documentStore;
            _requestExecutor = options.RequestExecutor ?? documentStore.GetRequestExecutor(DatabaseName);
            _releaseOperationContext = _requestExecutor.ContextPool.AllocateOperationContext(out _context);
            NoTracking = options.NoTracking;
            UseOptimisticConcurrency = _requestExecutor.Conventions.UseOptimisticConcurrency;
            MaxNumberOfRequestsPerSession = _requestExecutor.Conventions.MaxNumberOfRequestsPerSession;
            GenerateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(_requestExecutor.Conventions, GenerateId);
            JsonConverter = _requestExecutor.Conventions.Serialization.CreateConverter(this);
            _sessionInfo = new SessionInfo(this, options, _documentStore, asyncCommandRunning: false);
            TransactionMode = options.TransactionMode;
            DisableAtomicDocumentWritesInClusterWideTransaction = options.DisableAtomicDocumentWritesInClusterWideTransaction;

            _javascriptCompilationOptions = new JavascriptCompilationOptions(
                flags: JsCompilationFlags.BodyOnly | JsCompilationFlags.ScopeParameter,
                extensions: new JavascriptConversionExtension[]
                {
                    JavascriptConversionExtensions.LinqMethodsSupport.Instance,
                    JavascriptConversionExtensions.NullableSupport.Instance
                })
            {
                CustomMetadataProvider = new PropertyNameConventionJSMetadataProvider(RequestExecutor.Conventions)
            };
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

        public void SetTransactionMode(TransactionMode mode)
        {
            TransactionMode = mode;
        }

        /// <summary>
        /// Gets all counter names for the specified entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        public List<string> GetCountersFor<T>(T instance)
        {
            if (instance == null)
                throw new ArgumentNullException(nameof(instance));

            var documentInfo = GetDocumentInfo(instance);

            if (documentInfo.Metadata.TryGet(Constants.Documents.Metadata.Counters,
                out BlittableJsonReaderArray counters) == false)
                return null;
            return counters.Select(x => x.ToString()).ToList();
        }

        /// <summary>
        /// Gets all time series names for the specified entity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        public List<string> GetTimeSeriesFor<T>(T instance)
        {
            if (instance == null)
                throw new ArgumentNullException(nameof(instance));

            var documentInfo = GetDocumentInfo(instance);

            if (documentInfo.Metadata.TryGet(Constants.Documents.Metadata.TimeSeries,
                out BlittableJsonReaderArray bjra) == false)
                return new List<string>();

            var tsList = new List<string>(bjra.Length);

            for (int i = 0; i < bjra.Length; i++)
            {
                var val = bjra.GetStringByIndex(i);
                tsList.Add(val);
            }

            return tsList;
        }

        /// <summary>
        /// Gets the Change Vector for the specified entity.
        /// If the entity is transient, it will load the change vector from the store
        /// and associate the current state of the entity with the change vector from the server.
        /// </summary>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        public string GetChangeVectorFor<T>(T instance)
        {
            if (instance == null)
                throw new ArgumentNullException(nameof(instance));

            var documentInfo = GetDocumentInfo(instance);
            if (documentInfo.Metadata.TryGet(Constants.Documents.Metadata.ChangeVector, out string changeVectorJson))
                return changeVectorJson;

            return null;
        }

        public DateTime? GetLastModifiedFor<T>(T instance)
        {
            if (instance == null)
                throw new ArgumentNullException(nameof(instance));

            var documentInfo = GetDocumentInfo(instance);
            if (documentInfo.Metadata.TryGet(Constants.Documents.Metadata.LastModified, out DateTime lastModified))
                return lastModified;

            return null;
        }

        private DocumentInfo GetDocumentInfo<T>(T instance)
        {
            if (DocumentsByEntity.TryGetValue(instance, out DocumentInfo documentInfo))
                return documentInfo;

            if (GenerateEntityIdOnTheClient.TryGetIdFromInstance(instance, out string id) == false &&
                (instance is IDynamicMetaObjectProvider == false || GenerateEntityIdOnTheClient.TryGetIdFromDynamic(instance, out id) == false))
                throw new InvalidOperationException($"Could not find the document id for {instance}");

            AssertNoNonUniqueInstance(instance, id);

            throw new ArgumentException($"Document {id} doesn't exist in the session");
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
            return DocumentsById.TryGetValue(id, out DocumentInfo documentInfo) && (documentInfo.Document != null || documentInfo.Entity != null) ||
                   IsDeleted(id) ||
                   IncludedDocumentsById.ContainsKey(id);
        }

        /// <summary>
        /// Returns whether a document with the specified id is deleted
        /// or known to be missing
        /// </summary>
        public bool IsDeleted(string id)
        {
            return _knownMissingIds.Contains(id);
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
Raven provides facilities like Load(string[] ids) to load multiple documents at once and batch saves (call SaveChanges() only once).
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
        /// <param name="id">The document id.</param>
        /// <param name="document">The document.</param>
        /// <param name="metadata">The metadata.</param>'
        /// <param name="noTracking"></param>
        /// <returns></returns>
        public T TrackEntity<T>(string id, BlittableJsonReaderObject document, BlittableJsonReaderObject metadata, bool noTracking)
        {
            var entity = TrackEntity(typeof(T), id, document, metadata, noTracking);
            try
            {
                return (T)entity;
            }
            catch (InvalidCastException e)
            {
                var actual = typeof(T).Name;
                var expected = entity.GetType().Name;
                var message = string.Format("The query results type is '{0}' but you expected to get results of type '{1}'. " +
                                            "If you want to return a projection, you should use .ProjectInto<{1}>() (for Query) or .SelectFields<{1}>() (for DocumentQuery) before calling to .ToList().",
                    expected, actual);
                throw new InvalidOperationException(message, e);
            }
        }

        /// <summary>
        /// Tracks the entity inside the unit of work
        /// </summary>
        /// <param name="entityType"></param>
        /// <param name="documentFound">The document found.</param>
        /// <returns></returns>
        private object TrackEntity(Type entityType, DocumentInfo documentFound)
        {
            return TrackEntity(entityType, documentFound.Id, documentFound.Document, documentFound.Metadata, noTracking: NoTracking);
        }

        internal void RegisterExternalLoadedIntoTheSession(DocumentInfo info)
        {
            if (NoTracking)
                return;

            if (DocumentsById.TryGetValue(info.Id, out var existing))
            {
                if (ReferenceEquals(existing.Entity, info.Entity))
                    return;
                throw new InvalidOperationException("The document " + info.Id + " is already in the session with a different entity instance");
            }
            if (DocumentsByEntity.TryGetValue(info.Entity, out existing))
            {
                if (string.Equals(info.Id, existing.Id, StringComparison.OrdinalIgnoreCase))
                    return;
                throw new InvalidOperationException("Attempted to load an entity with id " + info.Id + ", but the entity instance already exists in the session with id: " + existing.Id);
            }
            DocumentsByEntity.Add(info.Entity, info);
            DocumentsById.Add(info);
            IncludedDocumentsById.Remove(info.Id);
        }

        /// <summary>
        /// Tracks the entity.
        /// </summary>
        /// <param name="entityType">The entity type.</param>
        /// <param name="id">The document ID.</param>
        /// <param name="document">The document.</param>
        /// <param name="metadata">The metadata.</param>
        /// <param name="noTracking">Entity tracking is enabled if true, disabled otherwise.</param>
        /// <returns></returns>
        private object TrackEntity(Type entityType, string id, BlittableJsonReaderObject document, BlittableJsonReaderObject metadata, bool noTracking)
        {
            noTracking = NoTracking || noTracking; // if noTracking is session-wide then we want to override the passed argument

            if (string.IsNullOrEmpty(id))
            {
                return DeserializeFromTransformer(entityType, null, document, false);
            }

            if (DocumentsById.TryGetValue(id, out var docInfo))
            {
                // the local instance may have been changed, we adhere to the current Unit of Work
                // instance, and return that, ignoring anything new.
                if (docInfo.Entity == null)
                    docInfo.Entity = JsonConverter.FromBlittable(entityType, ref document, id, trackEntity: noTracking == false);

                if (noTracking == false)
                {
                    IncludedDocumentsById.Remove(id);
                    DocumentsByEntity[docInfo.Entity] = docInfo;
                }

                return docInfo.Entity;
            }

            if (IncludedDocumentsById.TryGetValue(id, out docInfo))
            {
                if (docInfo.Entity == null)
                    docInfo.Entity = JsonConverter.FromBlittable(entityType, ref document, id, trackEntity: noTracking == false);

                if (noTracking == false)
                {
                    IncludedDocumentsById.Remove(id);
                    DocumentsById.Add(docInfo);
                    DocumentsByEntity[docInfo.Entity] = docInfo;
                }

                return docInfo.Entity;
            }

            var entity = JsonConverter.FromBlittable(entityType, ref document, id, trackEntity: noTracking == false);

            if (metadata.TryGet(Constants.Documents.Metadata.ChangeVector, out string changeVector) == false)
                throw new InvalidOperationException("Document " + id + " must have Change Vector");

            if (noTracking == false)
            {
                var newDocumentInfo = new DocumentInfo
                {
                    Id = id,
                    Document = document,
                    Metadata = metadata,
                    Entity = entity,
                    ChangeVector = changeVector
                };

                DocumentsById.Add(newDocumentInfo);
                DocumentsByEntity[entity] = newDocumentInfo;
            }

            return entity;
        }

        /// <summary>
        /// Gets the default value of the specified type.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object GetDefaultValue(Type type)
        {
            return type.IsValueType ? Activator.CreateInstance(type) : null;
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

            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo value) == false)
            {
                throw new InvalidOperationException(entity + " is not associated with the session, cannot delete unknown entity instance");
            }

            DeletedEntities.Add(entity);
            IncludedDocumentsById.Remove(value.Id);
            _countersByDocId?.Remove(value.Id);
            _knownMissingIds.Add(value.Id);
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

        public void Delete(string id, string expectedChangeVector)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            string changeVector = null;
            if (DocumentsById.TryGetValue(id, out DocumentInfo documentInfo))
            {
                using (var newObj = JsonConverter.ToBlittable(documentInfo.Entity, documentInfo))
                {
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
                    changeVector = documentInfo.ChangeVector;
                }
            }

            _knownMissingIds.Add(id);
            changeVector = UseOptimisticConcurrency ? changeVector : null;
            _countersByDocId?.Remove(id);
            Defer(new DeleteCommandData(id, expectedChangeVector ?? changeVector, expectedChangeVector ?? documentInfo?.ChangeVector));
        }

        /// <summary>
        /// Stores the specified entity in the session. The entity will be saved when SaveChanges is called.
        /// </summary>
        public void Store(object entity)
        {
            var hasId = GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out string _);
            StoreInternal(entity, null, null, hasId == false ? ConcurrencyCheckMode.Forced : ConcurrencyCheckMode.Auto);
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
        public void Store(object entity, string changeVector, string id)
        {
            StoreInternal(entity, changeVector, id, changeVector == null ? ConcurrencyCheckMode.Disabled : ConcurrencyCheckMode.Forced);
        }

        private void StoreInternal(object entity, string changeVector, string id, ConcurrencyCheckMode forceConcurrencyCheck)
        {
            if (NoTracking)
                throw new InvalidOperationException("Cannot store entity. Entity tracking is disabled in this session.");

            if (entity == null)
                throw new ArgumentNullException(nameof(entity));

            if (DocumentsByEntity.TryGetValue(entity, out var value))
            {
                value.ChangeVector = changeVector ?? value.ChangeVector;
                value.ConcurrencyCheckMode = forceConcurrencyCheck;
                return;
            }

            if (id == null)
            {
                if (GenerateDocumentIdsOnStore)
                {
                    id = GenerateEntityIdOnTheClient.GenerateDocumentIdForStorage(entity);
                }
                else
                {
                    RememberEntityForDocumentIdGeneration(entity);
                }
            }
            else
            {
                // Store it back into the Id field so the client has access to it
                GenerateEntityIdOnTheClient.TrySetIdentity(entity, id);
            }

            if (DeferredCommandsDictionary.ContainsKey((id, CommandType.ClientAnyCommand, null)))
                throw new InvalidOperationException("Can't store document, there is a deferred command registered for this document in the session. Document id: " + id);

            if (DeletedEntities.Contains(entity))
                throw new InvalidOperationException("Can't store object, it was already deleted in this session. Document id: " + id);

            // we make the check here even if we just generated the ID
            // users can override the ID generation behavior, and we need
            // to detect if they generate duplicates.
            AssertNoNonUniqueInstance(entity, id);

            var collectionName = _requestExecutor.Conventions.GetCollectionName(entity);
            var metadata = new DynamicJsonValue();
            if (collectionName != null)
                metadata[Constants.Documents.Metadata.Collection] = collectionName;

            var clrType = _requestExecutor.Conventions.GetClrTypeName(entity);
            if (clrType != null)
                metadata[Constants.Documents.Metadata.RavenClrType] = clrType;

            if (id != null)
                _knownMissingIds.Remove(id);

            StoreEntityInUnitOfWork(id, entity, changeVector, metadata, forceConcurrencyCheck);
        }

        public Task StoreAsync(object entity, CancellationToken token = default(CancellationToken))
        {
            var hasId = GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out string _);

            return StoreAsyncInternal(entity, null, null, hasId == false ? ConcurrencyCheckMode.Forced : ConcurrencyCheckMode.Auto, token: token);
        }

        public Task StoreAsync(object entity, string changeVector, string id, CancellationToken token = default(CancellationToken))
        {
            return StoreAsyncInternal(entity, changeVector, id, changeVector == null ? ConcurrencyCheckMode.Disabled : ConcurrencyCheckMode.Forced, token: token);
        }

        public Task StoreAsync(object entity, string id, CancellationToken token = default(CancellationToken))
        {
            return StoreAsyncInternal(entity, null, id, ConcurrencyCheckMode.Auto, token: token);
        }

        private async Task StoreAsyncInternal(object entity, string changeVector, string id, ConcurrencyCheckMode forceConcurrencyCheck,
            CancellationToken token = default(CancellationToken))
        {
            using (AsyncTaskHolder())
            {
                if (null == entity)
                    throw new ArgumentNullException(nameof(entity));

                if (id == null)
                {
                    id = await GenerateDocumentIdForStorageAsync(entity).WithCancellation(token).ConfigureAwait(false);
                }

                StoreInternal(entity, changeVector, id, forceConcurrencyCheck);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IDisposable AsyncTaskHolder()
        {
            return new AsyncTaskHolder(this);
        }

        protected abstract string GenerateId(object entity);

        protected virtual void RememberEntityForDocumentIdGeneration(object entity)
        {
            throw new NotImplementedException("You cannot set GenerateDocumentIdsOnStore to false without implementing RememberEntityForDocumentIdGeneration");
        }

        protected internal async Task<string> GenerateDocumentIdForStorageAsync(object entity)
        {
            if (Conventions.AddIdFieldToDynamicObjects && entity is IDynamicMetaObjectProvider)
            {
                if (GenerateEntityIdOnTheClient.TryGetIdFromDynamic(entity, out string id))
                    return id;

                id = await GenerateIdAsync(entity).ConfigureAwait(false);
                // If we generated a new id, store it back into the Id field so the client has access to it
                if (id != null)
                    GenerateEntityIdOnTheClient.TrySetIdOnDynamic(entity, id);
                return id;
            }

            var result = await GetOrGenerateDocumentIdAsync(entity).ConfigureAwait(false);
            GenerateEntityIdOnTheClient.TrySetIdentity(entity, result);
            return result;
        }

        protected abstract Task<string> GenerateIdAsync(object entity);

        protected virtual void StoreEntityInUnitOfWork(string id, object entity, string changeVector, DynamicJsonValue metadata,
            ConcurrencyCheckMode forceConcurrencyCheck)
        {
            if (id != null)
                _knownMissingIds.Remove(id);

            var documentInfo = new DocumentInfo
            {
                Id = id,
                Metadata = Context.ReadObject(metadata, id),
                ChangeVector = changeVector,
                ConcurrencyCheckMode = forceConcurrencyCheck,
                Entity = entity,
                IsNewDocument = true,
                Document = null
            };

            DocumentsByEntity.Add(entity, documentInfo);
            if (id != null)
                DocumentsById.Add(documentInfo);
        }

        protected void AssertNoNonUniqueInstance(object entity, string id)
        {
            DocumentInfo info;
            if (string.IsNullOrEmpty(id) ||
                id[id.Length - 1] == '|' ||
                id[id.Length - 1] == Conventions.IdentityPartsSeparator ||
                DocumentsById.TryGetValue(id, out info) == false ||
                ReferenceEquals(info.Entity, entity))
                return;

            throw new NonUniqueObjectException("Attempted to associate a different object with id '" + id + "'.");
        }

        protected async Task<string> GetOrGenerateDocumentIdAsync(object entity)
        {
            GenerateEntityIdOnTheClient.TryGetIdFromInstance(entity, out var id);

            Task<string> generator = id != null
                ? Task.FromResult(id)
                : GenerateIdAsync(entity);

            var result = await generator.ConfigureAwait(false);
            if (result != null && result.StartsWith("/"))
                throw new InvalidOperationException("Cannot use value '" + id + "' as a document id because it begins with a '/'");

            return result;
        }

        internal SaveChangesData PrepareForSaveChanges()
        {
            var result = new SaveChangesData(this);

            var deferredCommandsCount = DeferredCommands.Count;

            PrepareForEntitiesDeletion(result, null);
            PrepareForEntitiesPuts(result);
            PrepareForCreatingRevisionsFromIds(result);
            PrepareCompareExchangeEntities(result);

            if (DeferredCommands.Count > deferredCommandsCount)
            {
                // this allow OnBeforeStore to call Defer during the call to include
                // additional values during the same SaveChanges call
                result.DeferredCommands.AddRange(DeferredCommands.Skip(deferredCommandsCount));
                foreach (var item in DeferredCommandsDictionary)
                    result.DeferredCommandsDictionary[item.Key] = item.Value;
            }

            foreach (var deferredCommand in result.DeferredCommands)
                deferredCommand.OnBeforeSaveChanges(this);

            return result;
        }

        internal void ValidateClusterTransaction(SaveChangesData result)
        {
            if (TransactionMode != TransactionMode.ClusterWide)
                return;

            if (UseOptimisticConcurrency)
                throw new NotSupportedException(
                    $"{nameof(UseOptimisticConcurrency)} is not supported with {nameof(TransactionMode)} set to {nameof(TransactionMode.ClusterWide)}");

            foreach (var command in result.SessionCommands)
            {
                switch (command.Type)
                {
                    case CommandType.PUT:
                    case CommandType.DELETE:
                        if (command.ChangeVector != null)
                            throw new NotSupportedException($"Optimistic concurrency for '{command.Id}' is not supported when using a cluster transaction.");
                        break;
                    case CommandType.CompareExchangeDELETE:
                    case CommandType.CompareExchangePUT:
                        break;
                    default:
                        throw new NotSupportedException($"The command '{command.Type}' is not supported in a cluster session.");
                }
            }
        }

        private void PrepareCompareExchangeEntities(SaveChangesData result)
        {
            if (HasClusterSession == false)
                return;

            var clusterSession = GetClusterSession();
            if (clusterSession.NumberOfTrackedCompareExchangeValues == 0)
                return;

            if (TransactionMode != TransactionMode.ClusterWide)
                throw new InvalidOperationException($"Performing cluster transaction operations require the '{nameof(TransactionMode)}' to be set to '{nameof(TransactionMode.ClusterWide)}'.");

            clusterSession.PrepareCompareExchangeEntities(result);
        }

        protected abstract bool HasClusterSession { get; }

        protected abstract void ClearClusterSession();

        protected internal abstract ClusterTransactionOperationsBase GetClusterSession();

        private static bool UpdateMetadataModifications(DocumentInfo documentInfo)
        {
            if ((documentInfo.MetadataInstance == null ||
                ((MetadataAsDictionary)documentInfo.MetadataInstance).Changed == false) &&
                (documentInfo.Metadata.Modifications == null ||
                documentInfo.Metadata.Modifications.Properties.Count == 0))
                return false;

            if (documentInfo.Metadata.Modifications == null || documentInfo.Metadata.Modifications.Properties.Count == 0)
            {
                documentInfo.Metadata.Modifications = new DynamicJsonValue();
            }

            if (documentInfo.MetadataInstance != null)
            {
                foreach (var prop in documentInfo.MetadataInstance.Keys)
                {
                    var result = documentInfo.MetadataInstance[prop];
                    if(result is IMetadataDictionary md)
                    {
                        result = HandleDictionaryObject(md);
                    }
                    documentInfo.Metadata.Modifications[prop] =  result;
                }
            }

            return true;
        }

        private static object HandleDictionaryObject(IMetadataDictionary md)
        {
            var djv = new DynamicJsonValue();
            foreach (var item in md)
            {
                var v = item.Value;
                if(v is IMetadataDictionary nested)
                {
                    RuntimeHelpers.EnsureSufficientExecutionStack();
                    v = HandleDictionaryObject(nested);
                }
                djv[item.Key] = v;
            }
            return djv;
        }

        private void PrepareForCreatingRevisionsFromIds(SaveChangesData result)
        {
            // Note: here there is no point checking 'Before' or 'After' because if there were changes then forced revision is done from the PUT command....
            foreach (var idEntry in IdsForCreatingForcedRevisions)
            {
                result.SessionCommands.Add(new ForceRevisionCommandData(idEntry.Key));
            }

            IdsForCreatingForcedRevisions.Clear();
        }

        private void PrepareForEntitiesDeletion(SaveChangesData result, IDictionary<string, DocumentsChanges[]> changes)
        {
            using (DeletedEntities.PrepareEntitiesDeletes())
            {
                foreach (var deletedEntity in DeletedEntities)
                {
                    if (DocumentsByEntity.TryGetValue(deletedEntity.Entity, out DocumentInfo documentInfo) == false)
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
                        if (result.DeferredCommandsDictionary.TryGetValue((documentInfo.Id, CommandType.ClientAnyCommand, null), out ICommandData command))
                        {
                            // here we explicitly want to throw for all types of deferred commands, if the document
                            // is being deleted, we never want to allow any other operations on it
                            ThrowInvalidDeletedDocumentWithDeferredCommand(command);
                        }

                        string changeVector = null;
                        if (DocumentsById.TryGetValue(documentInfo.Id, out documentInfo))
                        {
                            changeVector = documentInfo.ChangeVector;

                            if (documentInfo.Entity != null)
                            {
                                result.OnSuccess.RemoveDocumentByEntity(documentInfo.Entity);
                                result.Entities.Add(documentInfo.Entity);
                            }

                            result.OnSuccess.RemoveDocumentById(documentInfo.Id);
                        }

                        if (UseOptimisticConcurrency == false)
                            changeVector = null;
                       
                        if (deletedEntity.ExecuteOnBeforeDelete)
                        {
                            OnBeforeDeleteInvoke(new BeforeDeleteEventArgs(this, documentInfo.Id, documentInfo.Entity));
                        }

                        var deleteCommandData = new DeleteCommandData(documentInfo.Id, changeVector, documentInfo.ChangeVector);
                        if (TransactionMode == TransactionMode.ClusterWide)
                        {
                            // we need this to send the cluster transaction index to the cluster state machine
                            deleteCommandData.Document = documentInfo.Metadata;
                        }
                        result.SessionCommands.Add(deleteCommandData);
                    }
                }
            }

            if (changes == null)
                result.OnSuccess.ClearDeletedEntities();
        }

        private void PrepareForEntitiesPuts(SaveChangesData result)
        {
            using (DocumentsByEntity.PrepareEntitiesPuts())
            {
                var shouldIgnoreEntityChanges = Conventions.ShouldIgnoreEntityChanges;
                foreach (var entity in DocumentsByEntity)
                {
                    if (entity.Value.IgnoreChanges)
                        continue;

                    if (shouldIgnoreEntityChanges != null)
                    {
                        if (shouldIgnoreEntityChanges(this, entity.Value.Entity, entity.Value.Id))
                            continue;
                    }

                    if (IsDeleted(entity.Value.Id))
                        continue;

                    var metadataUpdated = UpdateMetadataModifications(entity.Value);

                    var document = JsonConverter.ToBlittable(entity.Key, entity.Value);

                    if (EntityChanged(document, entity.Value, null) == false)
                    {
                        document.Dispose();
                        continue;
                    }

                    if (result.DeferredCommandsDictionary.TryGetValue((entity.Value.Id, CommandType.ClientModifyDocumentCommand, null), out ICommandData command))
                        ThrowInvalidModifiedDocumentWithDeferredCommand(command);

                    var onOnBeforeStore = OnBeforeStore;
                    if (onOnBeforeStore != null && entity.ExecuteOnBeforeStore)
                    {
                        var beforeStoreEventArgs = new BeforeStoreEventArgs(this, entity.Value.Id, entity.Key);
                        onOnBeforeStore(this, beforeStoreEventArgs);
                        if (metadataUpdated || beforeStoreEventArgs.MetadataAccessed)
                            metadataUpdated |= UpdateMetadataModifications(entity.Value);
                        if (beforeStoreEventArgs.MetadataAccessed ||
                            EntityChanged(document, entity.Value, null))
                        {
                            document.Dispose();
                            document = JsonConverter.ToBlittable(entity.Key, entity.Value);
                        }
                    }

                    result.Entities.Add(entity.Key);

                    if (entity.Value.Id != null)
                    {
                        result.OnSuccess.RemoveDocumentById(entity.Value.Id);
                    }

                    result.OnSuccess.UpdateEntityDocumentInfo(entity.Value, document);

                    if (metadataUpdated)
                    {
                        // we need to preserve the metadata after the changes, otherwise we'll consume the changes
                        // and any metadata changes will be gone afterward from the session data
                        if (document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                        {
                            ThrowMissingDocumentMetadata(document);
                        }

                        entity.Value.Metadata = Context.ReadObject(metadata, entity.Value.Id, BlittableJsonDocumentBuilder.UsageMode.None);
                    }

                    string changeVector;
                    if (UseOptimisticConcurrency)
                    {
                        if (entity.Value.ConcurrencyCheckMode != ConcurrencyCheckMode.Disabled)
                            // if the user didn't provide a change vector, we'll test for an empty one
                            changeVector = entity.Value.ChangeVector ?? string.Empty;
                        else
                            changeVector = null;
                    }
                    else if (entity.Value.ConcurrencyCheckMode == ConcurrencyCheckMode.Forced)
                        changeVector = entity.Value.ChangeVector;
                    else
                        changeVector = null;

                    var forceRevisionCreationStrategy = ForceRevisionStrategy.None;

                    if (entity.Value.Id != null)
                    {
                        // Check if user wants to Force a Revision
                        if (IdsForCreatingForcedRevisions.TryGetValue(entity.Value.Id, out var creationStrategy))
                        {
                            IdsForCreatingForcedRevisions.Remove(entity.Value.Id);
                            forceRevisionCreationStrategy = creationStrategy;
                        }
                    }

                    result.SessionCommands.Add(new PutCommandDataWithBlittableJson(entity.Value.Id, changeVector, entity.Value.ChangeVector, document, forceRevisionCreationStrategy));
                }
            }
        }

        private static void ThrowMissingDocumentMetadata(BlittableJsonReaderObject document)
        {
            throw new InvalidOperationException("Missing metadata in document. Unable to find " + Constants.Documents.Metadata.Key + " in " + document);
        }

        private static void ThrowInvalidDeletedDocumentWithDeferredCommand(ICommandData resultCommand)
        {
            throw new InvalidOperationException(
                $"Cannot perform save because document {resultCommand.Id} has been deleted by the session and is also taking part in deferred {resultCommand.Type} command");
        }

        private static void ThrowInvalidModifiedDocumentWithDeferredCommand(ICommandData resultCommand)
        {
            throw new InvalidOperationException(
                $"Cannot perform save because document {resultCommand.Id} has been modified by the session and is also taking part in deferred {resultCommand.Type} command");
        }

        private static void ThrowNoDatabase()
        {
            throw new InvalidOperationException(
                $"Cannot open a Session without specifying a name of a database to operate on. Database name can be passed as an argument when Session is being opened or default database can be defined using '{nameof(DocumentStore)}.{nameof(IDocumentStore.Database)}' property.");
        }

        protected bool EntityChanged(BlittableJsonReaderObject newObj, DocumentInfo documentInfo, IDictionary<string, DocumentsChanges[]> changes)
        {
            return BlittableOperation.EntityChanged(newObj, documentInfo, changes);
        }

        public IDictionary<string, DocumentsChanges[]> WhatChanged()
        {
            var changes = new Dictionary<string, DocumentsChanges[]>();

            GetAllEntitiesChanges(changes);
            PrepareForEntitiesDeletion(null, changes);
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
                    using (var document = JsonConverter.ToBlittable(entity.Key, entity.Value))
                    {
                        if (EntityChanged(document, entity.Value, null))
                        {
                            return true;
                        }
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
            using (var document = JsonConverter.ToBlittable(entity, documentInfo))
                return EntityChanged(document, documentInfo, null);
        }

        public void WaitForReplicationAfterSaveChanges(TimeSpan? timeout = null, bool throwOnTimeout = true,
            int replicas = 1, bool majority = false)
        {
            var realTimeout = timeout ?? Conventions.WaitForReplicationAfterSaveChangesTimeout;
            if (_saveChangesOptions == null)
                _saveChangesOptions = new BatchOptions();

            var replicationOptions = new ReplicationBatchOptions
            {
                WaitForReplicas = true,
                Majority = majority,
                NumberOfReplicasToWaitFor = replicas,
                WaitForReplicasTimeout = realTimeout,
                ThrowOnTimeoutInWaitForReplicas = throwOnTimeout
            };

            _saveChangesOptions.ReplicationOptions = replicationOptions;
        }

        public void WaitForIndexesAfterSaveChanges(TimeSpan? timeout = null, bool throwOnTimeout = false,
            string[] indexes = null)
        {
            var realTimeout = timeout ?? Conventions.WaitForIndexesAfterSaveChangesTimeout;
            if (_saveChangesOptions == null)
                _saveChangesOptions = new BatchOptions();

            var indexOptions = new IndexBatchOptions
            {
                WaitForIndexes = true,
                WaitForIndexesTimeout = realTimeout,
                ThrowOnTimeoutInWaitForIndexes = throwOnTimeout,
                WaitForSpecificIndexes = indexes
            };

            _saveChangesOptions.IndexOptions = indexOptions;
        }

        private void GetAllEntitiesChanges(IDictionary<string, DocumentsChanges[]> changes)
        {
            foreach (var pair in DocumentsById)
            {
                UpdateMetadataModifications(pair.Value);
                var newObj = JsonConverter.ToBlittable(pair.Value.Entity, pair.Value);
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
            if (DocumentsByEntity.TryGetValue(entity, out var documentInfo))
            {
                DocumentsByEntity.Evict(entity);
                DocumentsById.Remove(documentInfo.Id);
                _countersByDocId?.Remove(documentInfo.Id);
                _timeSeriesByDocId?.Remove(documentInfo.Id);
            }

            DeletedEntities.Evict(entity);
            JsonConverter.RemoveFromMissing(entity);
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
            _knownMissingIds.Clear();
            _countersByDocId?.Clear();
            DeferredCommands.Clear();
            DeferredCommandsDictionary.Clear();
            ClearClusterSession();
            PendingLazyOperations.Clear();
            JsonConverter.Clear();
        }

        /// <summary>
        ///     Defer commands to be executed on SaveChanges()
        /// </summary>
        /// <param name="command">Command to be executed</param>
        /// <param name="commands">Array of commands to be executed.</param>
        public void Defer(ICommandData command, params ICommandData[] commands)
        {
            // The signature here is like this in order to avoid calling 'Defer()' without any parameter.

            DeferredCommands.Add(command);
            DeferInternal(command);

            if (commands != null)
                Defer(commands);
        }

        /// <summary>
        /// Defer commands to be executed on SaveChanges()
        /// </summary>
        /// <param name="commands">The commands to be executed</param>
        public void Defer(ICommandData[] commands)
        {
            DeferredCommands.AddRange(commands);
            foreach (var command in commands)
            {
                DeferInternal(command);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeferInternal(ICommandData command)
        {
            if (command.Type == CommandType.BatchPATCH)
            {
                var batchPathCommand = (BatchPatchCommandData)command;
                foreach (var kvp in batchPathCommand.Ids)
                    AddCommand(kvp.Id, CommandType.PATCH, command.Name);

                return;
            }

            AddCommand(command.Id, command.Type, command.Name);

            void AddCommand(string id, CommandType commandType, string commandName)
            {
                DeferredCommandsDictionary[(id, commandType, commandName)] = command;
                DeferredCommandsDictionary[(id, CommandType.ClientAnyCommand, null)] = command;
                if (commandType != CommandType.AttachmentPUT &&
                    commandType != CommandType.AttachmentDELETE &&
                    commandType != CommandType.AttachmentCOPY &&
                    commandType != CommandType.AttachmentMOVE &&
                    commandType != CommandType.Counters &&
                    commandType != CommandType.TimeSeries &&
                    commandType != CommandType.TimeSeriesCopy)
                    DeferredCommandsDictionary[(id, CommandType.ClientModifyDocumentCommand, null)] = command;
            }
        }

        public void AssertNotDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException("session");
        }

        private void Dispose(bool isDisposing)
        {
            if (_isDisposed)
                return;

            ExceptionDispatchInfo edi = null;

            try
            {
                OnSessionDisposing?.Invoke(this, new SessionDisposingEventArgs(this));
            }
            catch (Exception e)
            {
                edi = ExceptionDispatchInfo.Capture(e);
            }

            var asyncTasksCounter = Interlocked.Read(ref _asyncTasksCounter);
            if (asyncTasksCounter != 0)
            {
                _forTestingPurposes?.OnSessionDisposeAboutToThrowDueToRunningAsyncTask?.Invoke();

                throw new InvalidOperationException($"Disposing session with active async task is forbidden, please make sure that all asynchronous session methods returning Task are awaited. Number of active async tasks: {asyncTasksCounter}");
            }

            _isDisposed = true;

            if (isDisposing && RunningOn.FinalizerThread == false)
            {
                GC.SuppressFinalize(this);

                _releaseOperationContext?.Dispose();
            }
            else
            {
                // when we are disposed from the finalizer then we have to dispose the context immediately instead of returning it to the pool because
                // the finalizer of ArenaMemoryAllocator could be already called so we cannot return such context to the pool (RavenDB-7571)

                Context?.Dispose();
            }

            edi?.Throw();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public virtual void Dispose()
        {
            Dispose(true);
        }

        public void RegisterMissing(string id)
        {
            if (NoTracking)
                return;

            _knownMissingIds.Add(id);
        }

        public void RegisterMissing(IEnumerable<string> ids)
        {
            if (NoTracking)
                return;

            _knownMissingIds.UnionWith(ids);
        }

        internal void RegisterIncludes(BlittableJsonReaderObject includes)
        {
            if (NoTracking)
                return;

            if (includes == null)
                return;

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < includes.Count; i++)
            {
                includes.GetPropertyByIndex(i, ref propertyDetails);

                if (propertyDetails.Value == null)
                    continue;

                var json = (BlittableJsonReaderObject)propertyDetails.Value;

                var newDocumentInfo = DocumentInfo.GetNewDocumentInfo(json);
                if (newDocumentInfo.Metadata.TryGetConflict(out var conflict) && conflict)
                    continue;

                IncludedDocumentsById[newDocumentInfo.Id] = newDocumentInfo;
            }
        }

        public void RegisterMissingIncludes(BlittableJsonReaderArray results, BlittableJsonReaderObject includes, ICollection<string> includePaths)
        {
            if (NoTracking)
                return;

            if (includePaths == null || includePaths.Count == 0)
                return;

            foreach (BlittableJsonReaderObject result in results)
            {
                foreach (var include in includePaths)
                {
                    if (include == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
                        continue;

                    IncludesUtil.Include(result, include, id =>
                    {
                        if (id == null)
                            return;

                        if (IsLoaded(id))
                            return;

                        if (includes.TryGet(id, out BlittableJsonReaderObject document))
                        {
                            var metadata = document.GetMetadata();
                            if (metadata.TryGetConflict(out var conflict) && conflict)
                                return;
                        }

                        RegisterMissing(id);
                    });
                }
            }
        }

        internal void RegisterCounters(BlittableJsonReaderObject resultCounters, string[] ids, string[] countersToInclude, bool gotAll)
        {
            if (NoTracking)
                return;

            if (resultCounters == null || resultCounters.Count == 0)
            {
                if (gotAll)
                {
                    foreach (var id in ids)
                    {
                        SetGotAllCountersForDocument(id);
                    }

                    return;
                }
            }
            else
            {
                RegisterCountersInternal(resultCounters, countersToInclude: null, fromQueryResult: false, gotAll: gotAll);
            }

            RegisterMissingCounters(ids, countersToInclude);
        }

        internal void RegisterCounters(BlittableJsonReaderObject resultCounters, Dictionary<string, string[]> countersToInclude)
        {
            if (NoTracking)
                return;

            if (resultCounters == null || resultCounters.Count == 0)
            {
                SetGotAllInCacheIfNeeded(countersToInclude);
            }
            else
            {
                RegisterCountersInternal(resultCounters, countersToInclude);
            }

            RegisterMissingCounters(countersToInclude);
        }

        private void RegisterCountersInternal(BlittableJsonReaderObject resultCounters, Dictionary<string, string[]> countersToInclude, bool fromQueryResult = true, bool gotAll = false)
        {
            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < resultCounters.Count; i++)
            {
                resultCounters.GetPropertyByIndex(i, ref propertyDetails);
                if (propertyDetails.Value == null)
                    continue;

                string[] counters = { };

                if (fromQueryResult)
                {
                    gotAll = countersToInclude.TryGetValue(propertyDetails.Name, out counters) &&
                             counters.Length == 0;
                }

                var bjra = (BlittableJsonReaderArray)propertyDetails.Value;
                if (bjra.Length == 0 && gotAll == false)
                {
                    if (CountersByDocId.TryGetValue(propertyDetails.Name, out var cache) == false)
                        continue;

                    if (counters == null)
                        continue;

                    foreach (var counter in counters)
                    {
                        cache.Values.Remove(counter);
                    }

                    CountersByDocId[propertyDetails.Name] = cache;
                    continue;
                }

                RegisterCountersForDocument(propertyDetails.Name, gotAll, bjra, countersToInclude);
            }
        }

        private void RegisterCountersForDocument(string id, bool gotAll, BlittableJsonReaderArray counters, Dictionary<string, string[]> countersToInclude)
        {
            if (CountersByDocId.TryGetValue(id, out var cache) == false)
            {
                cache.Values = new Dictionary<string, long?>(StringComparer.OrdinalIgnoreCase);
            }

            var deletedCounters = cache.Values.Count == 0
                ? new HashSet<string>()
                : countersToInclude[id].Length == 0 // IncludeAllCounters
                    ? new HashSet<string>(cache.Values.Keys)
                    : new HashSet<string>(countersToInclude[id]);

            foreach (BlittableJsonReaderObject counterBlittable in counters)
            {
                if (counterBlittable == null ||
                    counterBlittable.TryGet(nameof(CounterDetail.CounterName), out string name) == false ||
                    counterBlittable.TryGet(nameof(CounterDetail.TotalValue), out long value) == false)
                    continue;

                cache.Values[name] = value;
                deletedCounters.Remove(name);
            }

            if (deletedCounters.Count > 0)
            {
                foreach (var name in deletedCounters)
                {
                    cache.Values.Remove(name);
                }
            }

            cache.GotAll = gotAll;
            CountersByDocId[id] = cache;
        }

        private void SetGotAllInCacheIfNeeded(Dictionary<string, string[]> countersToInclude)
        {
            foreach (var kvp in countersToInclude)
            {
                if (kvp.Value.Length != 0)
                    continue;

                SetGotAllCountersForDocument(kvp.Key);
            }
        }

        private void SetGotAllCountersForDocument(string id)
        {
            if (CountersByDocId.TryGetValue(id, out var cache) == false)
            {
                cache.Values = new Dictionary<string, long?>(StringComparer.OrdinalIgnoreCase);
            }

            cache.GotAll = true;
            CountersByDocId[id] = cache;
        }

        private void RegisterMissingCounters(Dictionary<string, string[]> countersToInclude)
        {
            if (countersToInclude == null)
                return;

            foreach (var kvp in countersToInclude)
            {
                if (CountersByDocId.TryGetValue(kvp.Key, out var cache) == false)
                {
                    cache.Values = new Dictionary<string, long?>(StringComparer.OrdinalIgnoreCase);
                    CountersByDocId.Add(kvp.Key, cache);
                }

                foreach (var counter in kvp.Value)
                {
                    if (cache.Values.ContainsKey(counter))
                        continue;

                    cache.Values[counter] = null;
                }
            }
        }

        private void RegisterMissingCounters(string[] ids, string[] countersToInclude)
        {
            if (countersToInclude == null)
                return;

            foreach (var counter in countersToInclude)
            {
                foreach (var id in ids)
                {
                    if (CountersByDocId.TryGetValue(id, out var cache) == false)
                    {
                        cache.Values = new Dictionary<string, long?>(StringComparer.OrdinalIgnoreCase);
                        CountersByDocId.Add(id, cache);
                    }

                    if (cache.Values.ContainsKey(counter))
                        continue;

                    cache.Values[counter] = null;
                }
            }
        }

        internal void RegisterTimeSeries(BlittableJsonReaderObject resultTimeSeries)
        {
            if (NoTracking || resultTimeSeries == null)
                return;

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < resultTimeSeries.Count; i++)
            {
                resultTimeSeries.GetPropertyByIndex(i, ref propertyDetails);
                if (propertyDetails.Value == null)
                    continue;

                var id = propertyDetails.Name;

                if (TimeSeriesByDocId.TryGetValue(id, out var cache) == false)
                {
                    cache = new Dictionary<string, List<TimeSeriesRangeResult>>(StringComparer.OrdinalIgnoreCase);
                }

                if (!(propertyDetails.Value is BlittableJsonReaderObject timeseriesRangesByName))
                    throw new InvalidDataException($"Unable to read time series range results on document : '{id}'.");

                var innerBlittablePropDetails = new BlittableJsonReaderObject.PropertyDetails();
                for (int j = 0; j < timeseriesRangesByName.Count; j++)
                {
                    timeseriesRangesByName.GetPropertyByIndex(j, ref innerBlittablePropDetails);
                    if (innerBlittablePropDetails.Value == null)
                        continue;

                    var name = innerBlittablePropDetails.Name;

                    if (!(innerBlittablePropDetails.Value is BlittableJsonReaderArray timeseriesRanges))
                        throw new InvalidDataException($"Unable to read time series range results on document : '{id}', time series : '{name}' .");

                    foreach (BlittableJsonReaderObject blittableRange in timeseriesRanges)
                    {
                        if (JsonDeserializationClient.CacheForTimeSeriesRangeResult.TryGetValue(typeof(TimeSeriesEntry), out var func) == false)
                        {
                            func = JsonDeserializationBase.GenerateJsonDeserializationRoutine<TimeSeriesRangeResult<TimeSeriesEntry>>();
                            JsonDeserializationClient.CacheForTimeSeriesRangeResult.TryAdd(typeof(TimeSeriesEntry), func);
                        }

                        var newRange = (TimeSeriesRangeResult<TimeSeriesEntry>)func(blittableRange);

                        AddToCache(cache, newRange, name);
                    }
                }

                TimeSeriesByDocId[id] = cache;
            }
        }

        private static void AddToCache(Dictionary<string, List<TimeSeriesRangeResult>> cache, TimeSeriesRangeResult newRange, string name)
        {
            if (cache.TryGetValue(name, out var localRanges) == false ||
                localRanges.Count == 0)
            {
                // no local ranges in cache for this series

                cache[name] = new List<TimeSeriesRangeResult>
                {
                    newRange
                };
                return;
            }

            if (localRanges[0].From > newRange.To || localRanges[localRanges.Count - 1].To < newRange.From)
            {
                // the entire range [from, to] is out of cache bounds

                var index = localRanges[0].From > newRange.To ? 0 : localRanges.Count;
                localRanges.Insert(index, newRange);
                return;
            }

            int toRangeIndex;
            var fromRangeIndex = -1;
            var rangeAlreadyInCache = false;

            for (toRangeIndex = 0; toRangeIndex < localRanges.Count; toRangeIndex++)
            {
                if (localRanges[toRangeIndex].From <= newRange.From)
                {
                    if (localRanges[toRangeIndex].To >= newRange.To)
                    {
                        rangeAlreadyInCache = true;
                        break;
                    }

                    fromRangeIndex = toRangeIndex;
                    continue;
                }

                if (localRanges[toRangeIndex].To >= newRange.To)
                    break;
            }

            if (rangeAlreadyInCache)
            {
                UpdateExistingRange(localRanges[toRangeIndex], newRange);
                return;
            }

            var mergedValues = MergeRanges(fromRangeIndex, toRangeIndex, localRanges, newRange);
            AddToCache(name, newRange.From, newRange.To, fromRangeIndex, toRangeIndex, localRanges, cache, mergedValues);
        }

        internal static void AddToCache(
            string timeseries,
            DateTime from,
            DateTime to,
            int fromRangeIndex,
            int toRangeIndex,
            List<TimeSeriesRangeResult> ranges,
            Dictionary<string, List<TimeSeriesRangeResult>> cache,
            TimeSeriesEntry[] values)
        {
            if (fromRangeIndex == -1)
            {
                // didn't find a 'fromRange' => all ranges in cache start after 'from'

                if (toRangeIndex == ranges.Count)
                {
                    // the requested range [from, to] contains all the ranges that are in cache

                    // e.g. if cache is : [[2,3], [4,5], [7, 10]]
                    // and the requested range is : [1, 15]
                    // after this action cache will be : [[1, 15]]

                    cache[timeseries] = new List<TimeSeriesRangeResult>
                    {
                        new TimeSeriesRangeResult
                        {
                            From = from,
                            To = to,
                            Entries = values
                        }
                    };

                    return;
                }

                if (ranges[toRangeIndex].From > to)
                {
                    // requested range ends before 'toRange' starts
                    // remove all ranges that come before 'toRange' from cache
                    // add the new range at the beginning of the list

                    // e.g. if cache is : [[2,3], [4,5], [7,10]]
                    // and the requested range is : [1,6]
                    // after this action cache will be : [[1,6], [7,10]]

                    ranges.RemoveRange(0, toRangeIndex);
                    ranges.Insert(0, new TimeSeriesRangeResult
                    {
                        From = from,
                        To = to,
                        Entries = values
                    });

                    return;
                }

                // the requested range ends inside 'toRange'
                // merge the result from server into 'toRange'
                // remove all ranges that come before 'toRange' from cache

                // e.g. if cache is : [[2,3], [4,5], [7,10]]
                // and the requested range is : [1,8]
                // after this action cache will be : [[1,10]]

                ranges[toRangeIndex].From = from;
                ranges[toRangeIndex].Entries = values;
                ranges.RemoveRange(0, toRangeIndex);

                return;
            }

            // found a 'fromRange'

            if (toRangeIndex == ranges.Count)
            {
                // didn't find a 'toRange' => all the ranges in cache end before 'to'

                if (ranges[fromRangeIndex].To < from)
                {
                    // requested range starts after 'fromRange' ends,
                    // so it needs to be placed right after it
                    // remove all the ranges that come after 'fromRange' from cache
                    // add the merged values as a new range at the end of the list

                    // e.g. if cache is : [[2,3], [5,6], [7,10]]
                    // and the requested range is : [4,12]
                    // then 'fromRange' is : [2,3]
                    // after this action cache will be : [[2,3], [4,12]]

                    ranges.RemoveRange(fromRangeIndex + 1, ranges.Count - fromRangeIndex - 1);
                    ranges.Add(new TimeSeriesRangeResult
                    {
                        From = from,
                        To = to,
                        Entries = values
                    });

                    return;
                }

                // the requested range starts inside 'fromRange'
                // merge result into 'fromRange'
                // remove all the ranges from cache that come after 'fromRange'

                // e.g. if cache is : [[2,3], [4,6], [7,10]]
                // and the requested range is : [5,12]
                // then 'fromRange' is [4,6]
                // after this action cache will be : [[2,3], [4,12]]

                ranges[fromRangeIndex].To = to;
                ranges[fromRangeIndex].Entries = values;
                ranges.RemoveRange(fromRangeIndex + 1, ranges.Count - fromRangeIndex - 1);

                return;
            }

            // found both 'fromRange' and 'toRange'
            // the requested range is inside cache bounds

            if (ranges[fromRangeIndex].To < from)
            {
                // requested range starts after 'fromRange' ends

                if (ranges[toRangeIndex].From > to)
                {
                    // requested range ends before 'toRange' starts

                    // remove all ranges in between 'fromRange' and 'toRange'
                    // place new range in between 'fromRange' and 'toRange'

                    // e.g. if cache is : [[2,3], [5,6], [7,8], [10,12]]
                    // and the requested range is : [4,9]
                    // then 'fromRange' is [2,3] and 'toRange' is [10,12]
                    // after this action cache will be : [[2,3], [4,9], [10,12]]

                    ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                    ranges.Insert(fromRangeIndex + 1, new TimeSeriesRangeResult
                    {
                        From = from,
                        To = to,
                        Entries = values
                    });

                    return;
                }

                // requested range ends inside 'toRange'

                // merge the new range into 'toRange'
                // remove all ranges in between 'fromRange' and 'toRange'

                // e.g. if cache is : [[2,3], [5,6], [7,10]]
                // and the requested range is : [4,9]
                // then 'fromRange' is [2,3] and 'toRange' is [7,10]
                // after this action cache will be : [[2,3], [4,10]]

                ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);
                ranges[toRangeIndex].From = from;
                ranges[toRangeIndex].Entries = values;

                return;
            }

            // the requested range starts inside 'fromRange'

            if (ranges[toRangeIndex].From > to)
            {
                // requested range ends before 'toRange' starts

                // remove all ranges in between 'fromRange' and 'toRange'
                // merge new range into 'fromRange'

                // e.g. if cache is : [[2,4], [5,6], [8,10]]
                // and the requested range is : [3,7]
                // then 'fromRange' is [2,4] and 'toRange' is [8,10]
                // after this action cache will be : [[2,7], [8,10]]

                ranges[fromRangeIndex].To = to;
                ranges[fromRangeIndex].Entries = values;
                ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex - 1);

                return;
            }

            // the requested range starts inside 'fromRange'
            // and ends inside 'toRange'

            // merge all ranges in between 'fromRange' and 'toRange'
            // into a single range [fromRange.From, toRange.To]

            // e.g. if cache is : [[2,4], [5,6], [8,10]]
            // and the requested range is : [3,9]
            // then 'fromRange' is [2,4] and 'toRange' is [8,10]
            // after this action cache will be : [[2,10]]

            ranges[fromRangeIndex].To = ranges[toRangeIndex].To;
            ranges[fromRangeIndex].Entries = values;

            ranges.RemoveRange(fromRangeIndex + 1, toRangeIndex - fromRangeIndex);
        }

        private static TimeSeriesEntry[] MergeRanges(int fromRangeIndex, int toRangeIndex, List<TimeSeriesRangeResult> localRanges, TimeSeriesRangeResult newRange)
        {
            var mergedValues = new List<TimeSeriesEntry>();

            if (fromRangeIndex != -1 &&
                localRanges[fromRangeIndex].To >= newRange.From)
            {
                foreach (var val in localRanges[fromRangeIndex].Entries)
                {
                    if (val.Timestamp >= newRange.From)
                        break;
                    mergedValues.Add(val);
                }
            }

            mergedValues.AddRange(newRange.Entries);

            if (toRangeIndex < localRanges.Count && localRanges[toRangeIndex].From <= newRange.To)
            {
                foreach (var val in localRanges[toRangeIndex].Entries)
                {
                    if (val.Timestamp <= newRange.To)
                        continue;
                    mergedValues.Add(val);
                }
            }

            return mergedValues.ToArray();
        }

        private static void UpdateExistingRange(TimeSeriesRangeResult localRange, TimeSeriesRangeResult newRange)
        {
            var newValues = new List<TimeSeriesEntry>();
            int index;
            for (index = 0; index < localRange.Entries.Length; index++)
            {
                if (localRange.Entries[index].Timestamp >= newRange.From)
                    break;

                newValues.Add(localRange.Entries[index]);
            }

            newValues.AddRange(newRange.Entries);

            for (int j = index; j < localRange.Entries.Length; j++)
            {
                if (localRange.Entries[j].Timestamp <= newRange.To)
                    continue;

                newValues.Add(localRange.Entries[j]);
            }

            localRange.Entries = newValues.ToArray();
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
            if (result.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
                metadata.TryGet(Constants.Documents.Metadata.Id, out string id) == false)
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

            if (metadata.TryGet(Constants.Documents.Metadata.Collection, out string collectionName) == false)
                return;

            var idPropName = Conventions.FindIdentityPropertyNameFromCollectionName(collectionName);

            result.Modifications = new DynamicJsonValue
            {
                [idPropName] = id // this is being read by BlittableJsonReader for additional properties on the object
            };
        }

        private void HandleInternalMetadata(BlittableJsonReaderArray values)
        {
            foreach (var nested in values)
            {
                if (nested is BlittableJsonReaderObject bObject)
                    HandleInternalMetadata(bObject);
                var bArray = nested as BlittableJsonReaderArray;
                if (bArray == null)
                    continue;
                HandleInternalMetadata(bArray);
            }
        }

        private object DeserializeFromTransformer(Type entityType, string id, BlittableJsonReaderObject document, bool trackEntity)
        {
            HandleInternalMetadata(document);
            return JsonConverter.FromBlittable(entityType, ref document, id, trackEntity);
        }

        public bool CheckIfIdAlreadyIncluded(string[] ids, KeyValuePair<string, Type>[] includes)
        {
            return CheckIfIdAlreadyIncluded(ids, includes.Select(x => x.Key));
        }

        public bool CheckIfIdAlreadyIncluded(string[] ids, IEnumerable<string> includes)
        {
            foreach (var id in ids)
            {
                if (_knownMissingIds.Contains(id))
                    continue;

                // Check if document was already loaded, the check if we've received it through include
                if (DocumentsById.TryGetValue(id, out DocumentInfo documentInfo) == false &&
                    IncludedDocumentsById.TryGetValue(id, out documentInfo) == false)
                    return false;

                if ((documentInfo.Entity == null) && (documentInfo.Document == null))
                    return false;

                if (includes == null)
                    continue;

                foreach (var include in includes)
                {
                    var hasAll = true;
                    IncludesUtil.Include(documentInfo.Document, include, s => { hasAll &= IsLoaded(s); });

                    if (hasAll == false)
                        return false;
                }
            }

            return true;
        }

        protected void RefreshInternal<T>(T entity, RavenCommand<GetDocumentsResult> cmd, DocumentInfo documentInfo)
        {
            var document = (BlittableJsonReaderObject)cmd.Result.Results[0];
            if (document == null)
                throw new InvalidOperationException("Document '" + documentInfo.Id +
                                                    "' no longer exists and was probably deleted");

            document.TryGetMember(Constants.Documents.Metadata.Key, out object value);
            documentInfo.Metadata = value as BlittableJsonReaderObject;

            if (documentInfo.Metadata != null)
            {
                documentInfo.Metadata.TryGetMember(Constants.Documents.Metadata.ChangeVector, out var changeVector);
                documentInfo.ChangeVector = (LazyStringValue)changeVector;
            }

            if (documentInfo.Entity != null && NoTracking == false)
                JsonConverter.RemoveFromMissing(documentInfo.Entity);

            documentInfo.Entity = JsonConverter.FromBlittable<T>(ref document, documentInfo.Id, NoTracking == false);
            documentInfo.Document = document;

            var type = entity.GetType();
            foreach (var property in ReflectionUtil.GetPropertiesAndFieldsFor(type, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
            {
                var prop = property;
                if (prop.DeclaringType != type && prop.DeclaringType != null)
                    prop = prop.DeclaringType.GetProperty(prop.Name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic) ?? property;

                if (!prop.CanWrite() || !prop.CanRead() || prop.GetIndexParameters().Length != 0)
                    continue;
                prop.SetValue(ref entity, prop.GetValue(documentInfo.Entity));
            }

            if (DocumentsById.TryGetValue(documentInfo.Id, out DocumentInfo documentInfoById))
                documentInfoById.Entity = entity;

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

        /// <summary>
        /// Data for a batch command to the server
        /// </summary>
        internal class SaveChangesData
        {
            public readonly List<ICommandData> DeferredCommands;
            public readonly Dictionary<(string, CommandType, string), ICommandData> DeferredCommandsDictionary;
            public readonly List<ICommandData> SessionCommands = new List<ICommandData>();
            public readonly List<object> Entities = new List<object>();
            public readonly BatchOptions Options;
            internal readonly ActionsToRunOnSuccess OnSuccess;

            public SaveChangesData(InMemoryDocumentSessionOperations session)
            {
                DeferredCommands = new List<ICommandData>(session.DeferredCommands);
                DeferredCommandsDictionary = new Dictionary<(string, CommandType, string), ICommandData>(session.DeferredCommandsDictionary);
                Options = session._saveChangesOptions;
                OnSuccess = new ActionsToRunOnSuccess(session);
            }

            internal class ActionsToRunOnSuccess
            {
                private readonly InMemoryDocumentSessionOperations _session;
                private readonly List<string> _documentsByIdToRemove = new List<string>();
                private readonly List<object> _documentsByEntityToRemove = new List<object>();
                private readonly List<(DocumentInfo Info, BlittableJsonReaderObject Document)> _documentInfosToUpdate = new List<(DocumentInfo Info, BlittableJsonReaderObject Document)>();

                private bool _clearDeletedEntities;

                public ActionsToRunOnSuccess(InMemoryDocumentSessionOperations _session)
                {
                    this._session = _session;
                }

                public void RemoveDocumentById(string id)
                {
                    _documentsByIdToRemove.Add(id);
                }

                public void RemoveDocumentByEntity(object entity)
                {
                    _documentsByEntityToRemove.Add(entity);
                }

                public void UpdateEntityDocumentInfo(DocumentInfo documentInfo, BlittableJsonReaderObject document)
                {
                    _documentInfosToUpdate.Add((documentInfo, document));
                }

                public void ClearSessionStateAfterSuccessfulSaveChanges()
                {
                    foreach (var id in _documentsByIdToRemove)
                    {
                        _session.DocumentsById.Remove(id);
                    }

                    foreach (var entity in _documentsByEntityToRemove)
                    {
                        _session.DocumentsByEntity.Remove(entity);
                    }

                    foreach (var (info, document) in _documentInfosToUpdate)
                    {
                        info.IsNewDocument = false;
                        info.Document = document;
                    }

                    if (_clearDeletedEntities)
                        _session.DeletedEntities.Clear();

                    _session.DeferredCommands.Clear();
                    _session.DeferredCommandsDictionary.Clear();
                }

                public void ClearDeletedEntities()
                {
                    _clearDeletedEntities = true;
                }
            }
        }

        protected void UpdateSessionAfterSaveChanges(BatchCommandResult result)
        {
            var returnedTransactionIndex = result.TransactionIndex;
            _documentStore.SetLastTransactionIndex(DatabaseName, returnedTransactionIndex);
            _sessionInfo.LastClusterTransactionIndex = returnedTransactionIndex;
        }

        internal void OnBeforeConversionToDocumentInvoke(string id, object entity)
        {
            OnBeforeConversionToDocument?.Invoke(this, new BeforeConversionToDocumentEventArgs(this, id, entity));
        }

        internal void OnAfterConversionToDocumentInvoke(string id, object entity, ref BlittableJsonReaderObject document)
        {
            var onAfterConversionToDocument = OnAfterConversionToDocument;
            if (onAfterConversionToDocument != null)
            {
                var args = new AfterConversionToDocumentEventArgs(this, id, entity, document);
                onAfterConversionToDocument.Invoke(this, args);

                if (args.Document != null && ReferenceEquals(args.Document, document) == false)
                    document = args.Document;
            }
        }

        internal void OnBeforeConversionToEntityInvoke(string id, Type type, ref BlittableJsonReaderObject document)
        {
            var onBeforeConversionToEntity = OnBeforeConversionToEntity;
            if (onBeforeConversionToEntity != null)
            {
                var args = new BeforeConversionToEntityEventArgs(this, id, type, document);
                onBeforeConversionToEntity.Invoke(this, args);

                if (args.Document != null && ReferenceEquals(args.Document, document) == false)
                    document = args.Document;
            }
        }

        internal void OnAfterConversionToEntityInvoke(string id, BlittableJsonReaderObject document, object entity)
        {
            OnAfterConversionToEntity?.Invoke(this, new AfterConversionToEntityEventArgs(this, id, document, entity));
        }

        internal void OnAfterSaveChangesInvoke(AfterSaveChangesEventArgs args)
        {
            OnAfterSaveChanges?.Invoke(this, args);
        }

        internal void OnBeforeQueryInvoke(BeforeQueryEventArgs args)
        {
            OnBeforeQuery?.Invoke(this, args);
        }

        protected (string IndexName, string CollectionName) ProcessQueryParameters(Type type, string indexName, string collectionName, DocumentConventions conventions)
        {
            var isIndex = string.IsNullOrWhiteSpace(indexName) == false;
            var isCollection = string.IsNullOrWhiteSpace(collectionName) == false;

            if (isIndex && isCollection)
                throw new InvalidOperationException(
                    $"Parameters '{nameof(indexName)}' and '{nameof(collectionName)}' are mutually exclusive. Please specify only one of them.");

            if (isIndex == false && isCollection == false)
                collectionName = Conventions.GetCollectionName(type) ?? Constants.Documents.Collections.AllDocumentsCollection;

            return (indexName, collectionName);
        }

        private TestingStuff _forTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (_forTestingPurposes != null)
                return _forTestingPurposes;

            return _forTestingPurposes = new TestingStuff();
        }

        internal class TestingStuff
        {
            internal Action OnSessionDisposeAboutToThrowDueToRunningAsyncTask;

            internal IDisposable CallOnSessionDisposeAboutToThrowDueToRunningAsyncTask(Action action)
            {
                OnSessionDisposeAboutToThrowDueToRunningAsyncTask = action;

                return new DisposableAction(() => OnSessionDisposeAboutToThrowDueToRunningAsyncTask = null);
            }
        }

        internal void OnBeforeDeleteInvoke(BeforeDeleteEventArgs beforeDeleteEventArgs)
        {
            OnBeforeDelete?.Invoke(this, beforeDeleteEventArgs);
        }

        internal StreamResult<T> CreateStreamResult<T>(
            BlittableJsonReaderObject json,
            FieldsToFetchToken fieldsToFetch,
            bool isProjectInto)
        {
            var metadata = json.GetMetadata();
            var changeVector = metadata.GetChangeVector();
            //MapReduce indexes return reduce results that don't have @id property
            metadata.TryGetId(out string id);

            return new StreamResult<T>
            {
                ChangeVector = changeVector,
                Id = id,
                Metadata = new MetadataAsDictionary(metadata),
                Document = QueryOperation.Deserialize<T>(id, json, metadata, fieldsToFetch, true, this, isProjectInto)
            };
        }

        internal TimeSeriesStreamResult<T> CreateTimeSeriesStreamResult<T>(StreamOperation.YieldStreamResults enumerator) where T : ITimeSeriesQueryStreamResult, new()
        {
            var json = enumerator.Current;
            var metadata = json.GetMetadata();
            var changeVector = metadata.GetChangeVector();
            //MapReduce indexes return reduce results that don't have @id property
            metadata.TryGetId(out string id);

            var result = new TimeSeriesStreamResult<T>
            {
                ChangeVector = changeVector,
                Id = id,
                Metadata = new MetadataAsDictionary(metadata),
                Result = new T()
            };
            enumerator.ExposeTimeSeriesStream(result.Result);

            return result;
        }
    }

    internal struct AsyncTaskHolder : IDisposable
    {
        private readonly InMemoryDocumentSessionOperations _session;

        public AsyncTaskHolder(InMemoryDocumentSessionOperations session)
        {
            _session = session;

            if (Interlocked.Increment(ref _session._asyncTasksCounter) > 1)
            {
                throw new InvalidOperationException("Concurrent usage of async tasks in async session is forbidden, please make sure that async session doesn't execute async methods concurrently.");
            }
        }

        public void Dispose()
        {
            Interlocked.Decrement(ref _session._asyncTasksCounter);
        }
    }

    internal class DocumentsByEntityHolder
    {
        private readonly Dictionary<object, DocumentInfo> _documentsByEntity = new Dictionary<object, DocumentInfo>(ObjectReferenceEqualityComparer<object>.Default);

        private Dictionary<object, DocumentInfo> _onBeforeStoreDocumentsByEntity;

        private bool _prepareEntitiesPuts;

        public int Count => _documentsByEntity.Count + _onBeforeStoreDocumentsByEntity?.Count ?? 0;

        public void Remove(object entity)
        {
            _documentsByEntity.Remove(entity);
            _onBeforeStoreDocumentsByEntity?.Remove(entity);
        }

        public void Evict(object entity)
        {
            if (_prepareEntitiesPuts)
                throw new InvalidOperationException("Cannot Evict entity during OnBeforeStore");

            _documentsByEntity.Remove(entity);
        }

        public void Add(object entity, DocumentInfo documentInfo)
        {
            if (_prepareEntitiesPuts == false)
            {
                _documentsByEntity.Add(entity, documentInfo);
                return;
            }

            CreateOnBeforeStoreDocumentsByEntityIfNeeded();
            _onBeforeStoreDocumentsByEntity.Add(entity, documentInfo);
        }

        public DocumentInfo this[object obj]
        {
            set
            {
                if (_prepareEntitiesPuts == false)
                {
                    _documentsByEntity[obj] = value;
                }
                else
                {
                    CreateOnBeforeStoreDocumentsByEntityIfNeeded();
                    _onBeforeStoreDocumentsByEntity[obj] = value;
                }
            }
        }

        private void CreateOnBeforeStoreDocumentsByEntityIfNeeded()
        {
            if (_onBeforeStoreDocumentsByEntity != null)
                return;

            _onBeforeStoreDocumentsByEntity = new Dictionary<object, DocumentInfo>(ObjectReferenceEqualityComparer<object>.Default);
        }

        public void Clear()
        {
            _documentsByEntity.Clear();
            _onBeforeStoreDocumentsByEntity?.Clear();
        }

        public bool TryGetValue(object entity, out DocumentInfo documentInfo)
        {
            if (_documentsByEntity.TryGetValue(entity, out documentInfo))
                return true;

            return _onBeforeStoreDocumentsByEntity != null && _onBeforeStoreDocumentsByEntity.TryGetValue(entity, out documentInfo);
        }

        public IEnumerator<DocumentsByEntityEnumeratorResult> GetEnumerator()
        {
            foreach (var doc in _documentsByEntity)
            {
                yield return new DocumentsByEntityEnumeratorResult
                {
                    Key = doc.Key,
                    Value = doc.Value,
                    ExecuteOnBeforeStore = true
                };
            }

            if (_onBeforeStoreDocumentsByEntity != null)
            {
                foreach (var doc in _onBeforeStoreDocumentsByEntity)
                {
                    yield return new DocumentsByEntityEnumeratorResult
                    {
                        Key = doc.Key,
                        Value = doc.Value,
                        ExecuteOnBeforeStore = false
                    };
                }
            }
        }

        public IDisposable PrepareEntitiesPuts()
        {
            _prepareEntitiesPuts = true;

            return new DisposableAction(() => _prepareEntitiesPuts = false);
        }

        internal class DocumentsByEntityEnumeratorResult
        {
            public object Key { get; set; }

            public DocumentInfo Value { get; set; }

            public bool ExecuteOnBeforeStore { get; set; }
        }
    }

    internal class DeletedEntitiesHolder
    {
        private readonly HashSet<object> _deletedEntities = new HashSet<object>(ObjectReferenceEqualityComparer<object>.Default);

        private HashSet<object> _onBeforeDeletedEntities;

        private bool _prepareEntitiesDeletes;

        public int Count => _deletedEntities.Count + (_onBeforeDeletedEntities?.Count ?? 0);

        public void Add(object entity)
        {
            if (_prepareEntitiesDeletes)
            {
                if (_onBeforeDeletedEntities == null)
                    _onBeforeDeletedEntities = new HashSet<object>(ObjectReferenceEqualityComparer<object>.Default);

                _onBeforeDeletedEntities.Add(entity);
                return;
            }

            _deletedEntities.Add(entity);
        }

        public void Remove(object entity)
        {
            _deletedEntities.Remove(entity);
            _onBeforeDeletedEntities?.Remove(entity);
        }

        public void Evict(object entity)
        {
            if (_prepareEntitiesDeletes)
                throw new InvalidOperationException("Cannot Evict entity during OnBeforeDelete");

            _deletedEntities.Remove(entity);
        }

        public bool Contains(object entity)
        {
            if (_deletedEntities.Contains(entity))
                return true;

            if (_onBeforeDeletedEntities == null)
                return false;

            return _onBeforeDeletedEntities.Contains(entity);
        }

        public void Clear()
        {
            _deletedEntities.Clear();
            _onBeforeDeletedEntities?.Clear();
        }

        public IEnumerator<DeletedEntitiesEnumeratorResult> GetEnumerator()
        {
            foreach (var entity in _deletedEntities)
            {
                yield return new DeletedEntitiesEnumeratorResult
                {
                    Entity = entity,
                    ExecuteOnBeforeDelete = true
                };
            }

            if (_onBeforeDeletedEntities != null)
            {
                foreach (var entity in _onBeforeDeletedEntities)
                {
                    yield return new DeletedEntitiesEnumeratorResult
                    {
                        Entity = entity,
                        ExecuteOnBeforeDelete = false
                    };
                }
            }
        }

        public IDisposable PrepareEntitiesDeletes()
        {
            _prepareEntitiesDeletes = true;

            return new DisposableAction(() => _prepareEntitiesDeletes = false);
        }

        public class DeletedEntitiesEnumeratorResult
        {
            public object Entity { get; set; }

            public bool ExecuteOnBeforeDelete { get; set; }
        }
    }
}
