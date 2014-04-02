//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if !NETFX_CORE
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Document.Batches;
using Raven.Client.Connection;
using Raven.Client.Document.SessionOperations;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Client.Util;
using Raven.Client.WinRT.MissingFromWinRT;
using Raven.Imports.Newtonsoft.Json.Utilities;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSession : InMemoryDocumentSessionOperations, IDocumentSessionImpl, ITransactionalDocumentSession,
                                   ISyncAdvancedSessionOperation, IDocumentQueryGenerator
    {
 
        /// <summary>
        /// Gets the database commands.
        /// </summary>
        /// <value>The database commands.</value>
        public IDatabaseCommands DatabaseCommands { get; private set; }

        /// <summary>
        /// Access the lazy operations
        /// </summary>
        public ILazySessionOperations Lazily
        {
            get { return this; }
        }

        /// <summary>
        /// Access the eager operations
        /// </summary>
        public IEagerSessionOperations Eagerly
        {
            get { return this; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentSession"/> class.
        /// </summary>
        public DocumentSession(string dbName, DocumentStore documentStore,
                               DocumentSessionListeners listeners,
                               Guid id,
                               IDatabaseCommands databaseCommands)
            : base(dbName, documentStore, listeners, id)
        {
            DatabaseCommands = databaseCommands;
        }

        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate 
        /// property to avoid cluttering the API
        /// </remarks>
        public ISyncAdvancedSessionOperation Advanced
        {
            get { return this; }
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        ILazyLoaderWithInclude<T> ILazySessionOperations.Include<T>(Expression<Func<T, object>> path)
        {
            return new LazyMultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        Lazy<T[]> ILazySessionOperations.Load<T>(params string[] ids)
        {
            return Lazily.Load<T>(ids, null);
        }

        /// <summary>
        /// Loads the specified ids.
        /// </summary>
        Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<string> ids)
        {
            return Lazily.Load<T>(ids, null);
        }

        /// <summary>
        /// Loads the specified id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        Lazy<T> ILazySessionOperations.Load<T>(string id)
        {
            return Lazily.Load(id, (Action<T>)null);
        }

        /// <summary>
        /// Loads the specified ids and a function to call when it is evaluated
        /// </summary>
        public Lazy<T[]> Load<T>(IEnumerable<string> ids, Action<T[]> onEval)
        {
            return LazyLoadInternal(ids.ToArray(), new KeyValuePair<string, Type>[0], onEval);
        }

        /// <summary>
        /// Loads the specified id and a function to call when it is evaluated
        /// </summary>
        public Lazy<T> Load<T>(string id, Action<T> onEval)
        {
            if (IsLoaded(id))
                return new Lazy<T>(() => Load<T>(id));
            var lazyLoadOperation = new LazyLoadOperation<T>(id, new LoadOperation(this, DatabaseCommands.DisableAllCaching, id), handleInternalMetadata: HandleInternalMetadata);
            return AddLazyOperation(lazyLoadOperation, onEval);
        }

        /// <summary>
        /// Loads the specified entities with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        Lazy<T> ILazySessionOperations.Load<T>(ValueType id, Action<T> onEval)
        {
            var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return Lazily.Load(documentKey, onEval);
        }

        Lazy<T[]> ILazySessionOperations.Load<T>(params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Lazily.Load<T>(documentKeys, null);
        }

        Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<ValueType> ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Lazily.Load<T>(documentKeys, null);
        }

        Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<ValueType> ids, Action<T[]> onEval)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return LazyLoadInternal(documentKeys.ToArray(), new KeyValuePair<string, Type>[0], onEval);
        }

        Lazy<TResult> ILazySessionOperations.Load<TTransformer, TResult>(string id)
        {
            var transformer = new TTransformer().TransformerName;
            var ids = new[] { id };
            var lazyLoadOperation = new LazyTransformerLoadOperation<TResult>(ids, transformer,
                                                                          new LoadTransformerOperation(this, transformer, ids),
                                                                          singleResult: true);
            return AddLazyOperation<TResult>(lazyLoadOperation, null);
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TTransformer, TResult>(string[] ids)
        {
            var transformer = new TTransformer().TransformerName;
            var lazyLoadOperation = new LazyTransformerLoadOperation<TResult>(ids, transformer,
                                                                          new LoadTransformerOperation(this, transformer, ids),
                                                                          singleResult: false);
            return AddLazyOperation<TResult[]>(lazyLoadOperation, null);
        }

        /// <summary>
        /// Begin a load while including the specified path 
        /// </summary>
        /// <param name="path">The path.</param>
        ILazyLoaderWithInclude<object> ILazySessionOperations.Include(string path)
        {
            return new LazyMultiLoaderWithInclude<object>(this).Include(path);
        }

        /// <summary>
        /// Loads the specified entities with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        Lazy<T> ILazySessionOperations.Load<T>(ValueType id)
        {
            return Lazily.Load(id, (Action<T>)null);
        }

        /// <summary>
        /// Loads the specified entity with the specified id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        public T Load<T>(string id)
        {
            if (id == null)
                throw new ArgumentNullException("id", "The document id cannot be null");
            if (IsDeleted(id))
                return default(T);
            object existingEntity;

            if (entitiesByKey.TryGetValue(id, out existingEntity))
            {
                return (T)existingEntity;
            }
            JsonDocument value;
            if (includedDocumentsByKey.TryGetValue(id, out value))
            {
                includedDocumentsByKey.Remove(id);
                return TrackEntity<T>(value);
            }

            IncrementRequestCount();
            var loadOperation = new LoadOperation(this, DatabaseCommands.DisableAllCaching, id);
            bool retry;
            do
            {
                loadOperation.LogOperation();
                using (loadOperation.EnterLoadContext())
                {
                    retry = loadOperation.SetResult(DatabaseCommands.Get(id));
                }
            } while (retry);
            return loadOperation.Complete<T>();
        }

        /// <summary>
        /// Loads the specified entities with the specified ids.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public T[] Load<T>(params string[] ids)
        {
            return LoadInternal<T>(ids);
        }

        /// <summary>
        /// Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        public T[] Load<T>(IEnumerable<string> ids)
        {
            return ((IDocumentSessionImpl)this).LoadInternal<T>(ids.ToArray());
        }

        /// <summary>
        /// Loads the specified entity with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public T Load<T>(ValueType id)
        {
            var documentKey = Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false);
            return Load<T>(documentKey);
        }

        /// <summary>
        /// Loads the specified entities with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(1,2,3)
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public T[] Load<T>(params ValueType[] ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Load<T>(documentKeys);
        }

        /// <summary>
        /// Loads the specified entities with the specified id after applying
        /// conventions on the provided id to get the real document id.
        /// </summary>
        /// <remarks>
        /// This method allows you to call:
        /// Load{Post}(new List&lt;int&gt;(){1,2,3})
        /// And that call will internally be translated to 
        /// Load{Post}("posts/1","posts/2","posts/3");
        /// 
        /// Or whatever your conventions specify.
        /// </remarks>
        public T[] Load<T>(IEnumerable<ValueType> ids)
        {
            var documentKeys = ids.Select(id => Conventions.FindFullDocumentKeyFromNonStringIdentifier(id, typeof(T), false));
            return Load<T>(documentKeys);
        }


        private T[] LoadInternal<T>(string[] ids, string transformer, Dictionary<string, RavenJToken> queryInputs = null)
        {
            if (ids.Length == 0)
                return new T[0];

            IncrementRequestCount();

            var multiLoadResult = DatabaseCommands.Get(ids, new string[] { }, transformer, queryInputs);
            return new LoadTransformerOperation(this, transformer, ids).Complete<T>(multiLoadResult);
        }

      

        public T[] LoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes)
        {
            if (ids.Length == 0)
                return new T[0];

            var includePaths = includes != null ? includes.Select(x => x.Key).ToArray() : null;

            IncrementRequestCount();
            var multiLoadOperation = new MultiLoadOperation(this, DatabaseCommands.DisableAllCaching, ids, includes);
            MultiLoadResult multiLoadResult;
            do
            {
                multiLoadOperation.LogOperation();
                using (multiLoadOperation.EnterMultiLoadContext())
                {
                    multiLoadResult = DatabaseCommands.Get(ids, includePaths);
                }
            } while (multiLoadOperation.SetResult(multiLoadResult));

            return multiLoadOperation.Complete<T>();
        }

        public T[] LoadInternal<T>(string[] ids)
        {
            if (ids.Length == 0)
                return new T[0];

            // only load documents that aren't already cached
            var idsOfNotExistingObjects = ids.Where(id => IsLoaded(id) == false && IsDeleted(id) == false)
                                             .Distinct(StringComparer.OrdinalIgnoreCase)
                                             .ToArray();

            if (idsOfNotExistingObjects.Length > 0)
            {
                IncrementRequestCount();
                var multiLoadOperation = new MultiLoadOperation(this, DatabaseCommands.DisableAllCaching, idsOfNotExistingObjects, null);
                MultiLoadResult multiLoadResult;
                do
                {
                    multiLoadOperation.LogOperation();
                    using (multiLoadOperation.EnterMultiLoadContext())
                    {
                        multiLoadResult = DatabaseCommands.Get(idsOfNotExistingObjects, null);
                    }
                } while (multiLoadOperation.SetResult(multiLoadResult));

                multiLoadOperation.Complete<T>();
            }

            return ids.Select(Load<T>).ToArray();
        }

        /// <summary>
        /// Queries the specified index using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <param name="isMapReduce">Whatever we are querying a map/reduce index (modify how we treat identifier properties)</param>
        public IRavenQueryable<T> Query<T>(string indexName, bool isMapReduce = false)
        {
            var ravenQueryStatistics = new RavenQueryStatistics();
            var highlightings = new RavenQueryHighlightings();
            var ravenQueryProvider = new RavenQueryProvider<T>(this, indexName, ravenQueryStatistics, highlightings, DatabaseCommands, null, isMapReduce);
            return new RavenQueryInspector<T>(ravenQueryProvider, ravenQueryStatistics, highlightings, indexName, null, this, DatabaseCommands, null, isMapReduce);
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using Linq.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IRavenQueryable<T> Query<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return Query<T>(indexCreator.IndexName, indexCreator.IsMapReduce);
        }

        /// <summary>
        /// Refreshes the specified entity from Raven server.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The entity.</param>
        public void Refresh<T>(T entity)
        {
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
                throw new InvalidOperationException("Cannot refresh a transient instance");
            IncrementRequestCount();
            var jsonDocument = DatabaseCommands.Get(value.Key);
            if (jsonDocument == null)
                throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");

            value.Metadata = jsonDocument.Metadata;
            value.OriginalMetadata = (RavenJObject)jsonDocument.Metadata.CloneToken();
            value.ETag = jsonDocument.Etag;
            value.OriginalValue = jsonDocument.DataAsJson;
            var newEntity = ConvertToEntity(typeof(T),value.Key, jsonDocument.DataAsJson, jsonDocument.Metadata);
            var type = entity.GetType();
            foreach (var property in type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
            {
                var prop = property;
                if (prop.DeclaringType != type && prop.DeclaringType != null)
                {
                    prop = prop.DeclaringType.GetProperty(prop.Name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                    if (prop == null)
                        prop = property; // shouldn't happen ever...
                }
                if (!prop.CanWrite || !prop.CanRead || prop.GetIndexParameters().Length != 0)
                    continue;
                prop.SetValue(entity, prop.GetValue(newEntity, null), null);
            }
        }

        /// <summary>
        /// Get the json document by key from the store
        /// </summary>
        protected override JsonDocument GetJsonDocument(string documentKey)
        {
            var jsonDocument = DatabaseCommands.Get(documentKey);
            if (jsonDocument == null)
                throw new InvalidOperationException("Document '" + documentKey + "' no longer exists and was probably deleted");
            return jsonDocument;
        }

        protected override string GenerateKey(object entity)
        {
            return Conventions.GenerateDocumentKey(dbName, DatabaseCommands, entity);
        }

        protected override Task<string> GenerateKeyAsync(object entity)
        {
            throw new NotSupportedException("Cannot use async operation in sync session");
        }

        /// <summary>
        /// Begin a load while including the specified path
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public ILoaderWithInclude<object> Include(string path)
        {
            return new MultiLoaderWithInclude<object>(this).Include(path);
        }

        /// <summary>
        /// Begin a load while including the specified path
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public ILoaderWithInclude<T> Include<T>(Expression<Func<T, object>> path)
        {
            return new MultiLoaderWithInclude<T>(this).Include(path);
        }

        /// <summary>
        /// Begin a load while including the specified path
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public ILoaderWithInclude<T> Include<T, TInclude>(Expression<Func<T, object>> path)
        {
            return new MultiLoaderWithInclude<T>(this).Include<TInclude>(path);
        }

        public TResult Load<TTransformer, TResult>(string id) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            return LoadInternal<TResult>(new[] { id }, transformer).FirstOrDefault();
        }

        public TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new RavenLoadConfiguration();
            configure(configuration);
            return LoadInternal<TResult>(new[] { id }, transformer, configuration.QueryInputs).FirstOrDefault();
        }

        public TResult[] Load<TTransformer, TResult>(params string[] ids) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            return LoadInternal<TResult>(ids, transformer);

        }

        public TResult[] Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new RavenLoadConfiguration();
            configure(configuration);
            return LoadInternal<TResult>(ids.ToArray(), transformer, configuration.QueryInputs);
        }

        /// <summary>
        /// Gets the document URL for the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public string GetDocumentUrl(object entity)
        {
            DocumentMetadata value;
            if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
                throw new InvalidOperationException("Could not figure out identifier for transient instance");

            return DatabaseCommands.UrlFor(value.Key);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query)
        {
            QueryHeaderInformation _;
            return Stream(query, out _);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query, out QueryHeaderInformation queryHeaderInformation)
        {
            var queryProvider = (IRavenQueryProvider)query.Provider;
            var docQuery = queryProvider.ToDocumentQuery<T>(query.Expression);
            return Stream(docQuery, out queryHeaderInformation);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query)
        {
            QueryHeaderInformation _;
            return Stream(query, out _);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query, out QueryHeaderInformation queryHeaderInformation)
        {
            var ravenQueryInspector = ((IRavenQueryInspector)query);
            var indexQuery = ravenQueryInspector.GetIndexQuery(false);

            if (indexQuery.WaitForNonStaleResults || indexQuery.WaitForNonStaleResultsAsOfNow)
                throw new NotSupportedException(
                    "Since Stream() does not wait for indexing (by design), streaming query with WaitForNonStaleResults is not supported.");

            var enumerator = DatabaseCommands.StreamQuery(ravenQueryInspector.IndexQueried, indexQuery, out queryHeaderInformation);
            return YieldQuery(query, enumerator);
        }

        private static IEnumerator<StreamResult<T>> YieldQuery<T>(IDocumentQuery<T> query, IEnumerator<RavenJObject> enumerator)
        {
            using (enumerator)
            {
                var queryOperation = ((DocumentQuery<T>)query).InitializeQueryOperation(null);
                queryOperation.DisableEntitiesTracking = true;
                while (enumerator.MoveNext())
                {
                    var meta = enumerator.Current.Value<RavenJObject>(Constants.Metadata);

                    string key = null;
                    Etag etag = null;
                    if (meta != null)
                    {
                        key = meta.Value<string>("@id") ??
                              meta.Value<string>(Constants.DocumentIdFieldName);

                        var value = meta.Value<string>("@etag");
                        if (value != null)
                            etag = Etag.Parse(value);
                    }

                    yield return new StreamResult<T>
                    {
                        Document = queryOperation.Deserialize<T>(enumerator.Current),
                        Etag = etag,
                        Key = key,
                        Metadata = meta
                    };
                }
            }
        }

        public IEnumerator<StreamResult<T>> Stream<T>(Etag fromEtag, int start = 0, int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null)
        {
            return Stream<T>(fromEtag: fromEtag, startsWith: null, matches: null, start: start, pageSize: pageSize, pagingInformation: pagingInformation);
        }

        public IEnumerator<StreamResult<T>> Stream<T>(string startsWith, string matches = null, int start = 0, int pageSize = Int32.MaxValue, RavenPagingInformation pagingInformation = null)
        {
            return Stream<T>(fromEtag: null, startsWith: startsWith, matches: matches, start: start, pageSize: pageSize, pagingInformation: pagingInformation);
        }

        public FacetResults[] MultiFacetedSearch(params FacetQuery[] facetQueries)
        {
            return DatabaseCommands.GetMultiFacets(facetQueries);
        }

        private IEnumerator<StreamResult<T>> Stream<T>(Etag fromEtag, string startsWith, string matches, int start, int pageSize, RavenPagingInformation pagingInformation)
        {
            using (var enumerator = DatabaseCommands.StreamDocs(fromEtag, startsWith, matches, start, pageSize, null, pagingInformation))
            {
                while (enumerator.MoveNext())
                {
                    var document = SerializationHelper.RavenJObjectToJsonDocument(enumerator.Current);

                    yield return new StreamResult<T>
                    {
                        Document = (T)ConvertToEntity(typeof(T),document.Key, document.DataAsJson, document.Metadata),
                        Etag = document.Etag,
                        Key = document.Key,
                        Metadata = document.Metadata
                    };
                }
            }
        }

        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        public void SaveChanges()
        {
            using (EntityToJson.EntitiesToJsonCachingScope())
            {
                var data = PrepareForSaveChanges();

                if (data.Commands.Count == 0)
                    return; // nothing to do here
                IncrementRequestCount();
                LogBatch(data);

                var batchResults = DatabaseCommands.Batch(data.Commands);
                UpdateBatchResults(batchResults, data);
            }
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        [Obsolete("Use DocumentQuery instead.")]
        public IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            return DocumentQuery<T, TIndexCreator>();
        }

        /// <summary>
        /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
        /// <returns></returns>
        public IDocumentQuery<T> DocumentQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var index = new TIndexCreator();
            return DocumentQuery<T>(index.IndexName, index.IsMapReduce);
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <returns></returns>
        [Obsolete("Use DocumentQuery instead.")]
        public IDocumentQuery<T> LuceneQuery<T>(string indexName, bool isMapReduce = false)
        {
            return DocumentQuery<T>(indexName, isMapReduce);
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <returns></returns>
        public IDocumentQuery<T> DocumentQuery<T>(string indexName, bool isMapReduce = false)
        {
            return new DocumentQuery<T>(this, DatabaseCommands, null, indexName, null, null, theListeners.QueryListeners, isMapReduce);
        }

        /// <summary>
        /// Commits the specified tx id.
        /// </summary>
        /// <param name="txId">The tx id.</param>
        public override void Commit(string txId)
        {
            IncrementRequestCount();
            DatabaseCommands.Commit(txId);
            ClearEnlistment();
        }

        /// <summary>
        /// Rollbacks the specified tx id.
        /// </summary>
        /// <param name="txId">The tx id.</param>
        public override void Rollback(string txId)
        {
            IncrementRequestCount();
            DatabaseCommands.Rollback(txId);
            ClearEnlistment();
        }

        public void PrepareTransaction(string txId)
        {
            IncrementRequestCount();
            DatabaseCommands.PrepareTransaction(txId);
            ClearEnlistment();
        }

        /// <summary>
        /// Query RavenDB dynamically using LINQ
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        public IRavenQueryable<T> Query<T>()
        {
            var indexName = "dynamic";
            if (typeof(T).IsEntityType())
            {
                indexName += "/" + Conventions.GetTypeTagName(typeof(T));
            }
            return Query<T>(indexName);
        }

        /// <summary>
        /// Dynamically query RavenDB using Lucene syntax
        /// </summary>
        [Obsolete("Use DocumentQuery instead.")]
        public IDocumentQuery<T> LuceneQuery<T>()
        {
            return DocumentQuery<T>();
        }

        /// <summary>
        /// Dynamically query RavenDB using Lucene syntax
        /// </summary>
        public IDocumentQuery<T> DocumentQuery<T>()
        {
            string indexName = "dynamic";
            if (typeof(T).IsEntityType())
            {
                indexName += "/" + Conventions.GetTypeTagName(typeof(T));
            }
            return Advanced.DocumentQuery<T>(indexName);
        }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, bool isMapReduce)
        {
            return Advanced.DocumentQuery<T>(indexName, isMapReduce);
        }

        /// <summary>
        /// Create a new query for <typeparam name="T"/>
        /// </summary>
        IAsyncDocumentQuery<T> IDocumentQueryGenerator.AsyncQuery<T>(string indexName, bool isMapReduce)
        {
            throw new NotSupportedException();
        }

        internal Lazy<T> AddLazyOperation<T>(ILazyOperation operation, Action<T> onEval)
        {
            pendingLazyOperations.Add(operation);
            var lazyValue = new Lazy<T>(() =>
            {
                ExecuteAllPendingLazyOperations();
                return (T)operation.Result;
            });

            if (onEval != null)
                onEvaluateLazy[operation] = theResult => onEval((T)theResult);

            return lazyValue;
        }

        internal Lazy<int> AddLazyCountOperation(ILazyOperation operation)
        {
            pendingLazyOperations.Add(operation);
            var lazyValue = new Lazy<int>(() =>
            {
                ExecuteAllPendingLazyOperations();
                return operation.QueryResult.TotalResults;
            });

            return lazyValue;
        }

        /// <summary>
        /// Register to lazily load documents and include
        /// </summary>
        public Lazy<T[]> LazyLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<T[]> onEval)
        {
            var multiLoadOperation = new MultiLoadOperation(this, DatabaseCommands.DisableAllCaching, ids, includes);
            var lazyOp = new LazyMultiLoadOperation<T>(multiLoadOperation, ids, includes);
            return AddLazyOperation(lazyOp, onEval);
        }

        public ResponseTimeInformation ExecuteAllPendingLazyOperations()
        {
            if (pendingLazyOperations.Count == 0)
                return new ResponseTimeInformation();

            try
            {
                var sw = Stopwatch.StartNew();

                IncrementRequestCount();

                var responseTimeDuration = new ResponseTimeInformation();

                while (ExecuteLazyOperationsSingleStep(responseTimeDuration))
                {
                    ThreadSleep.Sleep(100);
                }

                responseTimeDuration.ComputeServerTotal();
                

                foreach (var pendingLazyOperation in pendingLazyOperations)
                {
                    Action<object> value;
                    if (onEvaluateLazy.TryGetValue(pendingLazyOperation, out value))
                        value(pendingLazyOperation.Result);
                }
                responseTimeDuration.TotalClientDuration = sw.Elapsed;
                return responseTimeDuration;
            }
            finally
            {
                pendingLazyOperations.Clear();
            }
        }

        private bool ExecuteLazyOperationsSingleStep(ResponseTimeInformation responseTimeInformation)
        {
            var disposables = pendingLazyOperations.Select(x => x.EnterContext()).Where(x => x != null).ToList();
            try
            {
                if (DatabaseCommands is ServerClient) // server mode
                {
                    var requests = pendingLazyOperations.Select(x => x.CreateRequest()).ToArray();
                    var responses = DatabaseCommands.MultiGet(requests);

                    for (int i = 0; i < pendingLazyOperations.Count; i++)
                    {
                        long totalTime;
                        long.TryParse(responses[i].Headers["Temp-Request-Time"], out totalTime);
                        
                        responseTimeInformation.DurationBreakdown.Add(new ResponseTimeItem
                        {
                            Url = requests[i].UrlAndQuery,
                            Duration = TimeSpan.FromMilliseconds(totalTime)
                        });
                        if (responses[i].RequestHasErrors())
                        {
                            throw new InvalidOperationException("Got an error from server, status code: " + responses[i].Status +
                                                                Environment.NewLine + responses[i].Result);
                        }
                        pendingLazyOperations[i].HandleResponse(responses[i]);
                        if (pendingLazyOperations[i].RequiresRetry)
                        {
                            return true;
                        }
                    }
                    return false;
                }
                else // embedded mode
                {
                    var responses = new List<object>();
                    foreach (var lazyOp in pendingLazyOperations)
                    {
                        var sw = Stopwatch.StartNew();
                        var result = lazyOp.ExecuteEmbedded(DatabaseCommands);
                        responses.Add(result);
                        responseTimeInformation.DurationBreakdown.Add(new ResponseTimeItem
                        {
                            Url = lazyOp.ToString(),
                            Duration = sw.Elapsed
                        });
                    }
                    
                    for (int i = 0; i < pendingLazyOperations.Count; i++)
                    {
                        pendingLazyOperations[i].HandleEmbeddedResponse(responses[i]);
                        if (pendingLazyOperations[i].RequiresRetry)
                        {
                            return true;
                        }
                    }
                    return false;
                }
            }
            finally
            {
                foreach (var disposable in disposables)
                {
                    disposable.Dispose();
                }
            }
        }

        public T[] LoadStartingWith<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null)
        {
            return DatabaseCommands.StartsWith(keyPrefix, matches, start, pageSize, exclude: exclude, pagingInformation: pagingInformation)
                                   .Select(TrackEntity<T>)
                                   .ToArray();
        }

	    public TResult[] LoadStartingWith<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0,
	                                                             int pageSize = 25, string exclude = null,
	                                                             RavenPagingInformation pagingInformation = null,
	                                                             Action<ILoadConfiguration> configure = null)
		    where TTransformer : AbstractTransformerCreationTask, new()
	    {
		    var transformer = new TTransformer().TransformerName;

			var configuration = new RavenLoadConfiguration();
			if (configure != null)
			{
				configure(configuration);
			}

		    return
			    DatabaseCommands.StartsWith(keyPrefix, matches, start, pageSize, exclude: exclude,
			                                pagingInformation: pagingInformation, transformer: transformer, queryInputs: configuration.QueryInputs)
			                    .Select(TrackEntity<TResult>)
			                    .ToArray();
	    }

	    public Lazy<TResult[]> MoreLikeThis<TResult>(MoreLikeThisQuery query)
        {
            var multiLoadOperation = new MultiLoadOperation(this, DatabaseCommands.DisableAllCaching, null, null);
            var lazyOp = new LazyMoreLikeThisOperation<TResult>(multiLoadOperation, query);
            return AddLazyOperation<TResult[]>(lazyOp, null);
        }

        Lazy<T[]> ILazySessionOperations.LoadStartingWith<T>(string keyPrefix, string matches, int start, int pageSize, string exclude, RavenPagingInformation pagingInformation)
        {
            var operation = new LazyStartsWithOperation<T>(keyPrefix, matches, exclude, start, pageSize, this, pagingInformation);

            return AddLazyOperation<T[]>(operation, null);
        }
    }
}

#endif
