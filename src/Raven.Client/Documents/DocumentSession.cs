//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Connection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Document;
using Raven.Client.Document.Batches;
using Raven.Client.Documents.SessionOperations;
using Raven.Client.Http;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Json.Linq;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSession : InMemoryDocumentSessionOperations, IDocumentQueryGenerator, ISyncAdvancedSessionOperation, IDocumentSessionImpl
    {
        /// <summary>
        /// Gets the database commands.
        /// </summary>
        /// <value>The database commands.</value>
        public IDatabaseCommands DatabaseCommands { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentSession"/> class.
        /// </summary>
        public DocumentSession(string dbName, DocumentStore documentStore, DocumentSessionListeners listeners, Guid id, IDatabaseCommands databaseCommands, RequestExecuter requestExecuter)
            : base(dbName, documentStore, requestExecuter, id)
        {
            DatabaseCommands = databaseCommands;
        }

        #region Lazy

        ILazyLoaderWithInclude<TResult> ILazySessionOperations.Include<TResult>(Expression<Func<TResult, object>> path)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TResult>(IEnumerable<string> ids)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids, Action<TResult[]> onEval)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult> ILazySessionOperations.Load<TResult>(string id)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult> Load<TResult>(string id, Action<TResult> onEval)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult> ILazySessionOperations.Load<TResult>(ValueType id)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult> Load<TResult>(ValueType id, Action<TResult> onEval)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TResult>(params ValueType[] ids)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult[]> ILazySessionOperations.Load<TResult>(IEnumerable<ValueType> ids)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult[]> Load<TResult>(IEnumerable<ValueType> ids, Action<TResult[]> onEval)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult> Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult> Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null)
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult[]> Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public Lazy<TResult[]> Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null, Action<TResult> onEval = null)
        {
            throw new NotImplementedException();
        }

        Lazy<TResult[]> ILazySessionOperations.LoadStartingWith<TResult>(string keyPrefix, string matches, int start, int pageSize,
            string exclude, RavenPagingInformation pagingInformation, string skipAfter)
        {
            throw new NotImplementedException();
        }


        public Lazy<TResult[]> MoreLikeThis<TResult>(MoreLikeThisQuery query)
        {
            throw new NotImplementedException();
        }

        ILazyLoaderWithInclude<object> ILazySessionOperations.Include(string path)
        {
            throw new NotImplementedException();
        }

        public Lazy<T[]> LazyLoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, Action<T[]> onEval)
        {
            throw new NotImplementedException();
        }

        public T[] LoadInternal<T>(string[] ids, string transformer, Dictionary<string, RavenJToken> transformerParameters = null)
        {
            throw new NotImplementedException();
        }

        public T[] LoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes, string transformer, Dictionary<string, RavenJToken> transformerParameters = null)
        {
            throw new NotImplementedException();
        }


        #endregion Lazy

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
        /// Loads the specified entity with the specified id.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id">The id.</param>
        /// <returns></returns>
        public T Load<T>(string id)
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ById(id);

            var command = loadOeration.CreateRequest();
            
            if (command != null )
            {
                RequestExecuter.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocument<T>();
        }

        /// <summary>
        /// Loads the specified entities with the specified ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        public T[] Load<T>(IEnumerable<string> ids)
        {
            return LoadInternal<T>(ids.ToArray());
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

        public TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            return LoadInternal<TResult>(new[] { id }, transformer, configuration.TransformerParameters).FirstOrDefault();
        }

        public TResult[] Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            var transformer = new TTransformer().TransformerName;
            var configuration = new RavenLoadConfiguration();
            if (configure != null)
                configure(configuration);

            return LoadInternal<TResult>(ids.ToArray(), transformer, configuration.TransformerParameters);
        }

        public TResult Load<TResult>(string id, string transformer, Action<ILoadConfiguration> configure)
        {
            throw new NotImplementedException();
        }

        public TResult[] Load<TResult>(IEnumerable<string> ids, string transformer, Action<ILoadConfiguration> configure = null)
        {
            throw new NotImplementedException();
        }

        public TResult Load<TResult>(string id, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            throw new NotImplementedException();
        }

        public TResult[] Load<TResult>(IEnumerable<string> ids, Type transformerType, Action<ILoadConfiguration> configure = null)
        {
            throw new NotImplementedException();
        }

        public T[] LoadInternal<T>(string[] ids)
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
        }

        public T[] LoadInternal<T>(string[] ids, KeyValuePair<string, Type>[] includes)
        {
            var loadOeration = new LoadOperation(this);
            loadOeration.ByIds(ids);
            loadOeration.WithIncludes(includes.Select(x => x.Key).ToArray());

            var command = loadOeration.CreateRequest();
            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                loadOeration.SetResult(command.Result);
            }

            return loadOeration.GetDocuments<T>();
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

        public IEagerSessionOperations Eagerly => this;
        ILazySessionOperations ISyncAdvancedSessionOperation.Lazily
        {
            get { return Lazily; }
        }

        IEagerSessionOperations ISyncAdvancedSessionOperation.Eagerly
        {
            get { return Eagerly; }
        }

        public ILazySessionOperations Lazily => this;

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

        public IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <param name="isMapReduce">Indicates if index is a map-reduce index.</param>
        /// <returns></returns>
        [Obsolete("Use DocumentQuery instead.")]
        public IDocumentQuery<T> LuceneQuery<T>(string indexName, bool isMapReduce = false)
        {
            return DocumentQuery<T>(indexName, isMapReduce);
        }

        public IDocumentQuery<T> LuceneQuery<T>()
        {
            throw new NotImplementedException();
        }

        public FacetedQueryResult[] MultiFacetedSearch(params FacetQuery[] queries)
        {
            throw new NotImplementedException();
        }

        public void Refresh<T>(T entity)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query)
        {
            throw new NotImplementedException();
        }
        
        public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query, out QueryHeaderInformation queryHeaderInformation)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query)
        {
            throw new NotImplementedException();
        }

        IEnumerator<StreamResult<T>> ISyncAdvancedSessionOperation.Stream<T>(IDocumentQuery<T> query, out QueryHeaderInformation queryHeaderInformation)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query, out QueryHeaderInformation queryHeaderInformation)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<StreamResult<T>> Stream<T>(long? fromEtag, int start = 0, int pageSize = Int32.MaxValue,
            RavenPagingInformation pagingInformation = null, string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null)
        {
            throw new NotImplementedException();
        }

        public IEnumerator<StreamResult<T>> Stream<T>(string startsWith, string matches = null, int start = 0, int pageSize = Int32.MaxValue,
            RavenPagingInformation pagingInformation = null, string skipAfter = null, string transformer = null,
            Dictionary<string, RavenJToken> transformerParameters = null)
        {
            throw new NotImplementedException();
        }
        
        Operation ISyncAdvancedSessionOperation.DeleteByIndex<T>(string indexName, Expression<Func<T, bool>> expression)
        {
            return DeleteByIndex(indexName, expression);
        }

        public Operation DeleteByIndex<T, TIndexCreator>(Expression<Func<T, bool>> expression) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            throw new NotImplementedException();
        }

        public Operation DeleteByIndex<T>(string indexName, Expression<Func<T, bool>> expression)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Query the specified index using Lucene syntax
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="indexName">Name of the index.</param>
        /// <param name="isMapReduce">Indicates if index is a map-reduce index.</param>
        /// <returns></returns>
        public IDocumentQuery<T> DocumentQuery<T>(string indexName, bool isMapReduce = false)
        {
            return new DocumentQuery<T>(this, DatabaseCommands, null, indexName, null, null, theListeners.QueryListeners, isMapReduce);
        }

        public IDocumentQuery<T> DocumentQuery<T>()
        {
            throw new NotImplementedException();
        }

        public string GetDocumentUrl(object entity)
        {
            throw new NotImplementedException();
        }

        public T[] LoadStartingWith<T>(string keyPrefix, string matches = null, int start = 0, int pageSize = 25, string exclude = null,
            RavenPagingInformation pagingInformation = null, string skipAfter = null)
        {
            throw new NotImplementedException();
        }

        public TResult[] LoadStartingWith<TTransformer, TResult>(string keyPrefix, string matches = null, int start = 0,
            int pageSize = 25, string exclude = null, RavenPagingInformation pagingInformation = null, Action<ILoadConfiguration> configure = null,
            string skipAfter = null) where TTransformer : AbstractTransformerCreationTask, new()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Query RavenDB dynamically using LINQ
        /// </summary>
        /// <typeparam name="T">The result of the query</typeparam>
        public IRavenQueryable<T> Query<T>()
        {
            var indexName = CreateDynamicIndexName<T>();

            return Query<T>(indexName);
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
            var inspector = new RavenQueryInspector<T>();
            inspector.Init(ravenQueryProvider, ravenQueryStatistics, highlightings, indexName, null, this, DatabaseCommands, null, isMapReduce);
            return inspector;
        }

        public IAsyncDocumentQuery<T> AsyncQuery<T>(string indexName, bool isMapReduce)
        {
            throw new NotImplementedException();
        }

        public RavenQueryInspector<S> CreateRavenQueryInspector<S>()
        {
            return new RavenQueryInspector<S>();
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
        
        IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, bool isMapReduce)
        {
            return Advanced.DocumentQuery<T>(indexName, isMapReduce);
        }

        protected override JsonDocument GetJsonDocument(string documentKey)
        {
            throw new NotImplementedException();
        }

        protected override string GenerateKey(object entity)
        {
            return Conventions.GenerateDocumentKey(databaseName, DatabaseCommands, entity);
        }

        protected override Task<string> GenerateKeyAsync(object entity)
        {
            throw new NotImplementedException();
        }

        public void SaveChanges()
        {
            var saveChangesOeration = new BatchOperation(this);

            var command = saveChangesOeration.CreateRequest();
            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                saveChangesOeration.SetResult(command.Result);
            }
        }
        
        public bool HasChanges { get; }

        Client.ISyncAdvancedSessionOperation IDocumentSession.Advanced
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public void Defer(params ICommandData[] commands)
        {
            throw new NotImplementedException();
        }

        public RavenJObject GetMetadataFor<T>(T instance)
        {
            throw new NotImplementedException();
        }

        public bool HasChanged(object entity)
        {
            throw new NotImplementedException();
        }

        public void MarkReadOnly(object entity)
        {
            throw new NotImplementedException();
        }

        public ResponseTimeInformation ExecuteAllPendingLazyOperations()
        {
            throw new NotImplementedException();
        }

        Linq.IRavenQueryable<T> IDocumentSession.Query<T>(string indexName, bool isMapReduce)
        {
            throw new NotImplementedException();
        }

        Linq.IRavenQueryable<T> IDocumentSession.Query<T>()
        {
            throw new NotImplementedException();
        }

        Linq.IRavenQueryable<T> IDocumentSession.Query<T, TIndexCreator>()
        {
            throw new NotImplementedException();
        }
    }
}