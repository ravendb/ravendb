package raven.client.document;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Defaults;
import com.mysema.commons.lang.Pair;
import com.mysema.query.types.Path;

import raven.abstractions.closure.Action1;
import raven.abstractions.data.BatchResult;
import raven.abstractions.data.Etag;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.DocumentStoreBase;
import raven.client.IDocumentSessionImpl;
import raven.client.ISyncAdvancedSessionOperation;
import raven.client.ITransactionalDocumentSession;
import raven.client.connection.IDatabaseCommands;
import raven.client.document.batches.IEagerSessionOperations;
import raven.client.document.batches.ILazyOperation;
import raven.client.document.batches.ILazySessionOperations;
import raven.client.document.sessionoperations.LoadOperation;
import raven.client.indexes.AbstractIndexCreationTask;
import raven.client.linq.IDocumentQueryGenerator;
import raven.client.linq.IRavenQueryable;

/**
 * Implements Unit of Work for accessing the RavenDB server
 *
 */
public class DocumentSession extends InMemoryDocumentSessionOperations implements IDocumentSessionImpl, ITransactionalDocumentSession, ISyncAdvancedSessionOperation, IDocumentQueryGenerator {

  private final List<ILazyOperation> pendingLazyOperations = new ArrayList<>();
  private final Map<ILazyOperation, Action1<Object>> onEvaluateLazy = new HashMap<ILazyOperation, Action1<Object>>();

  private IDatabaseCommands databaseCommands;

  /**
   * Gets the database commands.
   * @return
   */
  public IDatabaseCommands getDatabaseCommands() {
    return databaseCommands;
  }

  /**
   * Access the lazy operations
   * @return
   */
  public ILazySessionOperations getLazily() {
    return this;
  }

  /**
   * Access the eager operations
   */
  public IEagerSessionOperations getEagerly() {
    return this;
  }

  /**
   * Initializes a new instance of the {@link DocumentSession} class.
   * @param dbName
   * @param documentStore
   * @param listeners
   * @param id
   * @param databaseCommands
   */
  public DocumentSession(String dbName, DocumentStore documentStore,
      DocumentSessionListeners listeners,
      UUID id,
      IDatabaseCommands databaseCommands) {
    super(dbName, documentStore, listeners, id);
    this.databaseCommands = databaseCommands;
  }

  /**
   * Get the accessor for advanced operations
   *
   * Note: Those operations are rarely needed, and have been moved to a separate
   * property to avoid cluttering the API
   */
  public ISyncAdvancedSessionOperation getAdvanced() {
    return this;
  }

  /**
   * Begin a load while including the specified path
   */
  /* TODO:
  ILazyLoaderWithInclude<T> ILazySessionOperations.Include<T>(Expression<Func<T, object>> path)
  {
    return new LazyMultiLoaderWithInclude<T>(this).Include(path);
  }*/

  /**
   * Loads the specified ids.
   */
  /*TODO
  Lazy<T[]> ILazySessionOperations.Load<T>(params string[] ids)
  {
    return Lazily.Load<T>(ids, null);
  } */

  /**
   * Loads the specified ids.
   */
  /*TODO:
  Lazy<T[]> ILazySessionOperations.Load<T>(IEnumerable<string> ids)
  {
    return Lazily.Load<T>(ids, null);
  }*/

  /**
   * Loads the specified id.
   */
  /*TODO
  Lazy<T> ILazySessionOperations.Load<T>(string id)
  {
    return Lazily.Load(id, (Action<T>) null);
  } */

  /**
   * Loads the specified ids and a function to call when it is evaluated
   */
  /*TODO
  public Lazy<T[]> Load<T>(IEnumerable<string> ids, Action<T[]> onEval)
  {
    return LazyLoadInternal(ids.ToArray(), new KeyValuePair<string, Type>[0], onEval);
  }*/

  /**
   * Loads the specified id and a function to call when it is evaluated
   */
  /*TODO
  public Lazy<T> Load<T>(string id, Action<T> onEval)
  {
    if (IsLoaded(id))
      return new Lazy<T>(() => Load<T>(id));
    var lazyLoadOperation = new LazyLoadOperation<T>(id, new LoadOperation(this, DatabaseCommands.DisableAllCaching, id));
    return AddLazyOperation(lazyLoadOperation, onEval);
  }*/

  /**
   * Loads the specified entities with the specified id after applying
   *  conventions on the provided id to get the real document id.
   *  This method allows you to call:
   *  Load{Post}(1)
   *  And that call will internally be translated to
   *  Load{Post}("posts/1");
   *  Or whatever your conventions specify.
   */
  /*TODO
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
    return Lazily.Load(id, (Action<T>) null);
  }
   */


  /**
   * Loads the specified entity with the specified id.
   */
  public <T> T load(Class<T> clazz, String id) {
    if (id == null) {
      throw new IllegalArgumentException("The document id cannot be null");
    }
    if (isDeleted(id)) {
      return Defaults.defaultValue(clazz);
    }
    Object existingEntity;
    if (entitiesByKey.containsKey(id)) {
      existingEntity = entitiesByKey.get(id);
      return (T) existingEntity;
    }
    incrementRequestCount();

    LoadOperation loadOperation = new LoadOperation(this, databaseCommands.disableAllCaching(), id);
    boolean retry;
    do {
      loadOperation.logOperation();
      try (AutoCloseable close = loadOperation.enterLoadContext()) {
        retry = loadOperation.setResult(databaseCommands.get(id));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } while (retry);
    return loadOperation.complete(clazz);
  }


  /**
   * Loads the specified entities with the specified ids.
   */
  @Override
  public <T> T[] load(Class<T> clazz, String... ids) {
    return loadInternal(clazz, ids);
  }

  /**
   * Loads the specified entities with the specified ids.
   */
  @Override
  public <T> T[] load(Class<T> clazz, Collection<String> ids) {
    return ((IDocumentSessionImpl) this).loadInternal(clazz, ids.toArray(new String[0]));
  }


  public <T> T load(Class<T> clazz, Number id) {
    String documentKey = getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false);
    return load(clazz, documentKey);
  }

  public <T> T load(Class<T> clazz, UUID id) {
    String documentKey = getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false);
    return load(clazz, documentKey);
  }

  public <T> T[] load(Class<T> clazz, Number... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (Number id: ids) {
      documentKeys.add(getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return load(clazz, documentKeys.toArray(new String[0]));
  }

  public <T> T[] load(Class<T> clazz, UUID... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (UUID id: ids) {
      documentKeys.add(getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return load(clazz, documentKeys.toArray(new String[0]));
  }


  private <T> T[] loadInternal(Class<T> clazz, String[] ids, String transformer) {
    return loadInternal(clazz, ids, transformer, null);
  }

  private <T> T[] loadInternal(Class<T> clazz, String[] ids, String transformer, Map<String, RavenJToken> queryInputs) {
    if (ids.length == 0) {
      return (T[]) Array.newInstance(clazz, 0);
    }

    incrementRequestCount();
    if (clazz.isArray()) {

      // Returns array of arrays, public APIs don't surface that yet though as we only support Transform
      // With a single Id
      List<RavenJObject> results = getDatabaseCommands().get(ids, new String[] {}, transformer, queryInputs).getResults();
      //TODO: finish me
      /*
      var arrayOfArrays = DatabaseCommands.Get(ids, new string[] { }, transformer, queryInputs)
          .Results
          .Select(x => x.Value<RavenJArray>("$values").Cast<RavenJObject>())
          .Select(values =>
          {
            var array = values.Select(y => y.Deserialize(typeof(T).GetElementType(), Conventions)).ToArray();
            var newArray = Array.CreateInstance(typeof (T).GetElementType(), array.Length);
            Array.Copy(array, newArray, array.Length);
            return newArray;
          })
          .Cast<T>()
          .ToArray();

      return arrayOfArrays;*/
      return null; //TODO: delete me
    } else {
      /*TODO:

      var items = DatabaseCommands.Get(ids, new string[] { }, transformer, queryInputs)
          .Results
          .SelectMany(x => x.Value<RavenJArray>("$values").ToArray())
          .Select(JsonExtensions.ToJObject)
          .Select(x => x.Deserialize(typeof (T), Conventions))
          .Cast<T>()
          .ToArray();

      if (items.Length > ids.Length)
      {
        throw new InvalidOperationException(String.Format("A load was attempted with transformer {0}, and more than one item was returned per entity - please use {1}[] as the projection type instead of {1}",
            transformer,
            typeof(T).Name));
      }
      return items;*/
      return null; //TODO: delete me
    }
  }



  public <T> T[] loadInternal(Class<T> clazz, String[] ids, Pair<String, Class<?>>[] includes) {
    /*TODO
    if (ids.length == 0) {
      return (T[]) Array.newInstance(clazz, 0);
    }

    List<String> includePaths = null;
    if (includes != null) {
      includePaths = new ArrayList<>();
      for (Pair<String, Class<?>> item: includes) {
        includePaths.add(item.getFirst());
      }
    }

    incrementRequestCount();

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
     */
    return null; //TODO: delete me
  }

  public <T> T[] loadInternal(Class<T> clazz, String[] ids) {
    /*TODO
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

    return ids.Select(id =>
    {
      object val;
      entitiesByKey.TryGetValue(id, out val);
      return (T) val;
    }).ToArray();
     */
    return null; //TODO delete me
  }

  /*TODO
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
   */
  /**
   * Refreshes the specified entity from Raven server.
   */
  /*TODO
  public <T> void refresh(T entity) {
    DocumentMetadata value;
    if (entitiesAndMetadata.TryGetValue(entity, out value) == false)
      throw new InvalidOperationException("Cannot refresh a transient instance");
    IncrementRequestCount();
    var jsonDocument = DatabaseCommands.Get(value.Key);
    if (jsonDocument == null)
      throw new InvalidOperationException("Document '" + value.Key + "' no longer exists and was probably deleted");

    value.Metadata = jsonDocument.Metadata;
    value.OriginalMetadata = (RavenJObject) jsonDocument.Metadata.CloneToken();
    value.ETag = jsonDocument.Etag;
    value.OriginalValue = jsonDocument.DataAsJson;
    var newEntity = ConvertToEntity<T>(value.Key, jsonDocument.DataAsJson, jsonDocument.Metadata);
    foreach (var property in entity.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
    {
      if (!property.CanWrite || !property.CanRead || property.GetIndexParameters().Length != 0)
        continue;
      property.SetValue(entity, property.GetValue(newEntity, null), null);
    }
  }*/


  /**
   * Get the json document by key from the store
   */
  protected JsonDocument getJsonDocument(String documentKey) {
    JsonDocument jsonDocument = databaseCommands.get(documentKey);
    if (jsonDocument == null) {
      throw new IllegalStateException("Document '" + documentKey + "' no longer exists and was probably deleted");
    }
    return jsonDocument;
  }

  protected String generateKey(Object entity) {
    return getConventions().generateDocumentKey(dbName, databaseCommands, entity);
  }


  /**
   * Begin a load while including the specified path
   */
  public ILoaderWithInclude<Object> include(String path) {
    return null; //TODO: delete me
    //TODO return new MultiLoaderWithInclude<object>(this).Include(path);
  }

  /*TODO:
    /// <summary>
    /// Begin a load while including the specified path
    /// </summary>
    /// <param name="path">The path.</param>
    /// <returns></returns>
    public ILoaderWithInclude<T> include<T>(Expression<Func<T, object>> path)
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
      return this.LoadInternal<TResult>(new string[] {id}, transformer).FirstOrDefault();
    }

    public TResult Load<TTransformer, TResult>(string id, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new()
    {
      var transformer = new TTransformer().TransformerName;
      var configuration = new RavenLoadConfiguration();
      configure(configuration);
      return this.LoadInternal<TResult>(new string[] { id }, transformer, configuration.QueryInputs).FirstOrDefault();
    }

    public TResult[] Load<TTransformer, TResult>(params string[] ids) where TTransformer : AbstractTransformerCreationTask, new()
    {
      var transformer = new TTransformer().TransformerName;
      return this.LoadInternal<TResult>(ids, transformer);

    }

    public TResult[] Load<TTransformer, TResult>(IEnumerable<string> ids, Action<ILoadConfiguration> configure) where TTransformer : AbstractTransformerCreationTask, new()
    {
      var transformer = new TTransformer().TransformerName;
      var configuration = new RavenLoadConfiguration();
      configure(configuration);
      return this.LoadInternal<TResult>(ids.ToArray(), transformer, configuration.QueryInputs);
    }*/

  /**
   * Gets the document URL for the specified entity.
   */
  public String getDocumentUrl(Object entity) {
    if (!entitiesAndMetadata.containsKey(entity)) {
      throw new IllegalStateException("Could not figure out identifier for transient instance");
    }
    DocumentMetadata value = entitiesAndMetadata.get(entity);
    return databaseCommands.urlFor(value.getKey());
  }

  /*TODO
  public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query)
  {
    QueryHeaderInformation _;
    return Stream(query, out _);
  }

  public IEnumerator<StreamResult<T>> Stream<T>(IQueryable<T> query, out QueryHeaderInformation queryHeaderInformation)
  {
          var queryProvider = (IRavenQueryProvider)query.Provider;
          var docQuery = queryProvider.ToLuceneQuery<T>(query.Expression);
      return Stream(docQuery, out queryHeaderInformation);
  }

  public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query)
  {
    QueryHeaderInformation _;
    return Stream<T>(query, out _);
  }

  public IEnumerator<StreamResult<T>> Stream<T>(IDocumentQuery<T> query, out QueryHeaderInformation queryHeaderInformation)
  {
    var ravenQueryInspector = ((IRavenQueryInspector)query);
    var indexQuery = ravenQueryInspector.GetIndexQuery(false);
      var enumerator = DatabaseCommands.StreamQuery(ravenQueryInspector.IndexQueried, indexQuery, out queryHeaderInformation);
      return YieldQuery(query, enumerator);
  }

      private static IEnumerator<StreamResult<T>> YieldQuery<T>(IDocumentQuery<T> query, IEnumerator<RavenJObject> enumerator)
    {
        var queryOperation = ((DocumentQuery<T>) query).InitializeQueryOperation(null);
    queryOperation.DisableEntitiesTracking = true;
    while (enumerator.MoveNext())
        {
            var meta = enumerator.Current.Value<RavenJObject>(Constants.Metadata);

            string key = null;
            Etag etag = null;
            if (meta != null)
            {
                key = meta.Value<string>(Constants.DocumentIdFieldName);
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

    public IEnumerator<StreamResult<T>> Stream<T>(Etag fromEtag = null, string startsWith = null, string matches = null, int start = 0, int pageSize = Int32.MaxValue)
  {
    var enumerator = DatabaseCommands.StreamDocs(fromEtag, startsWith, matches, start, pageSize);

    while (enumerator.MoveNext())
    {
      var document = SerializationHelper.RavenJObjectToJsonDocument(enumerator.Current);

      yield return new StreamResult<T>
      {
        Document = (T) ConvertToEntity<T>(document.Key, document.DataAsJson, document.Metadata),
        Etag = document.Etag,
        Key = document.Key,
        Metadata = document.Metadata
      };
    }
  }*/

  /**
   * Saves all the changes to the Raven server.
   */
  public void saveChanges() {
    try (AutoCloseable scope = entityToJson.entitiesToJsonCachingScope()) {
      SaveChangesData data = prepareForSaveChanges();

      if (data.getCommands().size() == 0) {
        return ; // nothing to do here
      }

      incrementRequestCount();
      logBatch(data);

      BatchResult[] batchResults = getDatabaseCommands().batch(data.getCommands());
      updateBatchResults(Arrays.asList(batchResults), data);
    } catch (Exception e) {
      throw new RuntimeException("Unable to save changes", e);
    }
  }

  /*TODO:
    /// <summary>
    /// Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
    /// </summary>
    /// <typeparam name="T">The result of the query</typeparam>
    /// <typeparam name="TIndexCreator">The type of the index creator.</typeparam>
    /// <returns></returns>
    public IDocumentQuery<T> LuceneQuery<T, TIndexCreator>() where TIndexCreator : AbstractIndexCreationTask, new()
    {
      var index = new TIndexCreator();
      return LuceneQuery<T>(index.IndexName, index.IsMapReduce);
    }

    /// <summary>
    /// Query the specified index using Lucene syntax
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="indexName">Name of the index.</param>
    /// <returns></returns>
    public IDocumentQuery<T> LuceneQuery<T>(string indexName, bool isMapReduce = false)
    {
      return new DocumentQuery<T>(this, DatabaseCommands, null, indexName, null, null, listeners.QueryListeners, isMapReduce);
    }*/

  /**
   * Commits the specified tx id.
   * @param txId
   */
  public void commit(String txId) {
    incrementRequestCount();
    getDatabaseCommands().commit(txId);
    clearEnlistment();
  }

  /**
   * Rollbacks the specified tx id.
   * @param txId
   */
  public void rollback(String txId) {
    incrementRequestCount();
    getDatabaseCommands().rollback(txId);
    clearEnlistment();
  }

  public void prepareTransaction(String txId) {
    incrementRequestCount();
    getDatabaseCommands().prepareTransaction(txId);
    clearEnlistment();
  }

  /*TODO
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
    public IDocumentQuery<T> LuceneQuery<T>()
    {
      string indexName = "dynamic";
      if (typeof(T).IsEntityType())
      {
        indexName += "/" + Conventions.GetTypeTagName(typeof(T));
      }
      return Advanced.LuceneQuery<T>(indexName);
    }

    /// <summary>
    /// Create a new query for <typeparam name="T"/>
    /// </summary>
    IDocumentQuery<T> IDocumentQueryGenerator.Query<T>(string indexName, bool isMapReduce)
    {
      return Advanced.LuceneQuery<T>(indexName, isMapReduce);
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
        return (T) operation.Result;
      });

      if (onEval != null)
        onEvaluateLazy[operation] = theResult => onEval((T) theResult);

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
    }*/

  public void executeAllPendingLazyOperations() {
    if (pendingLazyOperations.size() == 0)
      return;

    try {
      incrementRequestCount();
      try {
        while (executeLazyOperationsSingleStep()) {
          Thread.sleep(100);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      for (ILazyOperation pendingLazyOperation : pendingLazyOperations) {
        if (onEvaluateLazy.containsKey(pendingLazyOperation)) {
          onEvaluateLazy.get(pendingLazyOperation).apply(pendingLazyOperation.getResult());
        }
      }
    } finally {
      pendingLazyOperations.clear();
    }
  }

  private boolean executeLazyOperationsSingleStep() {
    /*TODO
      var disposables = pendingLazyOperations.Select(x => x.EnterContext()).Where(x => x != null).ToList();
      try
      {
        if (DatabaseCommands is ServerClient) // server mode
        {
          var requests = pendingLazyOperations.Select(x => x.CreateRequest()).ToArray();
          var responses = DatabaseCommands.MultiGet(requests);
          for (int i = 0; i < pendingLazyOperations.Count; i++)
          {
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
          var responses = pendingLazyOperations.Select(x => x.ExecuteEmbedded(DatabaseCommands)).ToArray();
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
     */
    return false;//TODO: delete me
  }

  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix) {
    return loadStartingWith(clazz, keyPrefix, null, 0, 25);
  }

  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches) {
    return loadStartingWith(clazz, keyPrefix, matches, 0, 25);
  }

  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start) {
    return loadStartingWith(clazz, keyPrefix, matches, start, 25);
  }

  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize) {
    List<JsonDocument> results = getDatabaseCommands().startsWith(keyPrefix, matches, start, pageSize);
    for (JsonDocument doc: results) {
      trackEntity(clazz, doc);
    }
    return results.toArray((T[])Array.newInstance(clazz, 0));
  }






  /*TODO
    Lazy<T[]> ILazySessionOperations.LoadStartingWith<T>(string keyPrefix, string matches, int start, int pageSize)
    {
      var operation = new LazyStartsWithOperation<T>(keyPrefix, matches, start, pageSize, this);

      return AddLazyOperation<T[]>(operation, null);
    }
   */


}
