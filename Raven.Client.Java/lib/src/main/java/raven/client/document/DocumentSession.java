package raven.client.document;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import raven.abstractions.basic.Lazy;
import raven.abstractions.basic.Tuple;
import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Function0;
import raven.abstractions.data.BatchResult;
import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.MultiLoadResult;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.IDocumentQuery;
import raven.client.IDocumentSessionImpl;
import raven.client.ISyncAdvancedSessionOperation;
import raven.client.ITransactionalDocumentSession;
import raven.client.RavenQueryHighlightings;
import raven.client.RavenQueryStatistics;
import raven.client.connection.IDatabaseCommands;
import raven.client.document.batches.IEagerSessionOperations;
import raven.client.document.batches.ILazyLoaderWithInclude;
import raven.client.document.batches.ILazyOperation;
import raven.client.document.batches.ILazySessionOperations;
import raven.client.document.batches.LazyLoadOperation;
import raven.client.document.batches.LazyMultiLoadOperation;
import raven.client.document.batches.LazyMultiLoaderWithInclude;
import raven.client.document.batches.LazyStartsWithOperation;
import raven.client.document.sessionoperations.LoadOperation;
import raven.client.document.sessionoperations.MultiLoadOperation;
import raven.client.indexes.AbstractIndexCreationTask;
import raven.client.linq.IDocumentQueryGenerator;
import raven.client.linq.IRavenQueryable;
import raven.client.linq.RavenQueryInspector;
import raven.client.linq.RavenQueryProvider;
import raven.client.util.Types;
import raven.client.utils.Closer;

import com.google.common.base.Defaults;
import com.mysema.query.types.Path;

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
  public ILazySessionOperations lazily() {
    return this;
  }

  /**
   * Access the eager operations
   */
  public IEagerSessionOperations eagerly() {
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
  public ISyncAdvancedSessionOperation advanced() {
    return this;
  }

  /**
   * Begin a load while including the specified path
   */
  public ILazyLoaderWithInclude lazyInclude(Path<?> path) {
    return new LazyMultiLoaderWithInclude(this).lazyInclude(path);
  }

  /**
   * Loads the specified ids.
   */
  public <T> Lazy<T[]> lazyLoad(Class<T> clazz, String... ids) {
    return lazyLoad(clazz, Arrays.asList(ids), null);
  }

  /**
   * Loads the specified ids.
   */
  public <T> Lazy<T[]> lazyLoad(Class<T> clazz, Collection<String> ids) {
    return lazyLoad(clazz, ids, null);
  }

  /**
   * Loads the specified id.
   */
  public <T> Lazy<T> lazyLoad(Class<T> clazz, String id) {
    return lazyLoad(clazz, id, null);
  }


  /**
   * Loads the specified ids and a function to call when it is evaluated
   */
  @SuppressWarnings("unchecked")
  public <TResult> Lazy<TResult[]> lazyLoad(Class<TResult> clazz, Collection<String> ids, Action1<TResult[]> onEval) {
    return lazyLoadInternal(clazz, ids.toArray(new String[0]), new Tuple[0], onEval);
  }

  /**
   * Loads the specified id and a function to call when it is evaluated
   */
  public <T> Lazy<T> lazyLoad(final Class<T> clazz, final String id, Action1<T> onEval) {
    if (isLoaded(id)) {
      return new Lazy<T>(new Function0<T>() {
        @Override
        public T apply() {
          return load(clazz, id);
        }
      });
    }
    LazyLoadOperation<T> lazyLoadOperation = new LazyLoadOperation<T>(clazz, id, new LoadOperation(this, new DisableAllCachingCallback(), id));
    return addLazyOperation(lazyLoadOperation, onEval);
  }

  public <T> Lazy<T> lazyLoad(Class<T> clazz, Number id, Action1<T> onEval) {
    String documentKey = getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false);
    return lazyLoad(clazz, documentKey, onEval);
  }

  public <T> Lazy<T> lazyLoad(Class<T> clazz, UUID id, Action1<T> onEval) {
    String documentKey = getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false);
    return lazyLoad(clazz, documentKey, onEval);
  }

  public <T> Lazy<T[]> lazyLoad(Class<T> clazz, Number... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (Number id : ids) {
      documentKeys.add(getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return lazyLoad(clazz, documentKeys, null);
  }

  public <T> Lazy<T[]> lazyLoad(Class<T> clazz, UUID... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (UUID id : ids) {
      documentKeys.add(getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return lazyLoad(clazz, documentKeys, null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <TResult> Lazy<TResult[]> lazyLoad(Class<TResult> clazz, Action1<TResult[]> onEval, Number... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (Number id : ids) {
      documentKeys.add(getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return lazyLoadInternal(clazz, documentKeys.toArray(new String[0]), new Tuple[0], onEval);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <TResult> Lazy<TResult[]> lazyLoad(Class<TResult> clazz, Action1<TResult[]> onEval, UUID... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (UUID id : ids) {
      documentKeys.add(getConventions().getFindFullDocumentKeyFromNonStringIdentifier().apply(id, clazz, false));
    }
    return lazyLoadInternal(clazz, documentKeys.toArray(new String[0]), new Tuple[0], onEval);
  }

  /**
   * Begin a load while including the specified path
   */
  public ILazyLoaderWithInclude lazyInclude(String path) {
    return new LazyMultiLoaderWithInclude(this).lazyInclude(path);
  }

  public <T> Lazy<T> lazyLoad(Class<T> clazz, Number id) {
    return lazyLoad(clazz, id, (Action1<T>) null);
  }

  public <T> Lazy<T> lazyLoad(Class<T> clazz, UUID id) {
    return lazyLoad(clazz, id, (Action1<T>) null);
  }

  private class DisableAllCachingCallback implements Function0<AutoCloseable> {
    @Override
    public AutoCloseable apply() {
      return databaseCommands.disableAllCaching();
    }
  }

  /**
   * Loads the specified entity with the specified id.
   */
  @SuppressWarnings("unchecked")
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

    LoadOperation loadOperation = new LoadOperation(this, new DisableAllCachingCallback(), id);
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

  @SuppressWarnings("unchecked")
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



  @SuppressWarnings("unchecked")
  public <T> T[] loadInternal(Class<T> clazz, String[] ids, Tuple<String, Class<?>>[] includes) {
    if (ids.length == 0) {
      return (T[]) Array.newInstance(clazz, 0);
    }

    List<String> includePaths = null;
    if (includes != null) {
      includePaths = new ArrayList<>();
      for (Tuple<String, Class<?>> item: includes) {
        includePaths.add(item.getItem1());
      }
    }

    incrementRequestCount();

    MultiLoadOperation multiLoadOperation = new MultiLoadOperation(this, new DisableAllCachingCallback(), ids, includes);
    MultiLoadResult multiLoadResult = null;
    do {
      multiLoadOperation.logOperation();
      try (AutoCloseable context = multiLoadOperation.enterMultiLoadContext()) {
        multiLoadResult = databaseCommands.get(ids, includePaths.toArray(new String[0]));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } while (multiLoadOperation.setResult(multiLoadResult));

    return multiLoadOperation.complete(clazz);
  }

  @SuppressWarnings("unchecked")
  public <T> T[] loadInternal(Class<T> clazz, String[] ids) {
    if (ids.length == 0) {
      return (T[]) Array.newInstance(clazz, 0);
    }

    // only load documents that aren't already cached
    Set<String> idsOfNotExistingObjects = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    for (String id: ids) {
      if (!isLoaded(id) && !isDeleted(id)) {
        idsOfNotExistingObjects.add(id);
      }
    }

    if (idsOfNotExistingObjects.size() > 0) {
      incrementRequestCount();
      MultiLoadOperation multiLoadOperation = new MultiLoadOperation(this, new DisableAllCachingCallback(), idsOfNotExistingObjects.toArray(new String[0]), null);
      MultiLoadResult multiLoadResult = null;
      do {
        multiLoadOperation.logOperation();
        try (AutoCloseable context = multiLoadOperation.enterMultiLoadContext()) {
          multiLoadResult = databaseCommands.get(idsOfNotExistingObjects.toArray(new String[0]), null);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } while (multiLoadOperation.setResult(multiLoadResult));

      multiLoadOperation.complete(clazz);
    }

    List<Object> result = new ArrayList<>();
    for (String id: ids) {
      result.add(entitiesByKey.get(id));
    }
    return (T[]) result.toArray();
  }

  /**
   * Queries the specified index.
   * @param clazz
   * @param indexName
   * @return
   */
  public <T> IRavenQueryable<T> query(Class<T> clazz, String indexName) {
    return query(clazz, indexName, false);
  }

  /**
   * Queries the specified index.
   * @param clazz The result of the query
   * @param indexName Name of the index.
   * @param isMapReduce Whatever we are querying a map/reduce index (modify how we treat identifier properties)
   * @return
   */
  public <T> IRavenQueryable<T> query(Class<T> clazz, String indexName, boolean isMapReduce) {
    RavenQueryStatistics ravenQueryStatistics = new RavenQueryStatistics();
    RavenQueryHighlightings highlightings = new RavenQueryHighlightings();
    RavenQueryProvider<T> ravenQueryProvider = new RavenQueryProvider<T>(clazz, this, indexName, ravenQueryStatistics, highlightings, getDatabaseCommands(), isMapReduce);
    return new RavenQueryInspector<T>(clazz, ravenQueryProvider, ravenQueryStatistics, highlightings, indexName, null, this, getDatabaseCommands(), isMapReduce);
  }

  /**
   * Queries the index specified by tIndexCreator using Linq.
   * @param clazz The result of the query
   * @param tIndexCreator The type of the index creator
   * @return
   */
  public <T> IRavenQueryable<T> query(Class<T> clazz, Class<? extends AbstractIndexCreationTask> tIndexCreator) {
    try {
      AbstractIndexCreationTask indexCreator = tIndexCreator.newInstance();
      return query(clazz, indexCreator.getIndexName(), indexCreator.isMapReduce());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(tIndexCreator.getName() + " does not have argumentless constructor.");
    }
  }


  /**
   * Refreshes the specified entity from Raven server.
   */
  public <T> void refresh(T entity) {
    DocumentMetadata value;
    if (!entitiesAndMetadata.containsKey(entity)) {
      throw new IllegalStateException("Cannot refresh a transient instance");
    }
    value = entitiesAndMetadata.get(entity);
    incrementRequestCount();
    JsonDocument jsonDocument = databaseCommands.get(value.getKey());
    if (jsonDocument == null) {
      throw new IllegalStateException("Document '" + value.getKey() + "' no longer exists and was probably deleted");
    }
    value.setMetadata(jsonDocument.getMetadata());
    value.setOriginalMetadata(jsonDocument.getMetadata().cloneToken());
    value.setEtag(jsonDocument.getEtag());
    value.setOriginalValue(jsonDocument.getDataAsJson());
    Object newEntity = convertToEntity(entity.getClass(), value.getKey(), jsonDocument.getDataAsJson(), jsonDocument.getMetadata());
    /*TODO
    foreach (var property in entity.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
    {
      if (!property.CanWrite || !property.CanRead || property.GetIndexParameters().Length != 0)
        continue;
      property.SetValue(entity, property.GetValue(newEntity, null), null);
    }*/
  }


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
  public ILoaderWithInclude include(String path) {
    return new MultiLoaderWithInclude(this).include(path);
  }

  /**
   * Begin a load while including the specified path
   * @param path
   * @return
   */
  public ILoaderWithInclude include(Path<?> path) {
    return new MultiLoaderWithInclude(this).include(path);
  }
  /*TODO:


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

  /**
   * Queries the index specified by <typeparamref name="TIndexCreator"/> using lucene syntax.
   * @param clazz The result of the query
   * @param indexClazz The type of the index creator.
   * @return
   */
  public <T, TIndexCreator extends AbstractIndexCreationTask> IDocumentQuery<T> luceneQuery(Class<T> clazz, Class<TIndexCreator> indexClazz) {
    try {
      TIndexCreator index = indexClazz.newInstance();
      return luceneQuery(clazz, index.getIndexName(), index.isMapReduce());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(indexClazz.getName() + " does not have argumentless constructor.");
    }
  }

  public <T> IDocumentQuery<T> luceneQuery(Class<T> clazz, String indexName) {
    return luceneQuery(clazz, indexName, false);
  }

  /**
   * Query the specified index using Lucene syntax
   * @param clazz
   * @param indexName Name of the index.
   * @param isMapReduce
   * @return
   */
  public <T> IDocumentQuery<T> luceneQuery(Class<T> clazz, String indexName, boolean isMapReduce) {
    return new DocumentQuery<T>(clazz, this, getDatabaseCommands(), indexName, null, null, listeners.getQueryListeners(), isMapReduce);
  }

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

  /**
   * Query RavenDB dynamically using
   * @param clazz
   * @return
   */
  public <T> IRavenQueryable<T> query(Class<T> clazz) {
    String indexName = "dynamic";
    if (Types.isEntityType(clazz)) {
      indexName += "/" + getConventions().getTypeTagName(clazz);
    }
    return query(clazz, indexName);
  }


  /**
   * Dynamically query RavenDB using Lucene syntax
   */
  public <T> IDocumentQuery<T> luceneQuery(Class<T> clazz) {
    String indexName = "dynamic";
    if (Types.isEntityType(clazz)) {
      indexName += "/" + getConventions().getTypeTagName(clazz);
    }
    return advanced().luceneQuery(clazz, indexName);
  }

  @SuppressWarnings("unchecked")
  protected <T> Lazy<T> addLazyOperation(final ILazyOperation operation, final Action1<T> onEval) {
    pendingLazyOperations.add(operation);
    Lazy<T> lazyValue = new Lazy<>(new Function0<T>() {
      @Override
      public T apply() {
        executeAllPendingLazyOperations();
        return (T) operation.getResult();
      }
    });

    if (onEval != null) {
      onEvaluateLazy.put(operation, new Action1<Object>() {
        @Override
        public void apply(Object theResult) {
          onEval.apply((T)theResult);
        }
      });
    }
    return lazyValue;
  }


  /**
   * Register to lazily load documents and include
   */
  public <T> Lazy<T[]> lazyLoadInternal(Class<T> clazz, String[] ids, Tuple<String, Class<?>>[] includes, Action1<T[]> onEval) {
    MultiLoadOperation multiLoadOperation = new MultiLoadOperation(this, new DisableAllCachingCallback(), ids, includes);
    LazyMultiLoadOperation<T> lazyOp = new LazyMultiLoadOperation<T>(clazz, multiLoadOperation, ids, includes);
    return addLazyOperation(lazyOp, onEval);
  }

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
    List<AutoCloseable> disposables = new ArrayList<>();
    for (ILazyOperation lazyOp: pendingLazyOperations) {
      AutoCloseable context = lazyOp.enterContext();
      if (context != null) {
        disposables.add(context);
      }
    }

    try {
      List<GetRequest> requests = new ArrayList<>();
      for (ILazyOperation lazyOp: pendingLazyOperations) {
        requests.add(lazyOp.createRequest());
      }
      GetResponse[] responses = databaseCommands.multiGet(requests.toArray(new GetRequest[0]));
      for (int i = 0; i < pendingLazyOperations.size(); i++) {
        if (responses[i].isRequestHasErrors()) {
          throw new IllegalStateException("Got an error from server, status code: " + responses[i].getStatus()  + "\n" + responses[i].getResult());
        }
        pendingLazyOperations.get(i).handleResponse(responses[i]);
        if (pendingLazyOperations.get(i).isRequiresRetry()) {
          return true;
        }
      }
      return false;
    } finally {
      for (AutoCloseable closable: disposables) {
        Closer.close(closable);
      }
    }
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

  @SuppressWarnings("unchecked")
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize) {
    List<JsonDocument> results = getDatabaseCommands().startsWith(keyPrefix, matches, start, pageSize);
    for (JsonDocument doc: results) {
      trackEntity(clazz, doc);
    }
    return results.toArray((T[])Array.newInstance(clazz, 0));
  }

  public <T> Lazy<T[]> lazyLoadStartingWith(Class<T> clazz, String keyPrefix) {
    return lazyLoadStartingWith(clazz, keyPrefix, null, 0, 25);
  }

  public <T> Lazy<T[]> lazyLoadStartingWith(Class<T> clazz, String keyPrefix, String matches) {
    return lazyLoadStartingWith(clazz, keyPrefix, matches, 0, 25);
  }

  public <T> Lazy<T[]> lazyLoadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start) {
    return lazyLoadStartingWith(clazz, keyPrefix, matches, start, 25);
  }

  public <T> Lazy<T[]> lazyLoadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize) {
    LazyStartsWithOperation<T> operation = new LazyStartsWithOperation<T>(clazz, keyPrefix, matches, start, pageSize, this);
    return addLazyOperation(operation, null);
  }



}
