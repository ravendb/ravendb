package net.ravendb.client.document;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.closure.Function0;
import net.ravendb.abstractions.data.BatchResult;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.FacetQuery;
import net.ravendb.abstractions.data.FacetResults;
import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;
import net.ravendb.abstractions.data.IndexQuery;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.data.MultiLoadResult;
import net.ravendb.abstractions.data.QueryHeaderInformation;
import net.ravendb.abstractions.data.StreamResult;
import net.ravendb.abstractions.exceptions.ConcurrencyException;
import net.ravendb.abstractions.exceptions.ServerClientException;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.IDocumentQuery;
import net.ravendb.client.IDocumentSessionImpl;
import net.ravendb.client.ILoadConfiguration;
import net.ravendb.client.ISyncAdvancedSessionOperation;
import net.ravendb.client.LoadConfigurationFactory;
import net.ravendb.client.RavenPagingInformation;
import net.ravendb.client.RavenQueryHighlightings;
import net.ravendb.client.RavenQueryStatistics;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.connection.IRavenQueryInspector;
import net.ravendb.client.connection.SerializationHelper;
import net.ravendb.client.document.batches.IEagerSessionOperations;
import net.ravendb.client.document.batches.ILazyOperation;
import net.ravendb.client.document.batches.ILazySessionOperations;
import net.ravendb.client.document.batches.LazyMultiLoadOperation;
import net.ravendb.client.document.sessionoperations.LoadOperation;
import net.ravendb.client.document.sessionoperations.LoadTransformerOperation;
import net.ravendb.client.document.sessionoperations.MultiLoadOperation;
import net.ravendb.client.document.sessionoperations.QueryOperation;
import net.ravendb.client.exceptions.ConflictException;
import net.ravendb.client.indexes.AbstractIndexCreationTask;
import net.ravendb.client.indexes.AbstractTransformerCreationTask;
import net.ravendb.client.linq.IDocumentQueryGenerator;
import net.ravendb.client.linq.IRavenQueryProvider;
import net.ravendb.client.linq.IRavenQueryable;
import net.ravendb.client.linq.RavenQueryInspector;
import net.ravendb.client.linq.RavenQueryProvider;
import net.ravendb.client.util.Types;
import net.ravendb.client.utils.Closer;


import com.google.common.base.Defaults;
import com.mysema.query.types.Path;

/**
 * Implements Unit of Work for accessing the RavenDB server
 *
 */
public class DocumentSession extends InMemoryDocumentSessionOperations implements IDocumentSessionImpl, ISyncAdvancedSessionOperation, IDocumentQueryGenerator {

  protected final List<ILazyOperation> pendingLazyOperations = new ArrayList<>();
  protected final Map<ILazyOperation, Action1<Object>> onEvaluateLazy = new HashMap<>();

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
  @Override
  public ILazySessionOperations lazily() {
    return new LazySessionOperations(this);
  }

  /**
   * Access the eager operations
   */
  @Override
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
  @Override
  public ISyncAdvancedSessionOperation advanced() {
    return this;
  }



  protected class DisableAllCachingCallback implements Function0<AutoCloseable> {
    @Override
    public AutoCloseable apply() {
      return databaseCommands.disableAllCaching();
    }
  }

  /**
   * Loads the specified entity with the specified id.
   */
  @Override
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

    if (includedDocumentsByKey.containsKey(id)) {
      JsonDocument value = includedDocumentsByKey.get(id);
      includedDocumentsByKey.remove(id);
      return (T) trackEntity(clazz, value);
    }

    incrementRequestCount();

    LoadOperation loadOperation = new LoadOperation(this, new DisableAllCachingCallback(), id);
    boolean retry;
    do {
      loadOperation.logOperation();
      try (AutoCloseable close = loadOperation.enterLoadContext()) {
        retry = loadOperation.setResult(databaseCommands.get(id));
      } catch (ConflictException e) {
        throw e;
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


  @Override
  public <T> T load(Class<T> clazz, Number id) {
    String documentKey = getConventions().getFindFullDocumentKeyFromNonStringIdentifier().find(id, clazz, false);
    return load(clazz, documentKey);
  }

  @Override
  public <T> T load(Class<T> clazz, UUID id) {
    String documentKey = getConventions().getFindFullDocumentKeyFromNonStringIdentifier().find(id, clazz, false);
    return load(clazz, documentKey);
  }

  @Override
  public <T> T[] load(Class<T> clazz, Number... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (Number id: ids) {
      documentKeys.add(getConventions().getFindFullDocumentKeyFromNonStringIdentifier().find(id, clazz, false));
    }
    return load(clazz, documentKeys.toArray(new String[0]));
  }

  @Override
  public <T> T[] load(Class<T> clazz, UUID... ids) {
    List<String> documentKeys = new ArrayList<>();
    for (UUID id: ids) {
      documentKeys.add(getConventions().getFindFullDocumentKeyFromNonStringIdentifier().find(id, clazz, false));
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

    MultiLoadResult multiLoadResult = getDatabaseCommands().get(ids, new String[] { }, transformer, queryInputs);
    return new LoadTransformerOperation(this, transformer, ids).complete(clazz, multiLoadResult);
  }



  @SuppressWarnings("null")
  @Override
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

  @Override
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
      result.add(load(clazz, id));
    }
    return result.toArray((T[]) Array.newInstance(clazz, 0));
  }

  /**
   * Queries the specified index.
   * @param clazz
   * @param indexName
   * @return
   */
  @Override
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
  @Override
  public <T> IRavenQueryable<T> query(Class<T> clazz, String indexName, boolean isMapReduce) {
    RavenQueryStatistics ravenQueryStatistics = new RavenQueryStatistics();
    RavenQueryHighlightings highlightings = new RavenQueryHighlightings();
    RavenQueryProvider<T> ravenQueryProvider = new RavenQueryProvider<>(clazz, this, indexName, ravenQueryStatistics, highlightings, getDatabaseCommands(), isMapReduce);
    return new RavenQueryInspector<>(clazz, ravenQueryProvider, ravenQueryStatistics, highlightings, indexName, null, this, getDatabaseCommands(), isMapReduce);
  }

  /**
   * Queries the index specified by tIndexCreator using Linq.
   * @param clazz The result of the query
   * @param tIndexCreator The type of the index creator
   * @return
   */
  @Override
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
  @Override
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

    try {
      for (PropertyDescriptor propertyDescriptor : Introspector.getBeanInfo(entity.getClass()).getPropertyDescriptors()) {
        if (propertyDescriptor.getWriteMethod() == null || propertyDescriptor.getReadMethod() == null) {
          continue;
        }
        Object propValue = propertyDescriptor.getReadMethod().invoke(newEntity, new Object[0]);
        propertyDescriptor.getWriteMethod().invoke(entity, new Object[] { propValue });
      }
    } catch (IntrospectionException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Get the json document by key from the store
   */
  @Override
  protected JsonDocument getJsonDocument(String documentKey) {
    JsonDocument jsonDocument = databaseCommands.get(documentKey);
    if (jsonDocument == null) {
      throw new IllegalStateException("Document '" + documentKey + "' no longer exists and was probably deleted");
    }
    return jsonDocument;
  }

  @Override
  protected String generateKey(Object entity) {
    return getConventions().generateDocumentKey(dbName, databaseCommands, entity);
  }


  /**
   * Begin a load while including the specified path
   */
  @Override
  public ILoaderWithInclude include(String path) {
    return new MultiLoaderWithInclude(this).include(path);
  }

  /**
   * Begin a load while including the specified path
   * @param path
   * @return
   */
  @Override
  public ILoaderWithInclude include(Path<?> path) {
    return new MultiLoaderWithInclude(this).include(path);
  }

  /**
   * Begin a load while including the specified path
   * @param path
   * @return
   */
  @Override
  public ILoaderWithInclude include(Class<?> targetClass, Path<?> path) {
    return new MultiLoaderWithInclude(this).include(targetClass, path);
  }

  @Override
  public <TResult, TTransformer extends AbstractTransformerCreationTask> TResult load(Class<TTransformer> tranformerClass,
      Class<TResult> clazz, String id) {
    try {
      String transformer = tranformerClass.newInstance().getTransformerName();
      TResult[] loadResult = loadInternal(clazz, new String[] { id} , transformer);
      if (loadResult != null && loadResult.length > 0 ) {
        return loadResult[0];
      }
      return null;
    } catch (IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <TResult, TTransformer extends AbstractTransformerCreationTask> TResult load(Class<TTransformer> tranformerClass,
      Class<TResult> clazz, String id, LoadConfigurationFactory configureFactory) {
    try {
      String transformer = tranformerClass.newInstance().getTransformerName();
      RavenLoadConfiguration configuration = new RavenLoadConfiguration();
      configureFactory.configure(configuration);
      TResult[] loadResult = loadInternal(clazz, new String[] { id} , transformer, configuration.getQueryInputs());
      if (loadResult != null && loadResult.length > 0 ) {
        return loadResult[0];
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <TResult, TTransformer extends AbstractTransformerCreationTask> TResult[] load(Class<TTransformer> tranformerClass,
      Class<TResult> clazz, String... ids) {
    try {
      String transformer = tranformerClass.newInstance().getTransformerName();
      return loadInternal(clazz, ids , transformer);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <TResult, TTransformer extends AbstractTransformerCreationTask> TResult[] load(Class<TTransformer> tranformerClass,
      Class<TResult> clazz, List<String> ids, LoadConfigurationFactory configureFactory) {
    try {
      String transformer = tranformerClass.newInstance().getTransformerName();
      RavenLoadConfiguration configuration = new RavenLoadConfiguration();
      configureFactory.configure(configuration);
      return loadInternal(clazz, ids.toArray(new String[0]) , transformer, configuration.getQueryInputs());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the document URL for the specified entity.
   */
  @Override
  public String getDocumentUrl(Object entity) {
    if (!entitiesAndMetadata.containsKey(entity)) {
      throw new IllegalStateException("Could not figure out identifier for transient instance");
    }
    DocumentMetadata value = entitiesAndMetadata.get(entity);
    return databaseCommands.urlFor(value.getKey());
  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(IRavenQueryable<T> query) {
    Reference<QueryHeaderInformation> _ = new Reference<>();
    return stream(query, _);
  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(IRavenQueryable<T> query, Reference<QueryHeaderInformation> queryHeaderInformationRef) {
    IRavenQueryProvider queryProvider = (IRavenQueryProvider)query.getProvider();
    IDocumentQuery<T> docQuery = (IDocumentQuery<T>) queryProvider.toDocumentQuery(query.getElementType(), query.getExpression());
    return stream(docQuery, queryHeaderInformationRef);
  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(IDocumentQuery<T> query) {
    Reference<QueryHeaderInformation> _ = new Reference<>();
    return stream(query, _);
  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(IDocumentQuery<T> query, Reference<QueryHeaderInformation> queryHeaderInformation) {
    IRavenQueryInspector ravenQueryInspector = (IRavenQueryInspector) query;
    IndexQuery indexQuery = ravenQueryInspector.getIndexQuery();

    if (indexQuery.isWaitForNonStaleResults() || indexQuery.isWaitForNonStaleResultsAsOfNow()) {
      throw new IllegalArgumentException(
          "Since stream() does not wait for indexing (by design), streaming query with WaitForNonStaleResults is not supported.");
    }

    Iterator<RavenJObject> iterator = databaseCommands.streamQuery(ravenQueryInspector.getIndexQueried(), indexQuery, queryHeaderInformation);
    return new StreamIterator<>(query, iterator);
  }


  private static class StreamIterator<T> implements Iterator<StreamResult<T>> {

    private Iterator<RavenJObject> innerIterator;
    private DocumentQuery<T> query;
    private QueryOperation queryOperation;

    public StreamIterator(IDocumentQuery<T> query, Iterator<RavenJObject> innerIterator) {
      super();
      this.innerIterator = innerIterator;
      this.query = (DocumentQuery<T>) query;
      queryOperation = ((DocumentQuery<T>)query).initializeQueryOperation(null);
      queryOperation.setDisableEntitiesTracking(true);
    }

    @Override
    public boolean hasNext() {
      return innerIterator.hasNext();
    }

    @Override
    public StreamResult<T> next() {
      RavenJObject nextValue = innerIterator.next();
      RavenJObject meta = nextValue.value(RavenJObject.class, Constants.METADATA);

      String key = null;
      Etag etag = null;
      if (meta != null) {
        key = meta.value(String.class, Constants.DOCUMENT_ID_FIELD_NAME);
        String value = meta.value(String.class, "@etag");
        if (value != null) {
          etag = Etag.parse(value);
        }
      }

      StreamResult<T> streamResult = new StreamResult<>();
      streamResult.setDocument(queryOperation.deserialize(query.getElementType(), nextValue));
      streamResult.setEtag(etag);
      streamResult.setKey(key);
      streamResult.setMetadata(meta);
      return streamResult;
    }

    @Override
    public void remove() {
      throw new IllegalStateException("Not implemented!");
    }

  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass) {
    return stream(entityClass, null, null, null, 0, Integer.MAX_VALUE);
  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag) {
    return stream(entityClass, fromEtag, null, null, 0, Integer.MAX_VALUE);
  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag, String startsWith) {
    return stream(entityClass, fromEtag, startsWith, null, 0, Integer.MAX_VALUE);
  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag, String startsWith, String matches) {
    return stream(entityClass, fromEtag, startsWith, matches, 0, Integer.MAX_VALUE);
  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag, String startsWith, String matches, int start) {
    return stream(entityClass, fromEtag, startsWith, matches, start, Integer.MAX_VALUE);
  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag, String startsWith, String matches, int start, int pageSize) {
    return stream(entityClass, fromEtag, startsWith, matches, start, pageSize, null);
  }

  @Override
  public <T> Iterator<StreamResult<T>> stream(Class<T> entityClass, Etag fromEtag, String startsWith, String matches, int start, int pageSize, RavenPagingInformation pagingInformation) {
    Iterator<RavenJObject> iterator = databaseCommands.streamDocs(fromEtag, startsWith, matches, start, pageSize, null, pagingInformation);
    return new SimpleSteamIterator<>(iterator, entityClass);
  }

  @Override
  public FacetResults[] multiFacetedSearch(FacetQuery...facetQueries) {
    return databaseCommands.getMultiFacets(facetQueries);
  }

  private class SimpleSteamIterator<T> implements Iterator<StreamResult<T>> {
    private Iterator<RavenJObject> innerIterator;
    private Class<T> entityClass;

    public SimpleSteamIterator(Iterator<RavenJObject> innerIterator, Class<T> entityClass) {
      super();
      this.innerIterator = innerIterator;
      this.entityClass = entityClass;
    }

    @Override
    public boolean hasNext() {
      return innerIterator.hasNext();
    }

    @Override
    public StreamResult<T> next() {
      RavenJObject next = innerIterator.next();
      JsonDocument document = SerializationHelper.ravenJObjectToJsonDocument(next);
      StreamResult<T> streamResult = new StreamResult<>();
      streamResult.setDocument((T) convertToEntity(entityClass, document.getKey(), document.getDataAsJson(), document.getMetadata()));
      streamResult.setEtag(document.getEtag());
      streamResult.setKey(document.getKey());
      streamResult.setMetadata(document.getMetadata());
      return streamResult;
    }

    @Override
    public void remove() {
      throw new IllegalStateException("Not implemented!");
    }
  }

  /**
   * Saves all the changes to the Raven server.
   */
  @Override
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
    } catch (ConcurrencyException e) {
      throw e;
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
  @Override
  public <T, TIndexCreator extends AbstractIndexCreationTask> IDocumentQuery<T> documentQuery(Class<T> clazz, Class<TIndexCreator> indexClazz) {
    try {
      TIndexCreator index = indexClazz.newInstance();
      return documentQuery(clazz, index.getIndexName(), index.isMapReduce());
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(indexClazz.getName() + " does not have argumentless constructor.");
    }
  }

  @Override
  public <T> IDocumentQuery<T> documentQuery(Class<T> clazz, String indexName) {
    return documentQuery(clazz, indexName, false);
  }

  /**
   * Query the specified index using Lucene syntax
   * @param clazz
   * @param indexName Name of the index.
   * @param isMapReduce
   * @return
   */
  @Override
  public <T> IDocumentQuery<T> documentQuery(Class<T> clazz, String indexName, boolean isMapReduce) {
    return new DocumentQuery<>(clazz, this, getDatabaseCommands(), indexName, null, null, theListeners.getQueryListeners(), isMapReduce);
  }

  /**
   * Query RavenDB dynamically using
   * @param clazz
   * @return
   */
  @Override
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
  @Override
  public <T> IDocumentQuery<T> documentQuery(Class<T> clazz) {
    String indexName = "dynamic";
    if (Types.isEntityType(clazz)) {
      indexName += "/" + getConventions().getTypeTagName(clazz);
    }
    return advanced().documentQuery(clazz, indexName);
  }

  @SuppressWarnings("unchecked")
  public <T> Lazy<T> addLazyOperation(final ILazyOperation operation, final Action1<T> onEval) {
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

  public Lazy<Integer> addLazyCountOperation(final ILazyOperation operation)
  {
      pendingLazyOperations.add(operation);

      Lazy<Integer> lazyValue = new Lazy<>(new Function0<Integer>() {
        @Override
        public Integer apply() {
          executeAllPendingLazyOperations();
          return operation.getQueryResult().getTotalResults();
        }
      });
      return lazyValue;
  }


  /**
   * Register to lazily load documents and include
   */
  @Override
  public <T> Lazy<T[]> lazyLoadInternal(Class<T> clazz, String[] ids, Tuple<String, Class<?>>[] includes, Action1<T[]> onEval) {
    MultiLoadOperation multiLoadOperation = new MultiLoadOperation(this, new DisableAllCachingCallback(), ids, includes);
    LazyMultiLoadOperation<T> lazyOp = new LazyMultiLoadOperation<>(clazz, multiLoadOperation, ids, includes, null);
    return addLazyOperation(lazyOp, onEval);
  }

  @Override
  public ResponseTimeInformation executeAllPendingLazyOperations() {
    if (pendingLazyOperations.size() == 0)
      return new ResponseTimeInformation();

    try {
      incrementRequestCount();

      ResponseTimeInformation responseTimeDuration = new ResponseTimeInformation();
      long time1 = new Date().getTime();
      try {
        while (executeLazyOperationsSingleStep(responseTimeDuration)) {
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

      long time2 = new Date().getTime();
      responseTimeDuration.setTotalClientDuration(time2 - time1);

      return responseTimeDuration;
    } finally {
      pendingLazyOperations.clear();
    }
  }

  private boolean executeLazyOperationsSingleStep(ResponseTimeInformation responseTimeInformation) {

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

        String tempRequestTime = responses[0].getHeaders().get("Temp-Request-Time");
        Long parsedValue = 0L;
        try {
          parsedValue = Long.parseLong(tempRequestTime);
        } catch (NumberFormatException e)  {
          // ignore
        }
        ResponseTimeInformation.ResponseTimeItem responseTimeItem = new ResponseTimeInformation.ResponseTimeItem();
        responseTimeItem.setUrl(requests.get(i).getUrlAndQuery());
        responseTimeItem.setDuration(parsedValue);
        responseTimeInformation.getDurationBreakdown().add(responseTimeItem);

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

  @Override
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix) {
    return loadStartingWith(clazz, keyPrefix, null, 0, 25);
  }

  @Override
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches) {
    return loadStartingWith(clazz, keyPrefix, matches, 0, 25);
  }

  @Override
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start) {
    return loadStartingWith(clazz, keyPrefix, matches, start, 25);
  }

  @Override
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize) {
    return loadStartingWith(clazz, keyPrefix, matches, start, 25, null, null);
  }

  @Override
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize, String exclude) {
    return loadStartingWith(clazz, keyPrefix, matches, start, pageSize, exclude, null);
  }

  @Override
  public <T> T[] loadStartingWith(Class<T> clazz, String keyPrefix, String matches, int start, int pageSize, String exclude, RavenPagingInformation pagingInformation) {
    List<JsonDocument> results = getDatabaseCommands().startsWith(keyPrefix, matches, start, pageSize, false, exclude, pagingInformation);
    for (JsonDocument doc: results) {
      trackEntity(clazz, doc);
    }
    return results.toArray((T[])Array.newInstance(clazz, 0));
  }

  @Override
  public <TResult, TTransformer extends AbstractTransformerCreationTask> TResult[] loadStartingWith(Class<TResult> clazz, Class<TTransformer> transformerClass,
    String keyPrefix, String matches, int start, int pageSize, String exclude,
    RavenPagingInformation pagingInformation, Action1<ILoadConfiguration> configure) {

    try {
      String transformer = transformerClass.newInstance().getTransformerName();
      RavenLoadConfiguration configuration = new RavenLoadConfiguration();
      if (configure != null) {
        configure.apply(configuration);
      }

      List<JsonDocument> documents = getDatabaseCommands().startsWith(keyPrefix, matches, start, pageSize, false, exclude, pagingInformation, transformer, configuration.getQueryInputs());
      List<TResult> result = new ArrayList<>(documents.size());
      for (JsonDocument document : documents) {
        result.add((TResult)trackEntity(clazz, document));
      }

      return result.toArray((TResult[])Array.newInstance(clazz, 0));
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ServerClientException(e);
    }

  }

}
