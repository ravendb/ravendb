package raven.client.document;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.codehaus.jackson.map.DeserializationProblemHandler;

import com.mysema.query.types.Expression;

import raven.abstractions.basic.Reference;
import raven.abstractions.basic.Tuple;
import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Function1;
import raven.abstractions.closure.Function2;
import raven.abstractions.closure.Function3;
import raven.abstractions.closure.Function4;
import raven.abstractions.closure.Functions;
import raven.abstractions.data.Constants;
import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.indexing.SortOptions;
import raven.abstractions.json.linq.RavenJObject;
import raven.client.connection.IDatabaseCommands;
import raven.client.connection.ReplicationInformer;
import raven.client.converters.ITypeConverter;
import raven.client.converters.Int32Converter;
import raven.client.converters.Int64Converter;
import raven.client.converters.UUIDConverter;
import raven.client.indexes.AbstractIndexCreationTask;
import raven.client.linq.LinqPathProvider;
import raven.client.util.Inflector;

// TODO: finish me (copy java docs) and introduce interfaces instead of FuncN types
/**
 * Note: we removed logic related to applyReduceFunction because we don't support map/reduce on shards
 * we also don't support contractResolver - Jackson customiazation can be performed via {@link JsonExtensions#getDefaultObjectMapper()} instance
 *
 * for CreateSerializer use {@link JsonExtensions#getDefaultObjectMapper()}
 *
 */
public class DocumentConvention implements Serializable {

  private Map<Class<?>, Field> idPropertyCache = new HashMap<Class<?>, Field>();

  private final List<Tuple<Class<?>, Function3<String, IDatabaseCommands, Object, String>>> listOfRegisteredIdConventions =
      new ArrayList<Tuple<Class<?>, Function3<String, IDatabaseCommands, Object, String>>>();

  private EnumSet<FailoverBehavior> failoverBehavior = EnumSet.of(FailoverBehavior.FAIL_IMMEDIATELY);

  public boolean disableProfiling;

  public List<ITypeConverter> identityTypeConvertors;

  public String identityPartsSeparator;

  public int maxNumberOfRequestsPerSession;

  public boolean allowQueriesOnId;

  public ConsistencyOptions defaultQueryingConsistency;

  private static Map<Class<?>, String> CACHED_DEFAULT_TYPE_TAG_NAMES = new HashMap<Class<?>, String>();
  private AtomicInteger requestCount = new AtomicInteger(0);

  private Function3<String, RavenJObject, RavenJObject, String> findClrType;

  private Function1<Class<?>, String> findClrTypeName;

  private Function3<Object, Class<?>, Boolean, String> findFullDocumentKeyFromNonStringIdentifier;

  private DeserializationProblemHandler jsonContractResolver; //TODO: // find all usages in introduce them

  private TypeTagNameFinder findTypeTagName;

  private Function4<Class<?>, String, String, String, String> findPropertyNameForIndex;

  private Function4<Class<?>, String, String, String, String> findPropertyNameForDynamicIndex;

  private Function1<String, Boolean> shouldCacheRequest;

  private Function1<Field, Boolean> findIdentityProperty;

  private Function1<String, String> findIdentityPropertyNameFromEntityName;

  private DocumentKeyGenerator documentKeyGenerator;

  private boolean useParallelMultiGet;

  private boolean shouldAggressiveCacheTrackChanges;

  private boolean shouldSaveChangesForceAggressiveCacheCheck;

  private Function1<HttpResponse, Action1<HttpRequest>> handleForbiddenResponse;

  private Function1<HttpResponse, Action1<HttpRequest>> handleUnauthorizedResponse;

  private Function2<Object, String, String> findIdValuePartForValueTypeConversion;

  private boolean saveEnumsAsIntegers;

  private Function1<String, String> transformTypeTagNameToDocumentKeyPrefix;

  private boolean enlistInDistributedTransactions;

  private Function1<String, ReplicationInformer> replicationInformerFactory;

  /* The maximum amount of time that we will wait before checking
   * that a failed node is still up or not.
   * Default: 5 minutes */
  private long maxFailoverCheckPeriod = 300000;

  private final Map<String, SortOptions> customDefaultSortOptions = new HashMap<>();

  private final List<Class<?>> customRangeTypes = new ArrayList<>();

  private final List<Tuple<Class<?>, TryConvertValueForQueryDelegate<?>>> listOfQueryValueConverters = new ArrayList<>();


  public DocumentConvention() {

    setIdentityTypeConvertors(Arrays.<ITypeConverter> asList(new UUIDConverter(), new Int32Converter(), new Int64Converter()));
    setMaxFailoverCheckPeriod(5 * 60 * 1000); // 5 minutes
    setDisableProfiling(true);
    setEnlistInDistributedTransactions(true);
    setUseParallelMultiGet(true);
    setDefaultQueryingConsistency(ConsistencyOptions.MONOTONIC_READ);
    setFailoverBehavior(EnumSet.of(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES));
    shouldCacheRequest = Functions.alwaysTrue();
    setFindIdentityProperty(new Function1<Field, Boolean>() {

      @Override
      public Boolean apply(Field input) {
        return input.getName().equals("id");
      }
    });
    setFindClrType(new Function3<String, RavenJObject, RavenJObject, String>() {

      @Override
      public String apply(String id, RavenJObject doc, RavenJObject metadata) {
        return metadata.value(String.class, Constants.RAVEN_CLR_TYPE);
      }
    });
    setFindClrTypeName(new Function1<Class<?>, String>() {
      @Override
      public String apply(Class< ? > entityType) {
        return ReflectionUtil.getFullNameWithoutVersionInformation(entityType);
      }
    });

    setTransformTypeTagNameToDocumentKeyPrefix(new Function1<String, String>() {
      @Override
      public String apply(String typeTagName) {
        return defaultTransformTypeTagNameToDocumentKeyPrefix(typeTagName);
      }
    });
    setFindFullDocumentKeyFromNonStringIdentifier(new Function3<Object, Class<?>, Boolean, String>() {
      @Override
      public String apply(Object id, Class< ? > type, Boolean allowNull) {
        return defaultFindFullDocumentKeyFromNonStringIdentifier(id, type, allowNull);
      }
    });

    setFindIdentityPropertyNameFromEntityName(new Function1<String, String>() {
      @Override
      public String apply(String entityName) {
        return "id";
      }
    });

    setFindTypeTagName(new TypeTagNameFinder() {

      @Override
      public String find(Class< ? > clazz) {
        return defaultTypeTagName(clazz);
      }
    });

    setFindPropertyNameForIndex(new Function4<Class<?>, String, String, String, String>() {
      @Override
      public String apply(Class< ? > indexedType, String indexedName, String path, String prop) {
        return (path + prop).replace(',', '_').replace('.', '_');
      }
    });

    setFindPropertyNameForDynamicIndex(new Function4<Class<?>, String, String, String, String>() {
      @Override
      public String apply(Class< ? > indexedType, String indexedName, String path, String prop) {
        return path + prop;
      }
    });

    setIdentityPartsSeparator("/");
    setJsonContractResolver(new DefaultRavenContractResolver());

    setMaxNumberOfRequestsPerSession(30);
    setReplicationInformerFactory(new Function1<String, ReplicationInformer>() {
      @Override
      public ReplicationInformer apply(String url) {
        return new ReplicationInformer(DocumentConvention.this);
      }
    });
    setFindIdValuePartForValueTypeConversion(new Function2<Object, String, String>() {
      @Override
      public String apply(Object entity, String id) {
        String[] splits = id.split(identityPartsSeparator);
        for (int i = splits.length - 1; i >= 0; i--) {
          if (StringUtils.isNotEmpty(splits[i])) {
            return splits[i];
          }
        }
        return null;
      }
    });
    setShouldAggressiveCacheTrackChanges(true);
    setShouldSaveChangesForceAggressiveCacheCheck(true);
  }

  public static String defaultTransformTypeTagNameToDocumentKeyPrefix(String typeTagName) {
    char[] charArray = typeTagName.toCharArray();
    int count = 0;
    for (int i = 0; i < charArray.length; i++) {
      if (Character.isUpperCase(charArray[i])) {
        count++;
      }
    }

    if (count <= 1) { //simple name, just lower case it
      return typeTagName.toLowerCase();
    }
    // multiple capital letters, so probably something that we want to preserve caps on.
    return typeTagName;
  }

  /**
   * Find the full document name assuming that we are using the standard conventions
   * for generating a document key
   * @param id
   * @param type
   * @param allowNull
   * @return
   */
  public String defaultFindFullDocumentKeyFromNonStringIdentifier(Object id, Class<?> type, boolean allowNull) {
    ITypeConverter converter = null;
    for (ITypeConverter conv: getIdentityTypeConvertors()) {
      if (conv.canConvertFrom(id.getClass())) {
        converter = conv;
        break;
      }
    }
    String tag = getTypeTagName(type);
    if (tag != null) {
      tag = transformTypeTagNameToDocumentKeyPrefix.apply(tag);
      tag += identityPartsSeparator;
    }
    if (converter != null) {
      return converter.convertFrom(tag, id, allowNull);
    }
    return tag + id;
  }

  /**
   * How should we behave in a replicated environment when we can't
   *  reach the primary node and need to failover to secondary node(s).
   * @return the failoverBehavior
   */
  public EnumSet<FailoverBehavior> getFailoverBehavior() {
    return failoverBehavior;
  }

  /**
   * How should we behave in a replicated environment when we can't
   *  reach the primary node and need to failover to secondary node(s).
   * @param failoverBehavior the failoverBehavior to set
   */
  public void setFailoverBehavior(EnumSet<FailoverBehavior> failoverBehavior) {
    this.failoverBehavior = failoverBehavior;
  }

  /**
   * Disable all profiling support
   * @return
   */
  public boolean isDisableProfiling() {
    return disableProfiling;
  }

  /**
   * Disable all profiling support
   * @param b
   */
  public void setDisableProfiling(boolean b) {
    this.disableProfiling = b;
  }

  /**
   * A list of type converters that can be used to translate the document key (string)
   * to whatever type it is that is used on the entity, if the type isn't already a string
   * @return
   */
  public List<ITypeConverter> getIdentityTypeConvertors() {
    return identityTypeConvertors;
  }

  public DeserializationProblemHandler getJsonContractResolver() {
    return jsonContractResolver;
  }

  public void setJsonContractResolver(DeserializationProblemHandler jsonContractResolver) {
    this.jsonContractResolver = jsonContractResolver;
  }

  /**
   * A list of type converters that can be used to translate the document key (string)
   * to whatever type it is that is used on the entity, if the type isn't already a string
   * @param identityTypeConvertors
   */
  public void setIdentityTypeConvertors(List<ITypeConverter> identityTypeConvertors) {
    this.identityTypeConvertors = identityTypeConvertors;
  }

  /**
   * Gets the identity parts separator used by the HiLo generators
   * @return
   */
  public String getIdentityPartsSeparator() {
    return identityPartsSeparator;
  }

  /**
   * Sets the identity parts separator used by the HiLo generators
   * @param identityPartsSeparator
   */
  public void setIdentityPartsSeparator(String identityPartsSeparator) {
    this.identityPartsSeparator = identityPartsSeparator;
  }

  /**
   * Gets the default max number of requests per session.
   * @return
   */
  public int getMaxNumberOfRequestsPerSession() {
    return maxNumberOfRequestsPerSession;
  }

  /**
   * Sets the default max number of requests per session.
   * @param maxNumberOfRequestsPerSession
   */
  public void setMaxNumberOfRequestsPerSession(int maxNumberOfRequestsPerSession) {
    this.maxNumberOfRequestsPerSession = maxNumberOfRequestsPerSession;
  }

  /**
   *  Whatever to allow queries on document id.
   *  By default, queries on id are disabled, because it is far more efficient
   *  to do a Load() than a Query() if you already know the id.
   *  This is NOT recommended and provided for backward compatibility purposes only.
   * @return
   */
  public boolean isAllowQueriesOnId() {
    return allowQueriesOnId;
  }

  /**
   *  Whatever to allow queries on document id.
   *  By default, queries on id are disabled, because it is far more efficient
   *  to do a Load() than a Query() if you already know the id.
   *  This is NOT recommended and provided for backward compatibility purposes only.
   * @param allowQueriesOnId
   */
  public void setAllowQueriesOnId(boolean allowQueriesOnId) {
    this.allowQueriesOnId = allowQueriesOnId;
  }

  /**
   * The consistency options used when querying the database by default
   * @return
   */
  public ConsistencyOptions getDefaultQueryingConsistency() {
    return defaultQueryingConsistency;
  }

  /**
   * The consistency options used when querying the database by default
   * @param defaultQueryingConsistency
   */
  public void setDefaultQueryingConsistency(ConsistencyOptions defaultQueryingConsistency) {
    this.defaultQueryingConsistency = defaultQueryingConsistency;
  }

  /**
   * Generates the document key using identity.
   * @param conventions
   * @param entity
   * @return
   */
  public static String generateDocumentKeyUsingIdentity(DocumentConvention conventions, Object entity) {
    return conventions.findTypeTagName.find(entity.getClass()) + "/";
  }

  /**
   * Get the default tag name for the specified type.
   * @param t
   * @return
   */
  public static String defaultTypeTagName(Class<?> t) {
    String result;

    if (CACHED_DEFAULT_TYPE_TAG_NAMES.containsKey(t)) {
      return CACHED_DEFAULT_TYPE_TAG_NAMES.get(t);
    }

    result = Inflector.pluralize(t.getSimpleName());
    CACHED_DEFAULT_TYPE_TAG_NAMES.put(t, result);

    return result;
  }

  /**
   *  Gets the name of the type tag.
   * @param type
   * @return
   */
  public String getTypeTagName(Class<?> type) {
    String value = findTypeTagName.find(type);
    if (value != null) {
      return value;
    }
    return defaultTypeTagName(type);
  }

  /**
   * Generates the document key.
   * @param dbName
   * @param databaseCommands
   * @param entity
   * @return
   */
  public String generateDocumentKey(String dbName, IDatabaseCommands databaseCommands, Object entity) {
    Class<?> type = entity.getClass();
    for (Tuple<Class<?>, Function3<String, IDatabaseCommands, Object, String>> typeToRegisteredIdConvention : listOfRegisteredIdConventions) {
      if (typeToRegisteredIdConvention.getItem1().isAssignableFrom(type)) {
        return typeToRegisteredIdConvention.getItem2().apply(dbName, databaseCommands, entity);
      }
    }

    return documentKeyGenerator.generate(dbName, databaseCommands, entity);
  }

  /**
   * Gets the identity property.
   * @param type
   * @return
   */
  public Field getIdentityProperty(Class<?> type) {
    if (idPropertyCache.containsKey(type)) {
      return idPropertyCache.get(type);
    }
    // we want to ignore nested entities from index creation tasks
    if (type.isMemberClass() && type.getDeclaringClass() != null && AbstractIndexCreationTask.class.isAssignableFrom(type.getDeclaringClass())) {
      idPropertyCache.put(type, null);
      return null;
    }

    Field identityProperty = null;
    for (Field f : getPropertiesForType(type)) {
      if (findIdentityProperty.apply(f)) {
        identityProperty = f;
        break;
      }
    }

    if (identityProperty != null && !identityProperty.getDeclaringClass().equals(type)) {
      Field propertyInfo = FieldUtils.getField(identityProperty.getDeclaringClass(), identityProperty.getName());
      if (propertyInfo != null) {
        identityProperty = propertyInfo;
      }
    }

    idPropertyCache.put(type, identityProperty);
    return identityProperty;
  }

  private static Iterable<Field> getPropertiesForType(Class<?> type) {
    List<Field> result = new ArrayList<>();
    do {
      Field[] fields = type.getDeclaredFields();
      for (Field field : fields) {
        if (field.isSynthetic() || Modifier.isStatic(field.getModifiers())) {
          continue;
        }
        result.add(field);
      }
      type = type.getSuperclass();
    } while (type != null && !Object.class.equals(type));

    return result;
  }


  /**
   *  Gets the function to find the clr type of a document.
   * @return
   */
  public Function3<String, RavenJObject, RavenJObject, String> getFindClrType() {
    return findClrType;
  }

  /**
   *  Sets the function to find the clr type of a document.
   * @param findClrType
   */
  public void setFindClrType(Function3<String, RavenJObject, RavenJObject, String> findClrType) {
    this.findClrType = findClrType;
  }

  /**
   *  Gets the function to find the clr type name from a clr type
   * @return
   */
  public Function1<Class< ? >, String> getFindClrTypeName() {
    return findClrTypeName;
  }

  /**
   *  Sets the function to find the clr type name from a clr type
   * @param findClrTypeName
   */
  public void setFindClrTypeName(Function1<Class< ? >, String> findClrTypeName) {
    this.findClrTypeName = findClrTypeName;
  }

  /**
   * Gets the function to find the full document key based on the type of a document
   * and the value type identifier (just the numeric part of the id).
   * @return
   */
  public Function3<Object, Class< ? >, Boolean, String> getFindFullDocumentKeyFromNonStringIdentifier() {
    return findFullDocumentKeyFromNonStringIdentifier;
  }

  /**
   * Sets the function to find the full document key based on the type of a document
   * and the value type identifier (just the numeric part of the id).
   * @param findFullDocumentKeyFromNonStringIdentifier
   */
  public void setFindFullDocumentKeyFromNonStringIdentifier(Function3<Object, Class< ? >, Boolean, String> findFullDocumentKeyFromNonStringIdentifier) {
    this.findFullDocumentKeyFromNonStringIdentifier = findFullDocumentKeyFromNonStringIdentifier;
  }

  /**
   * Gets the function to find the type tag.
   * @return
   */
  public TypeTagNameFinder getFindTypeTagName() {
    return findTypeTagName;
  }

  /**
   * Sets the function to find the type tag.
   * @param findTypeTagName
   */
  public void setFindTypeTagName(TypeTagNameFinder findTypeTagName) {
    this.findTypeTagName = findTypeTagName;
  }

  /**
   * Gets the function to find the indexed property name
   * given the indexed document type, the index name, the current path and the property path.
   * @return
   */
  public Function4<Class< ? >, String, String, String, String> getFindPropertyNameForIndex() {
    return findPropertyNameForIndex;
  }

  /**
   * Sets the function to find the indexed property name
   * given the indexed document type, the index name, the current path and the property path.
   * @param findPropertyNameForIndex
   */
  public void setFindPropertyNameForIndex(Function4<Class< ? >, String, String, String, String> findPropertyNameForIndex) {
    this.findPropertyNameForIndex = findPropertyNameForIndex;
  }

  /**
   *  Gets the function to find the indexed property name
   *  given the indexed document type, the index name, the current path and the property path.
   * @return
   */
  public Function4<Class< ? >, String, String, String, String> getFindPropertyNameForDynamicIndex() {
    return findPropertyNameForDynamicIndex;
  }

  /**
   * Sets the function to find the indexed property name
   *  given the indexed document type, the index name, the current path and the property path.
   * @param findPropertyNameForDynamicIndex
   */
  public void setFindPropertyNameForDynamicIndex(Function4<Class< ? >, String, String, String, String> findPropertyNameForDynamicIndex) {
    this.findPropertyNameForDynamicIndex = findPropertyNameForDynamicIndex;
  }

  /**
   * Whatever or not RavenDB should cache the request to the specified url.
   * @return the shouldCacheRequest
   */
  public Function1<String, Boolean> getShouldCacheRequest() {
    return shouldCacheRequest;
  }

  /**
   * Gets the function to find the identity property.
   * @return
   */
  public Function1<Field, Boolean> getFindIdentityProperty() {
    return findIdentityProperty;
  }

  /**
   * Sets the function to find the identity property.
   * @param findIdentityProperty
   */
  public void setFindIdentityProperty(Function1<Field, Boolean> findIdentityProperty) {
    this.findIdentityProperty = findIdentityProperty;
  }

  /**
   * Get the function to get the identity property name from the entity name
   * @return
   */
  public Function1<String, String> getFindIdentityPropertyNameFromEntityName() {
    return findIdentityPropertyNameFromEntityName;
  }

  /**
   * Sets the function to get the identity property name from the entity name
   * @param findIdentityPropertyNameFromEntityName
   */
  public void setFindIdentityPropertyNameFromEntityName(Function1<String, String> findIdentityPropertyNameFromEntityName) {
    this.findIdentityPropertyNameFromEntityName = findIdentityPropertyNameFromEntityName;
  }

  /**
   * Gets the document key generator.
   * @return
   */
  public DocumentKeyGenerator getDocumentKeyGenerator() {
    return documentKeyGenerator;
  }

  /**
   * Sets the document key generator.
   * @param documentKeyGenerator
   */
  public void setDocumentKeyGenerator(DocumentKeyGenerator documentKeyGenerator) {
    this.documentKeyGenerator = documentKeyGenerator;
  }

  /**
   * Whatever or not RavenDB should in the aggressive cache mode use Changes API to track
   * changes and rebuild the cache. This will make that outdated data will be revalidated
   * to make the cache more updated, however it is still possible to get a state result because of the time
   * needed to receive the notification and forcing to check for cached data.
   * @return
   */
  public boolean isShouldAggressiveCacheTrackChanges() {
    return shouldAggressiveCacheTrackChanges;
  }

  /**
   * Whatever or not RavenDB should in the aggressive cache mode use Changes API to track
   * changes and rebuild the cache. This will make that outdated data will be revalidated
   * to make the cache more updated, however it is still possible to get a state result because of the time
   * needed to receive the notification and forcing to check for cached data.
   * @param shouldAggressiveCacheTrackChanges
   */
  public void setShouldAggressiveCacheTrackChanges(boolean shouldAggressiveCacheTrackChanges) {
    this.shouldAggressiveCacheTrackChanges = shouldAggressiveCacheTrackChanges;
  }

  /**
   * Whatever or not RavenDB should in the aggressive cache mode should force the aggressive cache
   * to check with the server after we called SaveChanges() on a non empty data set.
   * This will make any outdated data revalidated, and will work nicely as long as you have just a
   * single client. For multiple clients, {@link DocumentConvention#shouldAggressiveCacheTrackChanges}
   * @return
   */
  public boolean isShouldSaveChangesForceAggressiveCacheCheck() {
    return shouldSaveChangesForceAggressiveCacheCheck;
  }

  /**
   * Whatever or not RavenDB should in the aggressive cache mode should force the aggressive cache
   * to check with the server after we called SaveChanges() on a non empty data set.
   * This will make any outdated data revalidated, and will work nicely as long as you have just a
   * single client. For multiple clients, {@link DocumentConvention#shouldAggressiveCacheTrackChanges}
   * @param shouldSaveChangesForceAggressiveCacheCheck
   */
  public void setShouldSaveChangesForceAggressiveCacheCheck(boolean shouldSaveChangesForceAggressiveCacheCheck) {
    this.shouldSaveChangesForceAggressiveCacheCheck = shouldSaveChangesForceAggressiveCacheCheck;
  }

  /**
   *  Instruct RavenDB to parallel Multi Get processing
   * when handling lazy requests
   * @param useParallelMultiGet
   */
  public void setUseParallelMultiGet(boolean useParallelMultiGet) {
    this.useParallelMultiGet = useParallelMultiGet;
  }

  /**
   * Whatever or not RavenDB should cache the request to the specified url.
   * @param url
   * @return
   */
  public Boolean shouldCacheRequest(String url) {
    return shouldCacheRequest.apply(url);
  }

  /**
   * @param shouldCacheRequest the shouldCacheRequest to set
   */
  public void setShouldCacheRequest(Function1<String, Boolean> shouldCacheRequest) {
    this.shouldCacheRequest = shouldCacheRequest;
  }


  /**
   * Register an id convention for a single type (and all of its derived types.
   * Note that you can still fall back to the DocumentKeyGenerator if you want.
   */
  @SuppressWarnings("unchecked")
  public <TEntity> DocumentConvention registerIdConvention(Class<TEntity> type, Function3<String, IDatabaseCommands, TEntity, String> func) {
    for (Tuple<Class<?>, Function3<String, IDatabaseCommands, Object, String>> entry: listOfRegisteredIdConventions) {
      if (entry.getItem1().equals(type)) {
        listOfRegisteredIdConventions.remove(type);
        break;
      }
    }
    int index;
    for (index = 0; index < listOfRegisteredIdConventions.size(); index++) {
      Tuple<Class< ? >, Function3<String, IDatabaseCommands, Object, String>> entry = listOfRegisteredIdConventions.get(index);
      if (entry.getItem1().isAssignableFrom(type)) {
        break;
      }
    }
    Tuple<Class<?>, Function3<String, IDatabaseCommands, Object, String>> item =
        (Tuple<Class<?>, Function3<String, IDatabaseCommands, Object, String>>) (Object) Tuple.create(type, func);
    listOfRegisteredIdConventions.add(index, item);

    return this;
  }

  /**
   * Get the CLR type (if exists) from the document
   * @param id
   * @param document
   * @param metadata
   * @return
   */
  public String getClrType(String id, RavenJObject document, RavenJObject metadata) {
    return findClrType.apply(id, document, metadata);
  }

  /**
   * When RavenDB needs to convert between a string id to a value type like int or uuid, it calls
   * this to perform the actual work
   * @return
   */
  public Function2<Object, String, String> getFindIdValuePartForValueTypeConversion() {
    return findIdValuePartForValueTypeConversion;
  }

  /**
   * When RavenDB needs to convert between a string id to a value type like int or uuid, it calls
   * this to perform the actual work
   * @param findIdValuePartForValueTypeConversion
   */
  public void setFindIdValuePartForValueTypeConversion(Function2<Object, String, String> findIdValuePartForValueTypeConversion) {
    this.findIdValuePartForValueTypeConversion = findIdValuePartForValueTypeConversion;
  }

  /**
   * Saves Enums as integers and instruct the Linq provider to query enums as integer values.
   * @return
   */
  public boolean isSaveEnumsAsIntegers() {
    return saveEnumsAsIntegers;
  }

  /**
   * Saves Enums as integers and instruct the Linq provider to query enums as integer values.
   * @param saveEnumsAsIntegers
   */
  public void setSaveEnumsAsIntegers(boolean saveEnumsAsIntegers) {
    this.saveEnumsAsIntegers = saveEnumsAsIntegers;
  }

  /**
   * Translate the type tag name to the document key prefix
   * @return
   */
  public Function1<String, String> getTransformTypeTagNameToDocumentKeyPrefix() {
    return transformTypeTagNameToDocumentKeyPrefix;
  }

  /**
   * Translate the type tag name to the document key prefix
   * @param transformTypeTagNameToDocumentKeyPrefix
   */
  public void setTransformTypeTagNameToDocumentKeyPrefix(Function1<String, String> transformTypeTagNameToDocumentKeyPrefix) {
    this.transformTypeTagNameToDocumentKeyPrefix = transformTypeTagNameToDocumentKeyPrefix;
  }

  public void setReplicationInformerFactory(Function1<String, ReplicationInformer> replicationInformerFactory) {
    this.replicationInformerFactory = replicationInformerFactory;
  }

  /**
   * Get the CLR type name to be stored in the entity metadata
   */
  public String getClrTypeName(Class<?> entityType) {
    return findClrTypeName.apply(entityType);
  }

  /**
   * Clone the current conventions to a new instance
   */
  @Override
  public DocumentConvention clone() {
    return (DocumentConvention) SerializationUtils.clone(this);
  }

  /**
   *  Handles unauthenticated responses, usually by authenticating against the oauth server
   * @return the handleUnauthorizedResponse
   */
  public Function1<HttpResponse, Action1<HttpRequest>> getHandleUnauthorizedResponse() {
    return handleUnauthorizedResponse;
  }

  /**
   *  Handles unauthenticated responses, usually by authenticating against the oauth server
   * @param handleUnauthorizedResponse the handleUnauthorizedResponse to set
   */
  public void setHandleUnauthorizedResponse(Function1<HttpResponse, Action1<HttpRequest>> handleUnauthorizedResponse) {
    this.handleUnauthorizedResponse = handleUnauthorizedResponse;
  }

  /**
   * Handles forbidden responses
   * @return the handleForbiddenResponse
   */
  public Function1<HttpResponse, Action1<HttpRequest>> getHandleForbiddenResponse() {
    return handleForbiddenResponse;
  }

  /**
   * Handles forbidden responses
   * @param handleForbiddenResponse the handleForbiddenResponse to set
   */
  public void setHandleForbiddenResponse(Function1<HttpResponse, Action1<HttpRequest>> handleForbiddenResponse) {
    this.handleForbiddenResponse = handleForbiddenResponse;
  }


  public EnumSet<FailoverBehavior> getFailoverBehaviorWithoutFlags() {
    EnumSet<FailoverBehavior> result = this.failoverBehavior.clone();
    result.remove(FailoverBehavior.READ_FROM_ALL_SERVERS);
    return result;
  }

  /**
   * The maximum amount of time that we will wait before checking
   * that a failed node is still up or not.
   * Default: 5 minutes
   * @return
   */
  public long getMaxFailoverCheckPeriod() {
    return maxFailoverCheckPeriod;
  }

  /**
   * The maximum amount of time that we will wait before checking
   * that a failed node is still up or not.
   * Default: 5 minutes
   * @param maxFailoverCheckPeriod
   */
  public void setMaxFailoverCheckPeriod(long maxFailoverCheckPeriod) {
    this.maxFailoverCheckPeriod = maxFailoverCheckPeriod;
  }


  /**
   * Whatever or not RavenDB will automatically enlist in distributed transactions
   * @return
   */
  public boolean isEnlistInDistributedTransactions() {
    return enlistInDistributedTransactions;
  }

  /**
   * Whatever or not RavenDB will automatically enlist in distributed transactions
   * @param enlistInDistributedTransactions
   */
  public void setEnlistInDistributedTransactions(boolean enlistInDistributedTransactions) {
    this.enlistInDistributedTransactions = enlistInDistributedTransactions;
  }

  public int incrementRequestCount() {
    return requestCount.incrementAndGet();
  }

  /**
   * Instruct RavenDB to parallel Multi Get processing
   * when handling lazy requests
   * @return
   */
  public boolean isUseParallelMultiGet() {
    return useParallelMultiGet;
  }

  public void handleForbiddenResponse(HttpResponse forbiddenResponse) {
    handleForbiddenResponse.apply(forbiddenResponse);
  }

  public Action1<HttpRequest> handleUnauthorizedResponse(HttpResponse unauthorizedResponse) {
    return handleUnauthorizedResponse.apply(unauthorizedResponse);
  }

  /**
   * This is called to provide replication behavior for the client. You can customize
   * this to inject your own replication / failover logic.
   * @return
   */
  public Function1<String, ReplicationInformer> getReplicationInformerFactory() {
    return replicationInformerFactory;
  }

  public interface TryConvertValueForQueryDelegate<T> {
    public boolean tryConvertValue(String fieldName, T value, QueryValueConvertionType convertionType, Reference<String> strValue);
  }

  public <T> void registerQueryValueConverter(TryConvertValueForQueryDelegate<T> converter) {
    registerQueryValueConverter(converter, SortOptions.STRING, false);
  }

  public <T> void registerQueryValueConverter(TryConvertValueForQueryDelegate<T> converter, SortOptions defaultSortOption) {
    registerQueryValueConverter(converter, defaultSortOption, false);
  }

  public <T> void registerQueryValueConverter(TryConvertValueForQueryDelegate<T> converter, SortOptions defaultSortOption, boolean usesRangeField) {
    /*TODO
    TryConvertValueForQueryDelegate<object> actual = (string name, object value, QueryValueConvertionType convertionType, out string strValue) =>
    {
      if (value is T)
        return converter(name, (T)value, convertionType, out strValue);
      strValue = null;
      return false;
    };

    int index;
    for (index = 0; index < listOfQueryValueConverters.Count; index++)
    {
      var entry = listOfQueryValueConverters[index];
      if (entry.Item1.IsAssignableFrom(typeof(T)))
      {
        break;
      }
    }

    listOfQueryValueConverters.Insert(index, Tuple.Create(typeof(T), actual));

    if (defaultSortOption != SortOptions.String)
      customDefaultSortOptions.Add(typeof(T).Name, defaultSortOption);

    if (usesRangeField)
      customRangeTypes.Add(typeof(T));
      */
  }

  @SuppressWarnings("unchecked")
  public boolean tryConvertValueForQuery(String fieldName, Object value, QueryValueConvertionType convertionType, Reference<String> strValue) {
    for (Tuple<Class<?>, TryConvertValueForQueryDelegate<?>> queryValueConverterTuple : listOfQueryValueConverters) {
      if (queryValueConverterTuple.getItem1().isInstance(value)) {
        TryConvertValueForQueryDelegate< Object > valueForQueryDelegate = (TryConvertValueForQueryDelegate<Object>) queryValueConverterTuple.getItem2();
        return valueForQueryDelegate.tryConvertValue(fieldName, value, convertionType, strValue);
      }
    }
    strValue.value = null;
    return false;
  }

  public SortOptions getDefaultSortOption(String typeName) {

    switch (typeName) {
    case "java.lang.Short":
      return SortOptions.SHORT;
    case "java.lang.Integer":
      return SortOptions.INT;
    case "java.lang.Long":
      return SortOptions.LONG;
    case "java.lang.Double":
      return SortOptions.DOUBLE;
    case "java.lang.Float":
      return SortOptions.FLOAT;
    case "java.lang.String":
      return SortOptions.STRING;
    default:
      return customDefaultSortOptions.containsKey(typeName)? customDefaultSortOptions.get(typeName) : SortOptions.STRING;
    }
  }



  public boolean usesRangeType(Object o) {
    if (o == null) {
      return false;
    }
    Class<?> type = o.getClass();
    if (o instanceof Class) {
      type = (Class< ? >) o;
    }

    if (Integer.class.equals(type) || Long.class.equals(type) || Double.class.equals(type) || Float.class.equals(type)) {
      return true;
    }
    return customRangeTypes.contains(type);
  }


  private List<CustomQueryExpressionTranslator> customQueryTranslators = new ArrayList<>();

  public void registerCustomQueryTranslator(CustomQueryExpressionTranslator translator) {
    customQueryTranslators.add(translator);
  }

  public LinqPathProvider.Result translateCustomQueryExpression(LinqPathProvider provider, Expression<?> expression) {
    for (CustomQueryExpressionTranslator translator: customQueryTranslators) {
      if (translator.canTransform(expression)) {
        return translator.translate(expression);
      }
    }
    return null;
  }


}
