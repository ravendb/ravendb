package raven.client.indexes;

import java.util.HashMap;
import java.util.Map;

import raven.abstractions.closure.Action2;
import raven.abstractions.data.Constants;
import raven.abstractions.data.StringDistanceTypes;
import raven.abstractions.indexing.FieldIndexing;
import raven.abstractions.indexing.FieldStorage;
import raven.abstractions.indexing.FieldTermVector;
import raven.abstractions.indexing.IndexDefinition;
import raven.abstractions.indexing.SortOptions;
import raven.abstractions.indexing.SpatialOptions;
import raven.abstractions.indexing.SpatialOptionsFactory;
import raven.abstractions.indexing.SuggestionOptions;
import raven.client.IDocumentStore;
import raven.client.connection.IDatabaseCommands;
import raven.client.connection.ServerClient;
import raven.client.document.DocumentConvention;

import com.mysema.query.types.Path;

/**
 * Base class for creating indexes
 */
public class AbstractIndexCreationTask extends AbstractCommonApiForIndexesAndTransformers {

  protected DocumentConvention conventions;
  protected String map;
  protected String reduce;
  protected String transformResults;

  protected Map<Path<?>, FieldStorage> stores;
  protected Map<String, FieldStorage> storesStrings;
  protected Map<Path<?>, SortOptions> indexSortOptions;
  protected Map<Path<?>, String> analyzers;
  protected Map<String, String> analyzersStrings;
  protected Map<Path<?>, SuggestionOptions> indexSuggestions;
  protected Map<Path<?>, FieldTermVector> termVectors;
  protected Map<String, FieldTermVector> termVectorsStrings;
  protected Map<Path<?>, SpatialOptions> spatialIndexes;
  protected Map<String, SpatialOptions> spatialIndexesStrings;
  protected Map<Path<?>, FieldIndexing> indexes;
  protected Map<String, FieldIndexing> indexesStrings;


  public AbstractIndexCreationTask() {
    this.stores = new HashMap<Path<?>, FieldStorage>();
    this.storesStrings = new HashMap<>();
    this.indexes = new HashMap<>();
    this.indexesStrings = new HashMap<>();
    this.indexSortOptions = new HashMap<>();
    this.indexSuggestions = new HashMap<>();
    this.analyzers = new HashMap<>();
    this.analyzersStrings = new HashMap<>();
    this.termVectors = new HashMap<>();
    this.termVectorsStrings = new HashMap<>();
    this.spatialIndexes = new HashMap<>();
    this.spatialIndexesStrings = new HashMap<>();

  }

  public DocumentConvention getConventions() {
    return conventions;
  }
  public void setConventions(DocumentConvention conventions) {
    this.conventions = conventions;
  }

  public String getIndexName() {
    return getClass().getSimpleName().replace('_', '/');
  }


  /**
   * Executes the index creation against the specified document store.
   * @param store
   */
  public void execute(IDocumentStore store) {
    store.executeIndex(this);
  }

  /**
   * Executes the index creation against the specified document database using the specified conventions
   * @param databaseCommands
   * @param documentConvention
   */
  public void execute(final IDatabaseCommands databaseCommands, final DocumentConvention documentConvention) {
    conventions = documentConvention;
    final IndexDefinition indexDefinition = createIndexDefinition();
    // This code take advantage on the fact that RavenDB will turn an index PUT
    // to a noop of the index already exists and the stored definition matches
    // the new definition.
    databaseCommands.putIndex(getIndexName(), indexDefinition, true);

    updateIndexInReplication(databaseCommands, documentConvention, new Action2<ServerClient, String>() {
      @Override
      public void apply(ServerClient commands, String url) {
        commands.directPutIndex(getIndexName(), url, true, indexDefinition);
      }
    });
  }

  public IndexDefinition createIndexDefinition() {
    if (conventions == null) {
      conventions = new DocumentConvention();
    }

    IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
    builder.setIndexes(indexes);
    builder.setIndexesStrings(indexesStrings);
    builder.setSortOptions(indexSortOptions);
    builder.setAnalyzers(analyzers);
    builder.setAnalyzersStrings(analyzersStrings);
    builder.setMap(map);
    builder.setReduce(reduce);
    builder.setTransformResults(transformResults);
    builder.setStores(stores);
    builder.setStoresStrings(storesStrings);
    builder.setSuggestions(indexSuggestions);
    builder.setTermVectors(termVectors);
    builder.setTermVectorsStrings(termVectorsStrings);
    builder.setSpatialIndexes(spatialIndexes);
    builder.setSpatialIndexesStrings(spatialIndexesStrings);
    return builder.toIndexDefinition(conventions);

  }

  public boolean isMapReduce() {
    return reduce != null;
  }


  /**
   *  Register a field to be indexed
   * @param field
   * @param indexing
   */
  protected void index(Path<?> field, FieldIndexing indexing) {
    indexes.put(field, indexing);
  }

  /**
   * Register a field to be indexed
   * @param field
   * @param indexing
   */
  protected void index(String field, FieldIndexing indexing) {
    indexesStrings.put(field, indexing);
  }

  /**
   * Register a field to be spatially indexed
   *
   * Note: using {@link SpatialOptionsFactory} might be very helpful!
   * @param field
   * @param indexing
   */
  protected void spatial(Path<?> field, SpatialOptions indexing) {
    spatialIndexes.put(field, indexing);
  }

  /**
   * Register a field to be spatially indexed
   *
   * Note: using {@link SpatialOptionsFactory} might be very helpful!
   * @param field
   * @param indexing
   */
  protected void spatial(String field, SpatialOptions indexing) {
    spatialIndexesStrings.put(field, indexing);
  }


  /**
   * Register a field to be stored
   * @param field
   * @param storage
   */
  protected void Store(Path<?> field, FieldStorage storage) {
    stores.put(field, storage);
  }

  protected void storeAllFields(FieldStorage storage) {
    storesStrings.put(Constants.ALL_FIELDS, storage);
  }

  /**
   * Register a field to be stored
   * @param field
   * @param storage
   */
  protected void store(String field, FieldStorage storage) {
    storesStrings.put(field, storage);
  }

  /**
   * Register a field to be analyzed
   * @param field
   * @param analyzer
   */
  protected void analyze(Path<?> field, String analyzer) {
    analyzers.put(field, analyzer);
  }

  /**
   * Register a field to be analyzed
   * @param field
   * @param analyzer
   */
  protected void analyze(String field, String analyzer) {
    analyzersStrings.put(field, analyzer);
  }

  /**
   * Register a field to have term vectors
   * @param field
   * @param termVector
   */
  protected void termVector(Path<?> field, FieldTermVector termVector) {
    termVectors.put(field, termVector);
  }

  /**
   * Register a field to have term vectors
   * @param field
   * @param termVector
   */
  protected void termVector(String field, FieldTermVector termVector) {
    termVectorsStrings.put(field, termVector);
  }

  /**
   * Register a field to be sorted
   * @param field
   * @param sort
   */
  protected void sort(Path<?> field, SortOptions sort) {
    indexSortOptions.put(field, sort);
  }

  /**
   * Register a field to be sorted
   * @param field
   * @param suggestion
   */
  protected void suggestion(Path<?> field, SuggestionOptions suggestion) {
    indexSuggestions.put(field, suggestion);
  }

  /**
   * Register a field to be sorted
   * @param field
   * @param suggestion
   */
  protected void suggestion(Path<?> field) {
    SuggestionOptions options = new SuggestionOptions();
    options.setAccuracy(0.5f);
    options.setDistance(StringDistanceTypes.LEVENSHTEIN);
    suggestion(field, options);
  }

}
