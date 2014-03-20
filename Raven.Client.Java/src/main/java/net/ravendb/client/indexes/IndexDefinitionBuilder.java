package net.ravendb.client.indexes;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.ravendb.abstractions.extensions.ExpressionExtensions;
import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.FieldStorage;
import net.ravendb.abstractions.indexing.FieldTermVector;
import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.abstractions.indexing.SortOptions;
import net.ravendb.abstractions.indexing.SpatialOptions;
import net.ravendb.abstractions.indexing.SuggestionOptions;
import net.ravendb.client.document.DocumentConvention;


import com.mysema.query.types.Path;



public class IndexDefinitionBuilder {

  private String map;
  private String reduce;
  private String transformResults;
  private Map<Path<?>, FieldStorage> stores;
  private Map<String, FieldStorage> storesStrings;

  private Long maxIndexOutputsPerDocument;
  private boolean disableInMemoryIndexing;

  private Map<Path<?>, FieldIndexing> indexes;
  private Map<String, FieldIndexing> indexesStrings;

  private Map<Path<?>, SortOptions> sortOptions;
  private Map<String, SortOptions> sortOptionsStrings;
  private Map<Path<?>, String> analyzers;
  private Map<String, String> analyzersStrings;
  private Map<Path<?>, SuggestionOptions> suggestions;
  private Map<Path<?>, FieldTermVector> termVectors;
  private Map<String, FieldTermVector> termVectorsStrings;
  private Map<Path<?>, SpatialOptions> spatialIndexes;
  private Map<String, SpatialOptions> spatialIndexesStrings;


  public IndexDefinitionBuilder() {
    this.stores = new HashMap<>();
    this.storesStrings = new HashMap<>();
    this.indexes = new HashMap<>();
    this.indexesStrings = new HashMap<>();
    this.sortOptions = new HashMap<>();
    this.sortOptionsStrings = new HashMap<>();
    this.suggestions = new HashMap<>();
    this.analyzers = new HashMap<>();
    this.analyzersStrings = new HashMap<>();
    this.termVectors = new HashMap<>();
    this.termVectorsStrings = new HashMap<>();
    this.spatialIndexes = new HashMap<>();
    this.spatialIndexesStrings = new HashMap<>();
  }

  private <S> Map<String, S> convertToStringDictionary(Map<Path< ? >, S> input) {
    Map<String, S> result = new HashMap<>();
    for (Entry<Path<?>, S> value: input.entrySet()) {
      String propertyPath = ExpressionExtensions.toPropertyPath(value.getKey(), '_');
      result.put(propertyPath, value.getValue());
    }
    return result;
  }
  public Map<Path< ? >, String> getAnalyzers() {
    return analyzers;
  }
  public Map<String, String> getAnalyzersStrings() {
    return analyzersStrings;
  }
  public Map<Path< ? >, FieldIndexing> getIndexes() {
    return indexes;
  }
  public Map<String, FieldIndexing> getIndexesStrings() {
    return indexesStrings;
  }
  public String getMap() {
    return map;
  }
  public String getReduce() {
    return reduce;
  }

  public boolean isDisableInMemoryIndexing() {
    return disableInMemoryIndexing;
  }


  public void setDisableInMemoryIndexing(boolean disableInMemoryIndexing) {
    this.disableInMemoryIndexing = disableInMemoryIndexing;
  }

  public Map<Path< ? >, SortOptions> getSortOptions() {
    return sortOptions;
  }
  public Map<Path< ? >, SpatialOptions> getSpatialIndexes() {
    return spatialIndexes;
  }
  public Map<String, SpatialOptions> getSpatialIndexesStrings() {
    return spatialIndexesStrings;
  }
  public Map<Path< ? >, FieldStorage> getStores() {
    return stores;
  }
  public Map<String, FieldStorage> getStoresStrings() {
    return storesStrings;
  }
  public Map<Path< ? >, SuggestionOptions> getSuggestions() {
    return suggestions;
  }
  public Map<Path< ? >, FieldTermVector> getTermVectors() {
    return termVectors;
  }
  public Map<String, FieldTermVector> getTermVectorsStrings() {
    return termVectorsStrings;
  }
  public Map<String, SortOptions> getSortOptionsStrings() {
    return sortOptionsStrings;
  }
  public void setSortOptionsStrings(Map<String, SortOptions> sortOptionsStrings) {
    this.sortOptionsStrings = sortOptionsStrings;
  }
  public Long getMaxIndexOutputsPerDocument() {
    return maxIndexOutputsPerDocument;
  }
  public void setMaxIndexOutputsPerDocument(Long maxIndexOutputsPerDocument) {
    this.maxIndexOutputsPerDocument = maxIndexOutputsPerDocument;
  }

  public String getTransformResults() {
    return transformResults;
  }

  public void setAnalyzers(Map<Path< ? >, String> analyzers) {
    this.analyzers = analyzers;
  }
  public void setAnalyzersStrings(Map<String, String> analyzersStrings) {
    this.analyzersStrings = analyzersStrings;
  }
  public void setIndexes(Map<Path< ? >, FieldIndexing> indexes) {
    this.indexes = indexes;
  }
  public void setIndexesStrings(Map<String, FieldIndexing> indexesStrings) {
    this.indexesStrings = indexesStrings;
  }
  public void setMap(String map) {
    this.map = map;
  }
  public void setReduce(String reduce) {
    this.reduce = reduce;
  }
  public void setSortOptions(Map<Path< ? >, SortOptions> sortOptions) {
    this.sortOptions = sortOptions;
  }
  public void setSpatialIndexes(Map<Path< ? >, SpatialOptions> spatialIndexes) {
    this.spatialIndexes = spatialIndexes;
  }
  public void setSpatialIndexesStrings(Map<String, SpatialOptions> spatialIndexesStrings) {
    this.spatialIndexesStrings = spatialIndexesStrings;
  }
  public void setStores(Map<Path< ? >, FieldStorage> stores) {
    this.stores = stores;
  }
  public void setStoresStrings(Map<String, FieldStorage> storesStrings) {
    this.storesStrings = storesStrings;
  }
  public void setSuggestions(Map<Path< ? >, SuggestionOptions> suggestions) {
    this.suggestions = suggestions;
  }

  public void setTermVectors(Map<Path< ? >, FieldTermVector> termVectors) {
    this.termVectors = termVectors;
  }

  public void setTermVectorsStrings(Map<String, FieldTermVector> termVectorsStrings) {
    this.termVectorsStrings = termVectorsStrings;
  }

  public void setTransformResults(String transformResults) {
    this.transformResults = transformResults;
  }

  public IndexDefinition toIndexDefinition(DocumentConvention convention) {
    return toIndexDefinition(convention, true);
  }

  @SuppressWarnings("deprecation")
  public IndexDefinition toIndexDefinition(DocumentConvention convention, boolean validateMap) {
    if (map == null && validateMap)
      throw new IllegalStateException(
        String.format("Map is required to generate an index, you cannot create an index without a valid Map property (in index %s).", getClass().getSimpleName()));

    IndexDefinition indexDefinition = new IndexDefinition();
    if (reduce != null) {
      indexDefinition.setReduce(reduce);
    }
    if (transformResults != null) {
      indexDefinition.setTransformResults(transformResults);
    }
    indexDefinition.setIndexes(convertToStringDictionary(indexes));
    indexDefinition.setStores(convertToStringDictionary(stores));
    indexDefinition.setSortOptions(convertToStringDictionary(sortOptions));
    indexDefinition.setAnalyzers(convertToStringDictionary(analyzers));
    indexDefinition.setSuggestions(convertToStringDictionary(suggestions));
    indexDefinition.setTermVectors(convertToStringDictionary(termVectors));
    indexDefinition.setSpatialIndexes(convertToStringDictionary(spatialIndexes));

    for (Map.Entry<String, FieldIndexing> indexesString: indexesStrings.entrySet()) {
      if (indexDefinition.getIndexes().containsKey(indexesString.getKey())) {
        throw new IllegalArgumentException("There is a duplicate key in indexes: " + indexesString.getKey());
      }
      indexDefinition.getIndexes().put(indexesString.getKey(), indexesString.getValue());
    }

    for (Map.Entry<String, FieldStorage> storeString: storesStrings.entrySet()) {
      if (indexDefinition.getStores().containsKey(storeString.getKey()))
        throw new IllegalArgumentException("There is a duplicate key in stores: " + storeString.getKey());
      indexDefinition.getStores().put(storeString.getKey(), storeString.getValue());
    }

    for (Map.Entry<String, String> analyzerString: analyzersStrings.entrySet()) {
      if (indexDefinition.getAnalyzers().containsKey(analyzerString.getKey()))
        throw new IllegalArgumentException("There is a duplicate key in analyzers: " + analyzerString.getKey());
      indexDefinition.getAnalyzers().put(analyzerString.getKey(), analyzerString.getValue());
    }

    for (Map.Entry<String, FieldTermVector> termVectorString: termVectorsStrings.entrySet()) {
      if (indexDefinition.getTermVectors().containsKey(termVectorString.getKey()))
        throw new IllegalArgumentException("There is a duplicate key in term vectors: " + termVectorString.getKey());
      indexDefinition.getTermVectors().put(termVectorString.getKey(), termVectorString.getValue());
    }

    for (Map.Entry<String, SpatialOptions> spatialString: spatialIndexesStrings.entrySet()) {
      if (indexDefinition.getSpatialIndexes().containsKey(spatialString.getKey()))
        throw new IllegalArgumentException("There is a duplicate key in spatial indexes: " + spatialString.getKey());
      indexDefinition.getSpatialIndexes().put(spatialString.getKey(), spatialString.getValue());
    }

    for (Map.Entry<String, SortOptions> sortOptions: sortOptionsStrings.entrySet()) {
      if (indexDefinition.getSortOptions().containsKey(sortOptions.getKey()))
        throw new IllegalArgumentException("There is a duplicate key in sort options: " + sortOptions.getKey());
      indexDefinition.getSortOptions().put(sortOptions.getKey(), sortOptions.getValue());
    }

    indexDefinition.setDisableInMemoryIndexing(disableInMemoryIndexing);
    indexDefinition.setMaxIndexOutputsPerDocument(maxIndexOutputsPerDocument);

    if (map != null) {
      indexDefinition.setMap(map);
    }

    return indexDefinition;
  }



}
