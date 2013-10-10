package raven.client.indexes;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import raven.abstractions.indexing.IndexDefinition;
import raven.client.document.DocumentConvention;

/**
 * Allow to create indexes with multiple maps
 */
public class AbstractMultiMapIndexCreationTask extends AbstractIndexCreationTask {
  private final Set<String> maps = new HashSet<>();


  protected void addMap(String expression) {
    maps.add(expression);
  }

  protected void addMapForAll(Class<?> baseClass, String expression) {
    throw new NotImplementedException();    //TODO: implement me!
  }

  @Override
  public IndexDefinition createIndexDefinition() {
    if (map != null) {
      throw new IllegalStateException("Please use addMap or addMapForAll to insert map expression. If you want index with single map use " +
          AbstractIndexCreationTask.class.getSimpleName());
    }

    if (conventions == null) {
      conventions = new DocumentConvention();
    }

    IndexDefinitionBuilder builder = new IndexDefinitionBuilder();
    builder.setIndexes(indexes);
    builder.setIndexesStrings(indexesStrings);
    builder.setSortOptions(indexSortOptions);
    builder.setSortOptionsStrings(indexSortOptionsStrings);
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
    IndexDefinition indexDefinition = builder.toIndexDefinition(conventions, false);
    indexDefinition.setMaps(maps);

    return indexDefinition;

  }


}
