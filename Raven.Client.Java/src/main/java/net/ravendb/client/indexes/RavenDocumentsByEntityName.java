package net.ravendb.client.indexes;

import java.util.HashMap;
import java.util.Map;

import net.ravendb.abstractions.indexing.FieldIndexing;
import net.ravendb.abstractions.indexing.FieldStorage;
import net.ravendb.abstractions.indexing.FieldTermVector;
import net.ravendb.abstractions.indexing.IndexDefinition;


/**
 * Create an index that allows to tag entities by their entity name
 */
public class RavenDocumentsByEntityName extends AbstractIndexCreationTask {

  @Override
  public boolean isMapReduce() {
    return false;
  }

  @Override
  public String getIndexName() {
    return "Raven/DocumentsByEntityName";
  }

  @Override
  public IndexDefinition createIndexDefinition() {
    IndexDefinition def = new IndexDefinition();
    def.setMap("from doc in docs let Tag = doc[\"@metadata\"][\"Raven-Entity-Name\"] select new { Tag, LastModified = (DateTime)doc[\"@metadata\"][\"Last-Modified\"] };");
    Map<String, FieldIndexing> indexes = new HashMap<>();
    indexes.put("Tag", FieldIndexing.NOT_ANALYZED);
    indexes.put("LastModified", FieldIndexing.NOT_ANALYZED);
    def.setIndexes(indexes);

    Map<String, FieldStorage> stores = new HashMap<>();
    stores.put("Tag", FieldStorage.NO);
    stores.put("LastModified", FieldStorage.NO);
    def.setStores(stores);

    Map<String, FieldTermVector> termVectors = new HashMap<>();
    termVectors.put("Tag", FieldTermVector.NO);
    termVectors.put("LastModified", FieldTermVector.NO);
    def.setTermVectors(termVectors);

    def.setDisableInMemoryIndexing(true);

    return def;
  }



}
