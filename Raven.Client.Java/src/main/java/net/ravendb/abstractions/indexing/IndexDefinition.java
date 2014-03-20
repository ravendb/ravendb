package net.ravendb.abstractions.indexing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.ravendb.abstractions.data.StringDistanceTypes;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;


public class IndexDefinition {

  /**
   * Initializes a new instance of the {@link IndexDefinition} class.
   */
  public IndexDefinition() {
    maps = new HashSet<>();
    indexes = new HashMap<>();
    stores = new HashMap<>();
    analyzers = new HashMap<>();
    sortOptions = new HashMap<>();
    suggestions = new HashMap<>();
    termVectors = new HashMap<>();
    spatialIndexes = new HashMap<>();

    fields = new ArrayList<>();
  }


  public IndexDefinition(String map) {
    this();
    this.maps.add(map);
  }

  private Long maxIndexOutputsPerDocument;
  private int indexId;
  private String name;
  private IndexLockMode lockMode = IndexLockMode.UNLOCK;
  private Set<String> maps;
  private String reduce;
  private String transformResults;
  private boolean isCompiled;
  private Map<String, FieldStorage> stores;
  private Map<String, FieldIndexing> indexes;
  private Map<String, SortOptions> sortOptions;
  private Map<String, String> analyzers;
  private List<String> fields;
  private Map<String, SuggestionOptions> suggestions;
  private Map<String, FieldTermVector> termVectors;
  private Map<String, SpatialOptions> spatialIndexes;
  private Integer cachedHashCode;
  private boolean disableInMemoryIndexing;

  /**
   * Index specific setting that limits the number of map outputs that an index is allowed to create for a one source document. If a map operation applied to
   * the one document produces more outputs than this number then an index definition will be considered as a suspicious and the index will be marked as errored.
   * Default value: null means that the global value from Raven configuration will be taken to detect if number of outputs was exceeded.
   * @return
   */
  public Long getMaxIndexOutputsPerDocument() {
    return maxIndexOutputsPerDocument;
  }

  public void setMaxIndexOutputsPerDocument(Long maxIndexOutputsPerDocument) {
    this.maxIndexOutputsPerDocument = maxIndexOutputsPerDocument;
  }


  public boolean isDisableInMemoryIndexing() {
    return disableInMemoryIndexing;
  }



  public void setDisableInMemoryIndexing(boolean disableInMemoryIndexing) {
    this.disableInMemoryIndexing = disableInMemoryIndexing;
  }


  public int getIndexId() {
    return indexId;
  }


  public void setIndexId(int indexId) {
    this.indexId = indexId;
  }

  /**
   * @return the termVectors
   */
  public Map<String, FieldTermVector> getTermVectors() {
    return termVectors;
  }


  /**
   * @param termVectors the termVectors to set
   */
  public void setTermVectors(Map<String, FieldTermVector> termVectors) {
    this.termVectors = termVectors;
  }


  /**
   * @return the spatialIndexes
   */
  public Map<String, SpatialOptions> getSpatialIndexes() {
    return spatialIndexes;
  }


  /**
   * @param spatialIndexes the spatialIndexes to set
   */
  public void setSpatialIndexes(Map<String, SpatialOptions> spatialIndexes) {
    this.spatialIndexes = spatialIndexes;
  }


  /**
   * @return the lockMode
   */
  public IndexLockMode getLockMode() {
    return lockMode;
  }


  /**
   * @return the analyzers
   */
  public Map<String, String> getAnalyzers() {
    return analyzers;
  }


  /**
   * @param analyzers the analyzers to set
   */
  public void setAnalyzers(Map<String, String> analyzers) {
    this.analyzers = analyzers;
  }


  /**
   * @return the fields
   */
  public List<String> getFields() {
    return fields;
  }


  /**
   * @param fields the fields to set
   */
  public void setFields(List<String> fields) {
    this.fields = fields;
  }


  /**
   * @return the suggestions
   */
  public Map<String, SuggestionOptions> getSuggestions() {
    return suggestions;
  }


  /**
   * @param suggestions the suggestions to set
   */
  public void setSuggestions(Map<String, SuggestionOptions> suggestions) {
    this.suggestions = suggestions;
  }


  /**
   * @return the stores
   */
  public Map<String, FieldStorage> getStores() {
    return stores;
  }



  /**
   * @param stores the stores to set
   */
  public void setStores(Map<String, FieldStorage> stores) {
    this.stores = stores;
  }



  /**
   * @return the indexes
   */
  public Map<String, FieldIndexing> getIndexes() {
    return indexes;
  }



  /**
   * @param indexes the indexes to set
   */
  public void setIndexes(Map<String, FieldIndexing> indexes) {
    this.indexes = indexes;
  }



  /**
   * @return the sortOptions
   */
  public Map<String, SortOptions> getSortOptions() {
    return sortOptions;
  }



  /**
   * @param sortOptions the sortOptions to set
   */
  public void setSortOptions(Map<String, SortOptions> sortOptions) {
    this.sortOptions = sortOptions;
  }



  /**
   * @param lockMode the lockMode to set
   */
  public void setLockMode(IndexLockMode lockMode) {
    this.lockMode = lockMode;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the maps
   */
  public Set<String> getMaps() {
    return maps;
  }

  /**
   * @param maps the maps to set
   */
  public void setMaps(Set<String> maps) {
    this.maps = maps;
  }


  /**
   * @return the reduce
   */
  public String getReduce() {
    return reduce;
  }

  /**
   * Use Result Transformers instead.
   * @return the transformResults
   */
  @Deprecated
  public String getTransformResults() {
    return transformResults;
  }

  /**
   * Use Result Transformers instead.
   * @param transformResults the transformResults to set
   */
  @Deprecated
  public void setTransformResults(String transformResults) {
    this.transformResults = transformResults;
  }

  /**
   * @param reduce the reduce to set
   */
  public void setReduce(String reduce) {
    this.reduce = reduce;
  }

  public String getMap() {
    if (maps.isEmpty()) {
      return null;
    }
    return maps.iterator().next();
  }

  public void setMap(String value) {
    if (!maps.isEmpty()) {
      maps.remove(maps.iterator().next());
    }
    maps.add(value);

  }
  /**
   * Gets a value indicating whether this instance is map reduce index definition
   * @return <c>true</c> if this instance is map reduce; otherwise, <c>false</c>.
   */
  @JsonProperty("IsMapReduce")
  public boolean isMapReduce() {
    return StringUtils.isNotEmpty(reduce);
  }

  /**
   * @return the isCompiled
   */
  @JsonProperty("IsCompiled")
  public boolean isCompiled() {
    return isCompiled;
  }

  /**
   * @param isCompiled the isCompiled to set
   */
  public void setCompiled(boolean isCompiled) {
    this.isCompiled = isCompiled;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((analyzers == null) ? 0 : analyzers.hashCode());
    result = prime * result + ((indexes == null) ? 0 : indexes.hashCode());
    result = prime * result + ((maps == null) ? 0 : maps.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((reduce == null) ? 0 : reduce.hashCode());
    result = prime * result + ((sortOptions == null) ? 0 : sortOptions.hashCode());
    result = prime * result + ((spatialIndexes == null) ? 0 : spatialIndexes.hashCode());
    result = prime * result + ((stores == null) ? 0 : stores.hashCode());
    result = prime * result + ((suggestions == null) ? 0 : suggestions.hashCode());
    result = prime * result + ((termVectors == null) ? 0 : termVectors.hashCode());
    result = prime * result + ((transformResults == null) ? 0 : transformResults.hashCode());
    return result;
  }


  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    IndexDefinition other = (IndexDefinition) obj;
    if (analyzers == null) {
      if (other.analyzers != null)
        return false;
    } else if (!analyzers.equals(other.analyzers))
      return false;
    if (indexes == null) {
      if (other.indexes != null)
        return false;
    } else if (!indexes.equals(other.indexes))
      return false;
    if (maps == null) {
      if (other.maps != null)
        return false;
    } else if (!maps.equals(other.maps))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (reduce == null) {
      if (other.reduce != null)
        return false;
    } else if (!reduce.equals(other.reduce))
      return false;
    if (sortOptions == null) {
      if (other.sortOptions != null)
        return false;
    } else if (!sortOptions.equals(other.sortOptions))
      return false;
    if (spatialIndexes == null) {
      if (other.spatialIndexes != null)
        return false;
    } else if (!spatialIndexes.equals(other.spatialIndexes))
      return false;
    if (stores == null) {
      if (other.stores != null)
        return false;
    } else if (!stores.equals(other.stores))
      return false;
    if (suggestions == null) {
      if (other.suggestions != null)
        return false;
    } else if (!suggestions.equals(other.suggestions))
      return false;
    if (termVectors == null) {
      if (other.termVectors != null)
        return false;
    } else if (!termVectors.equals(other.termVectors))
      return false;
    if (transformResults == null) {
      if (other.transformResults != null)
        return false;
    } else if (!transformResults.equals(other.transformResults))
      return false;
    return true;
  }


  /**
   * Provide a cached version of the index hash code, which is used when generating
   * the index etag.
   *
   *  It isn't really useful for anything else, in particular, we cache that because
   *  we want to avoid calculating the cost of doing this over and over again on each
   *  query.
   */
  @JsonIgnore
  public int getIndexHash()
  {
    if (cachedHashCode != null)
      return cachedHashCode;

    cachedHashCode = hashCode();
    return cachedHashCode;
  }

  public String getType() {
    String name =  StringUtils.defaultIfEmpty(this.name, "");

    if (name.toLowerCase().startsWith("auto/"))
      return "Auto";
    if (isCompiled())
      return "Compiled";
    if (isMapReduce())
      return "MapReduce";
    return "Map";
  }

  /**
   * Remove the default values that we don't actually need
   */
  public void removeDefaultValues() {
    filterValues(stores, FieldStorage.NO);
    filterValues(indexes, FieldIndexing.DEFAULT);
    filterValues(sortOptions, SortOptions.NONE);

    for (String key: new HashSet<>(analyzers.keySet())) {
      if (StringUtils.isEmpty(analyzers.get(key))) {
        analyzers.remove(key);
      }
    }

    for (String key: new HashSet<>(suggestions.keySet())) {
      if (suggestions.get(key).getDistance() == StringDistanceTypes.NONE) {
        suggestions.remove(key);
      }
    }

    filterValues(termVectors, FieldTermVector.NO);
  }

  private static <T> void filterValues(Map<String, T> map, T valueToRemove) {
    Set<String> keysToRemove = new HashSet<>();
    for (Entry<String, T> entry: map.entrySet()) {
      if (entry.getValue().equals(valueToRemove)) {
        keysToRemove.add(entry.getKey());
      }
    }
    for (String key: keysToRemove) {
      map.remove(key);
    }

  }


  @Override
  public String toString() {
    if (name != null) {
      return name;
    }
    return getMap();
  }


  @Override
  public IndexDefinition clone() {
    IndexDefinition indexDefinition = new IndexDefinition();
    indexDefinition.setIndexId(indexId);
    indexDefinition.setName(name);
    indexDefinition.setReduce(reduce);
    indexDefinition.setTransformResults(transformResults);
    indexDefinition.cachedHashCode = cachedHashCode;

    indexDefinition.setMaxIndexOutputsPerDocument(maxIndexOutputsPerDocument);
    if (maps != null) {
      indexDefinition.setMaps(new HashSet<>(maps));
    }
    if (analyzers != null) {
      indexDefinition.setAnalyzers(new HashMap<>(analyzers));
    }
    if (fields != null) {
      indexDefinition.setFields(new ArrayList<>(fields));
    }
    if (indexes != null) {
      indexDefinition.setIndexes(new HashMap<>(indexes));
    }
    if (sortOptions != null) {
      indexDefinition.setSortOptions(new HashMap<>(sortOptions));
    }
    if (stores != null) {
      indexDefinition.setStores(new HashMap<>(stores));
    }
    if (suggestions != null) {
      indexDefinition.setSuggestions(new HashMap<>(suggestions));
    }
    if (termVectors != null) {
      indexDefinition.setTermVectors(new HashMap<>(termVectors));
    }
    if (spatialIndexes != null) {
      indexDefinition.setSpatialIndexes(new HashMap<>(spatialIndexes));
    }
    return indexDefinition;
  }

}
