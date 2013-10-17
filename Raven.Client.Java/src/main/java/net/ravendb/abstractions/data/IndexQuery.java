package net.ravendb.abstractions.data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.util.NetDateFormat;
import net.ravendb.client.utils.UrlUtils;

import org.apache.commons.lang.StringUtils;


/**
 * All the information required to query a Raven index
 */
public class IndexQuery {
  private int pageSize;

  /**
   * Initializes a new instance of the {@link IndexQuery} class.
   */
  public IndexQuery() {
    totalSize = new Reference<>();
    skippedResults = new Reference<>();
    pageSize = 128;
  }

  public IndexQuery(String query) {
    this();
    this.query = query;
  }



  private boolean pageSizeSet;
  private boolean distinct;
  private String query;
  private Reference<Integer> totalSize;
  private Map<String, RavenJToken> queryInputs;
  private int start;
  private String[] fieldsToFetch;
  private SortedField[] sortedFields;
  private Date cutoff;
  private Etag cutoffEtag;
  private String defaultField;
  private QueryOperator defaultOperator = QueryOperator.OR;
  private boolean skipTransformResults;
  private Reference<Integer> skippedResults;
  private boolean debugOptionGetIndexEntires;
  private HighlightedField[] highlightedFields;
  private String[] highlighterPreTags;
  private String[] highlighterPostTags;
  private String resultsTransformer;
  private boolean disableCaching;
  private boolean explainScores;

  public String getResultsTransformer() {
    return resultsTransformer;
  }

  public void setResultsTransformer(String resultsTransformer) {
    this.resultsTransformer = resultsTransformer;
  }

  /**
   * Whatever we should apply distinct operation to the query on the server side
   * @return
   */
  public boolean isDistinct() {
    return distinct;
  }


  public void setDistinct(boolean distinct) {
    this.distinct = distinct;
  }

  public HighlightedField[] getHighlightedFields() {
    return highlightedFields;
  }

  public void setHighlightedFields(HighlightedField[] highlightedFields) {
    this.highlightedFields = highlightedFields;
  }

  public String[] getHighlighterPreTags() {
    return highlighterPreTags;
  }



  public void setHighlighterPreTags(String[] highlighterPreTags) {
    this.highlighterPreTags = highlighterPreTags;
  }



  public String[] getHighlighterPostTags() {
    return highlighterPostTags;
  }



  public void setHighlighterPostTags(String[] highlighterPostTags) {
    this.highlighterPostTags = highlighterPostTags;
  }



  public boolean isDisableCaching() {
    return disableCaching;
  }



  public void setDisableCaching(boolean disableCaching) {
    this.disableCaching = disableCaching;
  }



  public boolean isDebugOptionGetIndexEntires() {
    return debugOptionGetIndexEntires;
  }


  public boolean isExplainScores() {
    return explainScores;
  }

  public void setExplainScores(boolean explainScores) {
    this.explainScores = explainScores;
  }

  public void setDebugOptionGetIndexEntires(boolean debugOptionGetIndexEntires) {
    this.debugOptionGetIndexEntires = debugOptionGetIndexEntires;
  }



  public Reference<Integer> getSkippedResults() {
    return skippedResults;
  }



  public void setSkippedResults(Reference<Integer> skippedResults) {
    this.skippedResults = skippedResults;
  }



  public boolean isSkipTransformResults() {
    return skipTransformResults;
  }



  public void setSkipTransformResults(boolean skipTransformResults) {
    this.skipTransformResults = skipTransformResults;
  }



  public QueryOperator getDefaultOperator() {
    return defaultOperator;
  }



  public void setDefaultOperator(QueryOperator defaultOperator) {
    this.defaultOperator = defaultOperator;
  }



  /**
   * Cutoff etag is used to check if the index has already process a document with the given
   * etag. Unlike Cutoff, which uses dates and is susceptible to clock synchronization issues between
   * machines, cutoff etag doesn't rely on both the server and client having a synchronized clock and
   * can work without it.
   * However, when used to query map/reduce indexes, it does NOT guarantee that the document that this
   * etag belong to is actually considered for the results.
   * What it does it guarantee that the document has been mapped, but not that the mapped values has been reduce.
   * Since map/reduce queries, by their nature,tend to be far less susceptible to issues with staleness, this is
   * considered to be an acceptable tradeoff.
   * If you need absolute no staleness with a map/reduce index, you will need to ensure synchronized clocks and
   * use the Cutoff date option, instead.
   * @return
   */
  public Etag getCutoffEtag() {
    return cutoffEtag;
  }



  public String getDefaultField() {
    return defaultField;
  }



  public void setDefaultField(String defaultField) {
    this.defaultField = defaultField;
  }



  public void setCutoffEtag(Etag cutoffEtag) {
    this.cutoffEtag = cutoffEtag;
  }

  public Date getCutoff() {
    return cutoff;
  }

  public void setCutoff(Date cutoff) {
    this.cutoff = cutoff;
  }

  public SortedField[] getSortedFields() {
    return sortedFields;
  }

  public void setSortedFields(SortedField[] sortedFields) {
    this.sortedFields = sortedFields;
  }

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public Map<String, RavenJToken> getQueryInputs() {
    return queryInputs;
  }

  public void setQueryInputs(Map<String, RavenJToken> queryInputs) {
    this.queryInputs = queryInputs;
  }

  public Reference<Integer> getTotalSize() {
    return totalSize;
  }

  /**
   * Whatever the page size was explicitly set or still at its default value
   * @return
   */
  public boolean isPageSizeSet() {
    return pageSizeSet;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
    this.pageSizeSet = true;
  }

  public String[] getFieldsToFetch() {
    return fieldsToFetch;
  }

  public void setFieldsToFetch(String[] fieldsToFetch) {
    this.fieldsToFetch = fieldsToFetch;
  }


  public String getIndexQueryUrl(String operationUrl, String index, String operationName) {
    return getIndexQueryUrl(operationUrl, index, operationName, true);
  }
  /**
   * Gets the index query URL.
   * @param operationUrl
   * @param index
   * @param operationName
   * @param includePageSizeEvenIfNotExplicitlySet
   * @return
   */
  public String getIndexQueryUrl(String operationUrl, String index, String operationName, boolean includePageSizeEvenIfNotExplicitlySet) {
    if (operationUrl.endsWith("/"))
      operationUrl = operationUrl.substring(0, operationUrl.length() - 1);
    StringBuilder path = new StringBuilder()
    .append(operationUrl)
    .append("/")
    .append(operationName)
    .append("/")
    .append(index);

    appendQueryString(path, includePageSizeEvenIfNotExplicitlySet);

    return path.toString();
  }

  public String getMinimalQueryString() {
    StringBuilder sb = new StringBuilder();
    appendMinimalQueryString(sb);
    return sb.toString();
  }


  public String getQueryString() {
    StringBuilder sb = new StringBuilder();
    appendQueryString(sb);
    return sb.toString();
  }

  public void appendQueryString(StringBuilder path){
    appendQueryString(path, true);
  }

  public void appendQueryString(StringBuilder path, boolean includePageSizeEvenIfNotExplicitlySet) {
    path.append("?");

    appendMinimalQueryString(path);

    if (start != 0) {
      path.append("&start=").append(start);
    }

    if (includePageSizeEvenIfNotExplicitlySet || pageSizeSet) {
      path.append("&pageSize=").append(pageSize);
    }

    if (fieldsToFetch != null) {
      for (String field: fieldsToFetch) {
        if (StringUtils.isNotEmpty(field)) {
          path.append("&fetch=").append(UrlUtils.escapeDataString(field));
        }
      }
    }

    if (sortedFields != null) {
      for (SortedField field: sortedFields) {
        if (field != null) {
          path.append("&sort=").append(field.isDescending()? "-" : "").append(UrlUtils.escapeDataString(field.getField()));
        }
      }
    }

    if (skipTransformResults) {
      path.append("&skipTransformResults=true");
    }

    if (StringUtils.isNotEmpty(resultsTransformer)) {
      path.append("&resultsTransformer=").append(UrlUtils.escapeDataString(resultsTransformer));
    }

    if (queryInputs != null) {
      for (Map.Entry<String, RavenJToken> input: queryInputs.entrySet()) {
        path.append("&qp-").append(input.getKey()).append("=").append(input.getValue().toString());
      }
    }

    if (cutoff != null) {
      NetDateFormat sdf = new NetDateFormat();
      String cutOffAsString = UrlUtils.escapeDataString(sdf.format(cutoff));
      path.append("&cufOff=").append(cutOffAsString);
    }
    if (cutoffEtag != null) {
      path.append("&cutOffEtag=").append(cutoffEtag);
    }
    if (highlightedFields != null) {
      for( HighlightedField field: highlightedFields) {
        path.append("&highlight=").append(field);
      }
    }
    if (highlighterPreTags != null) {
      for(String preTag: highlighterPreTags) {
        path.append("&preTags=").append(UrlUtils.escapeUriString(preTag));
      }
    }

    if (highlighterPostTags != null) {
      for (String postTag: highlighterPostTags) {
        path.append("&postTags=").append(UrlUtils.escapeUriString(postTag));
      }
    }

    if (debugOptionGetIndexEntires) {
      path.append("&debug=entries");
    }
  }

  private void appendMinimalQueryString(StringBuilder path) {
    if (StringUtils.isNotEmpty(query)) {
      path.append("&query=").append(UrlUtils.escapeDataString(query));
    }

    if (StringUtils.isNotEmpty(defaultField)) {
      path.append("&defaultField=").append(UrlUtils.escapeDataString(defaultField));
    }
    if (defaultOperator != QueryOperator.OR) {
      path.append("&operator=AND");
    }
    String vars = getCustomQueryStringVariables();
    if (StringUtils.isNotEmpty(vars)) {
      path.append(vars.startsWith("&") ? vars : ("&" + vars));
    }
  }

  protected String getCustomQueryStringVariables() {
    return "";
  }

  @Override
  public IndexQuery clone() {
    try {

      IndexQuery clone = new IndexQuery();

      clone.pageSizeSet = pageSizeSet;
      clone.query = query;
      clone.totalSize = totalSize;
      clone.queryInputs = new HashMap<>();
      for (String key: queryInputs.keySet()) {
        clone.queryInputs.put(key, queryInputs.get(key).cloneToken());
      }
      clone.start = start;
      clone.fieldsToFetch = fieldsToFetch.clone();
      clone.sortedFields = new SortedField[sortedFields.length];
      for (int i = 0 ; i <  sortedFields.length; i++) {
        clone.sortedFields[i] = sortedFields[i].clone();
      }
      clone.cutoff = cutoff;
      clone.cutoffEtag = cutoffEtag.clone();
      clone.defaultField = defaultField;
      clone.defaultOperator = defaultOperator;
      clone.skipTransformResults = skipTransformResults;
      clone.skippedResults = new Reference<>(skippedResults.value);
      clone.debugOptionGetIndexEntires = debugOptionGetIndexEntires;
      clone.highlightedFields = new HighlightedField[highlightedFields.length];
      for (int i = 0; i < highlightedFields.length; i++) {
        clone.highlightedFields[i] = highlightedFields[i].clone();
      }
      clone.highlighterPreTags = highlighterPreTags.clone();
      clone.highlighterPostTags = highlighterPostTags.clone();
      clone.resultsTransformer = resultsTransformer;
      clone.disableCaching = disableCaching;

      return clone;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return query;
  }

}
