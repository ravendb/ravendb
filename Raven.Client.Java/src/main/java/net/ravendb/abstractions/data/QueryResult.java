package net.ravendb.abstractions.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.ravendb.abstractions.json.linq.RavenJObject;


public class QueryResult {
  private List<RavenJObject> results;
  private List<RavenJObject> includes;
  private boolean isStale;
  private Date indexTimestamp;
  private int totalResults;
  private int skippedResults;
  private String indexName;
  private Etag indexEtag;
  private Etag resultEtag;
  private Map<String, Map<String , String[]>> highlightings;
  private boolean nonAuthoritativeInformation;
  private Date lastQueryTime;
  private long durationMiliseconds;
  private Map<String, String> scoreExplanations;

  /**
   * Initializes a new instance of the {@link QueryResult} class.
   */
  public QueryResult() {
    results = new ArrayList<>();
    includes = new ArrayList<>();
    highlightings = new HashMap<>();
    scoreExplanations = new HashMap<>();
  }

  public Map<String, String> getScoreExplanations() {
    return scoreExplanations;
  }


  public void setScoreExplanations(Map<String, String> scoreExplanations) {
    this.scoreExplanations = scoreExplanations;
  }

  /**
   * Creates a snapshot of the query results
   * @return
   */
  public QueryResult createSnapshot() {
    QueryResult snapshot = new QueryResult();
    for (RavenJObject obj: results) {
      snapshot.getResults().add(obj.createSnapshot());
    }
    for (RavenJObject obj: includes) {
      snapshot.getIncludes().add(obj.createSnapshot());
    }
    snapshot.setIndexEtag(getIndexEtag());
    snapshot.setIndexName(getIndexName());
    snapshot.setIndexTimestamp(getIndexTimestamp());
    snapshot.setStale(isStale);
    snapshot.setSkippedResults(skippedResults);
    snapshot.setTotalResults(getTotalResults());

    for (Map.Entry<String, Map<String, String[]>> entry: highlightings.entrySet()) {
      snapshot.getHighlightings().put(entry.getKey(), new HashMap<>(entry.getValue()));
    }

    for (Map.Entry<String, String> entry: scoreExplanations.entrySet()) {
      snapshot.getScoreExplanations().put(entry.getKey(), entry.getValue());
    }
    return snapshot;
  }

  /**
   * Ensures that the query results can be used in snapshots
   */
  public void ensureSnapshot() {
    for(RavenJObject result: results) {
      result.ensureCannotBeChangeAndEnableShapshotting();
    }
    for(RavenJObject include: includes) {
      include.ensureCannotBeChangeAndEnableShapshotting();
    }
  }

  public long getDurationMiliseconds() {
    return durationMiliseconds;
  }
  public Map<String, Map<String, String[]>> getHighlightings() {
    return highlightings;
  }
  public Collection<RavenJObject> getIncludes() {
    return includes;
  }
  public Etag getIndexEtag() {
    return indexEtag;
  }
  public String getIndexName() {
    return indexName;
  }
  public Date getIndexTimestamp() {
    return indexTimestamp;
  }
  public Date getLastQueryTime() {
    return lastQueryTime;
  }
  public Etag getResultEtag() {
    return resultEtag;
  }
  public List<RavenJObject> getResults() {
    return results;
  }
  public int getSkippedResults() {
    return skippedResults;
  }
  public int getTotalResults() {
    return totalResults;
  }
  public boolean isNonAuthoritativeInformation() {
    return nonAuthoritativeInformation;
  }
  public boolean isStale() {
    return isStale;
  }
  public void setDurationMiliseconds(long durationMiliseconds) {
    this.durationMiliseconds = durationMiliseconds;
  }
  public void setHighlightings(Map<String, Map<String, String[]>> highlightings) {
    this.highlightings = highlightings;
  }
  public void setIncludes(List<RavenJObject> includes) {
    this.includes = includes;
  }
  public void setIndexEtag(Etag indexEtag) {
    this.indexEtag = indexEtag;
  }
  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }
  public void setIndexTimestamp(Date indexTimestamp) {
    this.indexTimestamp = indexTimestamp;
  }
  public void setLastQueryTime(Date lastQueryTime) {
    this.lastQueryTime = lastQueryTime;
  }
  public void setNonAuthoritativeInformation(boolean nonAuthoritativeInformation) {
    this.nonAuthoritativeInformation = nonAuthoritativeInformation;
  }
  public void setResultEtag(Etag resultEtag) {
    this.resultEtag = resultEtag;
  }
  public void setResults(List<RavenJObject> results) {
    this.results = results;
  }
  public void setSkippedResults(int skippedResults) {
    this.skippedResults = skippedResults;
  }
  public void setStale(boolean isStale) {
    this.isStale = isStale;
  }
  public void setTotalResults(int totalResults) {
    this.totalResults = totalResults;
  }




}
