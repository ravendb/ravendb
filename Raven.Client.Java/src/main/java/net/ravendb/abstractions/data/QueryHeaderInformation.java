package net.ravendb.abstractions.data;

import java.util.Date;


public class QueryHeaderInformation {
  private String index;
  private boolean isStable;
  private Date indexTimestamp;
  private int totalResults;
  private Etag resultEtag;
  private Etag indexEtag;


  public String getIndex() {
    return index;
  }
  public void setIndex(String index) {
    this.index = index;
  }
  public boolean isStable() {
    return isStable;
  }
  public void setStable(boolean isStable) {
    this.isStable = isStable;
  }
  public Date getIndexTimestamp() {
    return indexTimestamp;
  }
  public void setIndexTimestamp(Date indexTimestamp) {
    this.indexTimestamp = indexTimestamp;
  }
  public int getTotalResults() {
    return totalResults;
  }
  public void setTotalResults(int totalResults) {
    this.totalResults = totalResults;
  }
  public Etag getResultEtag() {
    return resultEtag;
  }
  public void setResultEtag(Etag resultEtag) {
    this.resultEtag = resultEtag;
  }
  public Etag getIndexEtag() {
    return indexEtag;
  }
  public void setIndexEtag(Etag indexEtag) {
    this.indexEtag = indexEtag;
  }



}
