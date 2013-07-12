package raven.abstractions.data;

import java.util.Date;

import raven.abstractions.json.linq.RavenJObject;

/**
 * A document representation:
 * - Data / Projection
 * - Etag
 * - Metadata
 */
//TODO: finish me
public class JsonDocument {
  private RavenJObject dataAsJson;
  private RavenJObject metadata;
  private String key;
  private boolean nonAuthoritativeInformation;
  private Etag etag;
  private Date lastModified;
  private Float tempIndexScore;

  public JsonDocument(RavenJObject dataAsJson, RavenJObject metadata, String key, boolean nonAuthoritativeInformation, Etag etag, Date lastModified) {
    super();
    this.dataAsJson = dataAsJson;
    this.metadata = metadata;
    this.key = key;
    this.nonAuthoritativeInformation = nonAuthoritativeInformation;
    this.etag = etag;
    this.lastModified = lastModified;
  }

  /**
   * @return the dataAsJson
   */
  public RavenJObject getDataAsJson() {
    return dataAsJson != null ? dataAsJson : new RavenJObject();
  }

  /**
   * @return the etag
   */
  public Etag getEtag() {
    return etag;
  }



  /**
   * @return the key
   */
  public String getKey() {
    return key;
  }

  /**
   * @return the lastModified
   */
  public Date getLastModified() {
    return lastModified;
  }

  /**
   * @return the metadata
   */
  public RavenJObject getMetadata() {
    return metadata;
  }

  public Float getTempIndexScore() {
    return tempIndexScore;
  }

  /**
   * @return the nonAuthoritativeInformation
   */
  public boolean isNonAuthoritativeInformation() {
    return nonAuthoritativeInformation;
  }

  /**
   * @param dataAsJson the dataAsJson to set
   */
  public void setDataAsJson(RavenJObject dataAsJson) {
    this.dataAsJson = dataAsJson;
  }

  /**
   * @param etag the etag to set
   */
  public void setEtag(Etag etag) {
    this.etag = etag;
  }

  /**
   * @param key the key to set
   */
  public void setKey(String key) {
    this.key = key;
  }

  /**
   * @param lastModified the lastModified to set
   */
  public void setLastModified(Date lastModified) {
    this.lastModified = lastModified;
  }

  /**
   * @param metadata the metadata to set
   */
  public void setMetadata(RavenJObject metadata) {
    this.metadata = metadata;
  }

  /**
   * @param nonAuthoritativeInformation the nonAuthoritativeInformation to set
   */
  public void setNonAuthoritativeInformation(boolean nonAuthoritativeInformation) {
    this.nonAuthoritativeInformation = nonAuthoritativeInformation;
  }

  public void setTempIndexScore(Float tempIndexScore) {
    this.tempIndexScore = tempIndexScore;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "JsonDocument [dataAsJson=" + dataAsJson + ", metadata=" + metadata + ", key=" + key + ", etag=" + etag + ", lastModified=" + lastModified + "]";
  }

}
