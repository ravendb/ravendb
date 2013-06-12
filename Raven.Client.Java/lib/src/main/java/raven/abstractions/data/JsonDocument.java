package raven.abstractions.data;

import java.util.Date;
import java.util.UUID;

import raven.abstractions.json.linq.RavenJObject;


/**
 * A document representation:
 * - Data / Projection
 * - Etag
 * - Metadata
 *
 */
public class JsonDocument {
  private RavenJObject dataAsJson;
  private RavenJObject metadata;
  private String key;
  private boolean nonAuthoritativeInformation;
  private UUID etag;
  private Date lastModified;



  public JsonDocument(RavenJObject dataAsJson, RavenJObject metadata, String key, boolean nonAuthoritativeInformation, UUID etag, Date lastModified) {
    super();
    this.dataAsJson = dataAsJson;
    this.metadata = metadata;
    this.key = key;
    this.nonAuthoritativeInformation = nonAuthoritativeInformation;
    this.etag = etag;
    this.lastModified = lastModified;
  }
  /**
   * @return the key
   */
  public String getKey() {
    return key;
  }
  /**
   * @param key the key to set
   */
  public void setKey(String key) {
    this.key = key;
  }
  /**
   * @return the nonAuthoritativeInformation
   */
  public boolean isNonAuthoritativeInformation() {
    return nonAuthoritativeInformation;
  }
  /**
   * @param nonAuthoritativeInformation the nonAuthoritativeInformation to set
   */
  public void setNonAuthoritativeInformation(boolean nonAuthoritativeInformation) {
    this.nonAuthoritativeInformation = nonAuthoritativeInformation;
  }
  /**
   * @return the etag
   */
  public UUID getEtag() {
    return etag;
  }
  /**
   * @param etag the etag to set
   */
  public void setEtag(UUID etag) {
    this.etag = etag;
  }
  /**
   * @return the lastModified
   */
  public Date getLastModified() {
    return lastModified;
  }
  /**
   * @param lastModified the lastModified to set
   */
  public void setLastModified(Date lastModified) {
    this.lastModified = lastModified;
  }
  /**
   * @return the dataAsJson
   */
  public RavenJObject getDataAsJson() {
    return dataAsJson != null ? dataAsJson : new RavenJObject();
  }
  /**
   * @param dataAsJson the dataAsJson to set
   */
  public void setDataAsJson(RavenJObject dataAsJson) {
    this.dataAsJson = dataAsJson;
  }
  /**
   * @return the metadata
   */
  public RavenJObject getMetadata() {
    return metadata;
  }
  /**
   * @param metadata the metadata to set
   */
  public void setMetadata(RavenJObject metadata) {
    this.metadata = metadata;
  }
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "JsonDocument [dataAsJson=" + dataAsJson + ", metadata=" + metadata + ", key=" + key + ", etag=" + etag + ", lastModified=" + lastModified + "]";
  }



}
