package raven.abstractions.data;

import java.util.Date;
import java.util.UUID;

import raven.abstractions.json.linq.RavenJObject;

public class JsonDocumentMetadata {

  private RavenJObject metadata;
  private String key;
  private Boolean monAuthoritativeInformation;
  private UUID etag;
  private Date lastModified;

  /**
   * @return the etag
   */
  public UUID getEtag() {
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

  /**
   * @return the monAuthoritativeInformation
   */
  public Boolean getMonAuthoritativeInformation() {
    return monAuthoritativeInformation;
  }

  /**
   * @param etag the etag to set
   */
  public void setEtag(UUID etag) {
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
   * @param monAuthoritativeInformation the monAuthoritativeInformation to set
   */
  public void setMonAuthoritativeInformation(Boolean monAuthoritativeInformation) {
    this.monAuthoritativeInformation = monAuthoritativeInformation;
  }

}
