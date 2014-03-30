package net.ravendb.abstractions.data;

import java.util.Date;

import net.ravendb.abstractions.json.linq.RavenJObject;


public class JsonDocumentMetadata implements IJsonDocumentMetadata {

  private RavenJObject metadata;
  private String key;
  private Boolean nonAuthoritativeInformation;
  private Etag etag;
  private Date lastModified;

  /**
   * @return the etag
   */
  @Override
  public Etag getEtag() {
    return etag;
  }

  /**
   * @return the key
   */
  @Override
  public String getKey() {
    return key;
  }

  /**
   * @return the lastModified
   */
  @Override
  public Date getLastModified() {
    return lastModified;
  }

  /**
   * @return the metadata
   */
  @Override
  public RavenJObject getMetadata() {
    return metadata;
  }

  /**
   * @return the monAuthoritativeInformation
   */
  @Override
  public Boolean getNonAuthoritativeInformation() {
    return nonAuthoritativeInformation;
  }

  /**
   * @param etag the etag to set
   */
  @Override
  public void setEtag(Etag etag) {
    this.etag = etag;
  }

  /**
   * @param key the key to set
   */
  @Override
  public void setKey(String key) {
    this.key = key;
  }

  /**
   * @param lastModified the lastModified to set
   */
  @Override
  public void setLastModified(Date lastModified) {
    this.lastModified = lastModified;
  }

  /**
   * @param metadata the metadata to set
   */
  @Override
  public void setMetadata(RavenJObject metadata) {
    this.metadata = metadata;
  }

  /**
   * @param monAuthoritativeInformation the nonAuthoritativeInformation to set
   */
  @Override
  public void setNonAuthoritativeInformation(Boolean nonAuthoritativeInformation) {
    this.nonAuthoritativeInformation = nonAuthoritativeInformation;
  }

}
