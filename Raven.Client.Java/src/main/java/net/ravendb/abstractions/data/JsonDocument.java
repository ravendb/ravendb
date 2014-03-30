package net.ravendb.abstractions.data;

import java.util.Date;

import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.abstractions.util.NetDateFormat;


/**
 * A document representation:
 * - Data / Projection
 * - Etag
 * - Metadata
 */
public class JsonDocument implements IJsonDocumentMetadata {
  private RavenJObject dataAsJson;
  private RavenJObject metadata;
  private String key;
  private Boolean nonAuthoritativeInformation;
  private Etag etag;
  private Date lastModified;
  private Float tempIndexScore;

  public JsonDocument(RavenJObject dataAsJson, RavenJObject metadata, String key, Boolean nonAuthoritativeInformation, Etag etag, Date lastModified) {
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
    if (metadata == null) {
      metadata = new RavenJObject(String.CASE_INSENSITIVE_ORDER);
    }
    return metadata;
  }

  public Float getTempIndexScore() {
    return tempIndexScore;
  }

  /**
   * @return the nonAuthoritativeInformation
   */
  @Override
  public Boolean getNonAuthoritativeInformation() {
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
   * @param nonAuthoritativeInformation the nonAuthoritativeInformation to set
   */
  @Override
  public void setNonAuthoritativeInformation(Boolean nonAuthoritativeInformation) {
    this.nonAuthoritativeInformation = nonAuthoritativeInformation;
  }

  public void setTempIndexScore(Float tempIndexScore) {
    this.tempIndexScore = tempIndexScore;
  }

  public RavenJObject toJson() {
    dataAsJson.ensureCannotBeChangeAndEnableShapshotting();
    metadata.ensureCannotBeChangeAndEnableShapshotting();

    RavenJObject doc = dataAsJson.createSnapshot();
    RavenJObject metadata = this.metadata.createSnapshot();

    if (lastModified != null) {
      NetDateFormat fdf = new NetDateFormat();
      metadata.add(Constants.LAST_MODIFIED, new RavenJValue(this.lastModified));
      metadata.add(Constants.RAVEN_LAST_MODIFIED, new RavenJValue(fdf.format(this.lastModified)));
    }
    if (etag != null) {
      metadata.add("@etag", new RavenJValue(etag.toString()));
    }
    if (nonAuthoritativeInformation) {
      metadata.add("Non-Authoritative-Information", new RavenJValue(nonAuthoritativeInformation));
    }
    doc.add("@metadata", metadata);

    return doc;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "JsonDocument [dataAsJson=" + dataAsJson + ", metadata=" + metadata + ", key=" + key + ", etag=" + etag + ", lastModified=" + lastModified + "]";
  }

}
