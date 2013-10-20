package net.ravendb.abstractions.data;

import net.ravendb.abstractions.json.linq.RavenJObject;

public class Attachment {
  private byte[] data;
  private int size;
  private RavenJObject metadata;
  private Etag etag;
  private String key;
  private boolean canGetData;

  /**
   * @return the data
   */
  public byte[] getData() {
    if (!canGetData) {
      throw new IllegalArgumentException("Cannot get attachment data because it was NOT loaded using GET method");
    }
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(byte[] data) {
    this.data = data;
  }

  /**
   * @return the size
   */
  public int getSize() {
    return size;
  }

  /**
   * @param size the size to set
   */
  public void setSize(int size) {
    this.size = size;
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

  /**
   * @return the etag
   */
  public Etag getEtag() {
    return etag;
  }

  /**
   * @param etag the etag to set
   */
  public void setEtag(Etag etag) {
    this.etag = etag;
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

  public Attachment(boolean canGetData, byte[] data, int size, RavenJObject metadata, Etag etag, String key) {
    super();
    this.canGetData = canGetData;
    this.data = data;
    this.size = size;
    this.metadata = metadata;
    this.etag = etag;
    this.key = key;
  }

}
