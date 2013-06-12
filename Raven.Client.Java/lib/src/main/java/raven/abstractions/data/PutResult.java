package raven.abstractions.data;

import java.util.UUID;

public class PutResult {
  private String key;
  private UUID etag;

  public PutResult(String key, UUID etag) {
    super();
    this.key = key;
    this.etag = etag;
  }

  /**
   * @return the eTag
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

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "PutResult [key=" + key + ", etag=" + etag + "]";
  }

}
