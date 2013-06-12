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
   * @return the key
   */
  public String getKey() {
    return key;
  }
  /**
   * @return the eTag
   */
  public UUID getEtag() {
    return etag;
  }
  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "PutResult [key=" + key + ", etag=" + etag + "]";
  }


}
