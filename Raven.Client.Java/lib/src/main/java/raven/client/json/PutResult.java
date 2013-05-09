package raven.client.json;

import java.util.UUID;

public class PutResult {
  private String key;
  private UUID etag;
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
   * @return the eTag
   */
  public UUID geteTag() {
    return etag;
  }
  /**
   * @param eTag the eTag to set
   */
  public void seteTag(UUID eTag) {
    this.etag = eTag;
  }


}
