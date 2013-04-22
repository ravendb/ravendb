package raven.client.json;

public class PutResult {
  private String key;
  private Guid eTag;
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
  public Guid geteTag() {
    return eTag;
  }
  /**
   * @param eTag the eTag to set
   */
  public void seteTag(Guid eTag) {
    this.eTag = eTag;
  }


}
