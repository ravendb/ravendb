package net.ravendb.abstractions.data;

public class PutResult {
  private String key;
  private Etag etag;

  public PutResult(String key, Etag etag) {
    super();
    this.key = key;
    this.etag = etag;
  }

  /**
   * @return the eTag
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

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "PutResult [key=" + key + ", etag=" + etag + "]";
  }

}
