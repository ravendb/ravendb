package raven.abstractions.data;

/**
 * Transaction information that identify the transaction id and timeout
 */
public class TransactionInformation {
  private String id;
  private Long timeout;


  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public Long getTimeout() {
    return timeout;
  }
  public void setTimeout(Long timeout) {
    this.timeout = timeout;
  }

}
