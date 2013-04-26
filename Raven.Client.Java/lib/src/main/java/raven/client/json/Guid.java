package raven.client.json;

public class Guid {
  private String value;


  public Guid(String value) {
    super();
    this.value = value;
  }

  /**
   * @return the value
   */
  public String getValue() {
    return value;
  }

  /**
   * @param value the value to set
   */
  public void setValue(String value) {
    this.value = value;
  }


}
