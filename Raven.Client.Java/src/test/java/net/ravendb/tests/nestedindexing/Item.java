package net.ravendb.tests.nestedindexing;

public class Item {

  private String id;
  private String ref;
  private String name;

  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public String getRef() {
    return ref;
  }
  public void setRef(String ref) {
    this.ref = ref;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public Item(String id, String ref, String name) {
    super();
    this.id = id;
    this.ref = ref;
    this.name = name;
  }
  public Item() {
    super();
  }

}
