package net.ravendb.client.document;

public class DocumentStoreTest {

  public static class Animal {
    private Integer id;
    private String name;


    public Integer getId() {
      return id;
    }

    public void setId(Integer id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

  }

}
