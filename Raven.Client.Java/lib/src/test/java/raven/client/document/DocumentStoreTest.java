package raven.client.document;

import raven.client.IDocumentSession;

public class DocumentStoreTest {
  public static void main(String[] args) {
    try (DocumentStore documentStore = new DocumentStore()) {
      documentStore.parseConnectionString("Url=http://localhost:8123;");
      documentStore.getConventions().setEnlistInDistributedTransactions(false);
      documentStore.initialize();

      try (IDocumentSession session = documentStore.openSession()) {

        session.store(new Animal());
        session.saveChanges();
      }


    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static class Animal {
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
