package raven.client.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import raven.client.IDocumentSession;

public class DocumentStoreTest {
  public static void main(String[] args) {
    try (DocumentStore documentStore = new DocumentStore()) {
      documentStore.parseConnectionString("Url=http://localhost:8123;");
      documentStore.getConventions().setEnlistInDistributedTransactions(false);
      documentStore.initialize();

      try (IDocumentSession session = documentStore.openSession()) {
        Animal a = new Animal();
        a.setName("Dog");
        session.store(a);
        session.saveChanges();
        session.saveChanges();
        assertNotNull(a.getId());

        Integer animalId = a.getId();
        Animal animal = session.load(Animal.class, animalId);
        assertEquals("Dog", animal.getName());

        session.delete(animal);
        session.saveChanges();

        assertNull(session.load(Animal.class, animalId));
      }


    } catch (Exception e) {
      e.printStackTrace();
    }
  }

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
