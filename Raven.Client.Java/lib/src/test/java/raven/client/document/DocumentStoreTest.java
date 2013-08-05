package raven.client.document;

import raven.client.IDocumentSession;
import raven.samples.Person;

public class DocumentStoreTest {
  public static void main(String[] args) {
    try (DocumentStore documentStore = new DocumentStore()) {
      documentStore.parseConnectionString("Url=http://localhost:8123;");
      documentStore.initialize();

      try (IDocumentSession session = documentStore.openSession()) {


        session.store(new Person());
        session.saveChanges();
      }


    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
