package raven.client.document;

import static org.junit.Assert.assertEquals;

public class DocumentStoreTest {
  public static void main(String[] args) {
    try (DocumentStore documentStore = new DocumentStore()) {
      documentStore.parseConnectionString("Url=http://localhost:8123;");

      assertEquals("http://localhost:8123", documentStore.getUrl());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
