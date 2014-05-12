package net.ravendb.client;

import static org.junit.Assert.assertNotNull;
import net.ravendb.abstractions.basic.CloseableIterator;
import net.ravendb.abstractions.data.StreamResult;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.User;

import org.junit.Test;

public class StreamDocumentTest extends RemoteClientTest  {
  @Test
  public void streamingTest() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        for (int i = 0 ; i < 1000; i++) {
          User user = new User();
          user.setName("ayende");
          session.store(user);
        }
        session.saveChanges();

        CloseableIterator<StreamResult<User>> stream = session.advanced().stream(User.class);
        for (int i =0 ;i < 10;i++) {
          StreamResult<User> streamResult = stream.next();
          assertNotNull(streamResult.getDocument());
        }
        stream.close();
      }
    }
  }
}
