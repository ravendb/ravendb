package net.ravendb.tests.multiget;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import net.ravendb.abstractions.basic.Lazy;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.QUser;
import net.ravendb.tests.bugs.User;

import org.junit.Test;


public class MultiGetNonStaleResultsTest extends RemoteClientTest {

  @Test
  public void shouldBeAbleToGetNonStaleResults() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        QUser u = QUser.user;
        session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .where(u.name.eq("oren")).toList();
      }

      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("oren");
        User user3 = new User();
        user3.setName("ayende");
        session.store(user1);
        session.store(new User());
        session.store(user3);
        session.store(new User());
        session.saveChanges();
      }

      waitForAllRequestsToComplete();

      try (IDocumentSession session = store.openSession()) {
        QUser u = QUser.user;
        Lazy<List<User>> result1 = session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
          .where(u.name.eq("oren")).lazily();

        assertNotNull(result1.getValue());
        assertEquals(1, session.advanced().getNumberOfRequests());
      }

    }
  }
}
