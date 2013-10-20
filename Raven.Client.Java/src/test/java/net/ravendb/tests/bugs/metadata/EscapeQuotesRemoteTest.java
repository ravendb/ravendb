package net.ravendb.tests.bugs.metadata;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.User;

import org.junit.Test;


public class EscapeQuotesRemoteTest extends RemoteClientTest {
  @Test
  public void canProperlyEscapeQuotesInMetadata_Remote_1() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
          User user = new User();
          session.store(user);
          session.advanced().getMetadataFor(user).add("Foo", new RavenJValue("\"Bar\""));
          session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        User user = session.load(User.class, "users/1");
        RavenJObject metadata = session.advanced().getMetadataFor(user);
        assertEquals("\"Bar\"", metadata.value(String.class, "Foo"));
      }

    }
  }

  @Test
  public void canProperlyEscapeQuotesInMetadata_2() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
          User user = new User();
          session.store(user);
          session.advanced().getMetadataFor(user).add("Foo", new RavenJValue("\\\"Bar\\\""));
          session.saveChanges();
      }

      try (IDocumentSession session = store.openSession()) {
        User user = session.load(User.class, "users/1");
        RavenJObject metadata = session.advanced().getMetadataFor(user);
        assertEquals("\\\"Bar\\\"", metadata.value(String.class, "Foo"));
      }
    }
  }

}
