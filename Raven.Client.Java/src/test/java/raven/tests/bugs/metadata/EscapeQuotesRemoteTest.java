package raven.tests.bugs.metadata;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentStore;
import raven.tests.bugs.User;

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
