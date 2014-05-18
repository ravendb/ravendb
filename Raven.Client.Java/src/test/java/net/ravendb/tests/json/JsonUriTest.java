package net.ravendb.tests.json;

import static org.junit.Assert.assertNotNull;

import java.net.URI;

import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


public class JsonUriTest extends RemoteClientTest {

  public static class ObjectWithUri {
    private URI uri;

    public URI getUri() {
      return uri;
    }

    public void setUri(URI uri) {
      this.uri = uri;
    }

  }

  @Test
  public void can_serialize_uri_props_correctly() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        ObjectWithUri objectWithUri = new ObjectWithUri();
        objectWithUri.setUri(new URI("http://test.com/%22foo+bar%22"));
        session.store(objectWithUri, "test");
        session.saveChanges();
      }

      try (IDocumentSession session  =store.openSession()) {
        ObjectWithUri uri = session.load(ObjectWithUri.class, "test");
        assertNotNull(uri);
      }
    }
  }
}
