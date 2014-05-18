package net.ravendb.tests.notmodified;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.connection.HttpExtensions;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.User;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;


/**
 * The tests in this class test that 304 is returned when nothing at all is changed, and that 200 is returned when the
 * object being tested is changed; they do not test what happens when other objects are changed
 */
public class NotModifiedTest extends RemoteClientTest {

  @Test
  public void serverReturnsNotModifiedWhenAppropriateForDocument() throws Exception {
    User user1 = new User();
    user1.setId("users/ayende");
    user1.setName("Ayende");

    User user2 = new User();
    user2.setId("users/ayende");
    user2.setName("Rahien");

    runNotModifiedTestsForUrl(user1, user2, getDefaultUrl() + "/docs/users/ayende");
  }

  @Test
  public void serverReturnsNotModifiedWhenAppropriateForAllDocs() throws Exception {
    User user1 = new User();
    user1.setId("users/ayende");
    user1.setName("Ayende");

    User user2 = new User();
    user2.setId("users/ayende");
    user2.setName("Rahien");

    runNotModifiedTestsForUrl(user1, user2, getDefaultUrl() + "/docs/");
  }

  @Test
  public void serverReturnsNotModifiedWhenAppropriateForDatabases() throws Exception {
    RavenJObject obj1 = new RavenJObject();
    obj1.add("Id", "Raven/Databases/FirstDatabase");

    RavenJObject obj2 = new RavenJObject();
    obj2.add("Id", "Raven/Databases/SecondDatabase");

    runNotModifiedTestsForUrl(obj1, obj2, getDefaultUrl() + "/databases/");

  }

  private void runNotModifiedTestsForUrl(Object firstItemToStore, Object secondItemToStore, String url) throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl()).initialize()) {

      // store an item
      Etag firstEtag;
      try (IDocumentSession session = store.openSession()) {
        session.store(firstItemToStore);
        session.saveChanges();
      }
      // Here, we should get the same etag we got when we asked the session
      try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

        HttpGet get = new HttpGet(url);
        HttpResponse response = httpClient.execute(get);

        assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        firstEtag = HttpExtensions.getEtagHeader(response);
        EntityUtils.consumeQuietly(response.getEntity());

        // If we ask with If-None-Match (and it's a match), we'll get 304 Not Modified
        get = new HttpGet(url);
        get.addHeader("If-None-Match", firstEtag.toString());
        response = httpClient.execute(get);
        assertEquals(HttpStatus.SC_NOT_MODIFIED, response.getStatusLine().getStatusCode());
        EntityUtils.consumeQuietly(response.getEntity());

        // Change the item or add a second item
        Etag secondEtag;
        try (IDocumentSession session = store.openSession()) {
          session.store(secondItemToStore);
          session.saveChanges();
        }

        // If we ask with the old etag, we'll get a new result
        get = new HttpGet(url);
        response = httpClient.execute(get);

        assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        secondEtag = HttpExtensions.getEtagHeader(response);
        EntityUtils.consumeQuietly(response.getEntity());


        // If we ask with the new etag, we'll get 304 Not Modified
        get = new HttpGet(url);
        get.addHeader("If-None-Match", secondEtag.toString());
        response = httpClient.execute(get);
        assertEquals(HttpStatus.SC_NOT_MODIFIED, response.getStatusLine().getStatusCode());
        EntityUtils.consumeQuietly(response.getEntity());
      }
    }
  }
}
