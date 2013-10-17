package net.ravendb.tests.multiget;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;
import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentQueryCustomizationFactory;
import net.ravendb.client.document.DocumentStore;
import net.ravendb.tests.bugs.QUser;
import net.ravendb.tests.bugs.User;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;


public class MultiGetBasicTest extends RemoteClientTest {

  @Test
  public void canUseMultiGetToBatchGetDocumentRequests() throws Exception {

    try (IDocumentStore store = new DocumentStore(getDefaultUrl()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("Ayende");
        session.store(user1);

        User user2 = new User();
        user2.setName("Oren");
        session.store(user2);

        session.saveChanges();
      }

      try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

        HttpPost post = new HttpPost(getDefaultUrl() + "/multi_get");

        String requestString = JsonExtensions.createDefaultJsonSerializer().writeValueAsString(new GetRequest[] {
          new GetRequest("/docs/users/1"),
          new GetRequest("/docs/users/2")
        });
        post.setEntity(new StringEntity(requestString, ContentType.APPLICATION_JSON));

        HttpResponse httpResponse = httpClient.execute(post);
        String response = IOUtils.toString(httpResponse.getEntity().getContent());
        EntityUtils.consumeQuietly(httpResponse.getEntity());

        assertTrue(response, response.contains("Ayende"));
        assertTrue(response, response.contains("Oren"));
      }

    }
  }

  @Test
  public void canUseMultiQuery() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("Ayende");
        session.store(user1);

        User user2 = new User();
        user2.setName("Oren");
        session.store(user2);

        session.saveChanges();
      }
      try (IDocumentSession session = store.openSession())  {
        QUser u = QUser.user;
        session.query(User.class).customize(new DocumentQueryCustomizationFactory().waitForNonStaleResults())
        .where(u.name.eq("Ayende")).toList();
      }

      try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

        HttpPost post = new HttpPost(getDefaultUrl() + "/multi_get");

        String requestString = JsonExtensions.createDefaultJsonSerializer().writeValueAsString(new GetRequest[] {
          new GetRequest("/indexes/dynamic/Users", "query=Name:Ayende"),
          new GetRequest("/indexes/dynamic/Users", "query=Name:Oren")
        });
        post.setEntity(new StringEntity(requestString, ContentType.APPLICATION_JSON));

        HttpResponse httpResponse = httpClient.execute(post);
        String response = IOUtils.toString(httpResponse.getEntity().getContent());
        EntityUtils.consumeQuietly(httpResponse.getEntity());

        assertTrue(response, response.contains("Ayende"));
        assertTrue(response, response.contains("Oren"));
      }

    }
  }

  @Test
  public void canHandleCaching() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl()).initialize()) {
      try (IDocumentSession session = store.openSession()) {
        User user1 = new User();
        user1.setName("Ayende");
        session.store(user1);

        User user2 = new User();
        user2.setName("Oren");
        session.store(user2);

        session.saveChanges();
      }

      try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

        HttpPost post = new HttpPost(getDefaultUrl() + "/multi_get");

        String requestString = JsonExtensions.createDefaultJsonSerializer().writeValueAsString(new GetRequest[] {
          new GetRequest("/docs/users/1"),
          new GetRequest("/docs/users/2")
        });
        post.setEntity(new StringEntity(requestString, ContentType.APPLICATION_JSON));

        HttpResponse httpResponse = httpClient.execute(post);
        String response = IOUtils.toString(httpResponse.getEntity().getContent());
        EntityUtils.consumeQuietly(httpResponse.getEntity());

        GetResponse[] results = JsonExtensions.createDefaultJsonSerializer().readValue(response, GetResponse[].class);
        assertTrue(results[0].getHeaders().containsKey("ETag"));
        assertTrue(results[1].getHeaders().containsKey("ETag"));

        post = new HttpPost(getDefaultUrl() + "/multi_get");

        GetRequest get1 = new GetRequest("/docs/users/1");
        get1.getHeaders().put("If-None-Match", results[0].getHeaders().get("ETag"));
        GetRequest get2 = new GetRequest("/docs/users/2");
        get2.getHeaders().put("If-None-Match", results[1].getHeaders().get("ETag"));


        requestString = JsonExtensions.createDefaultJsonSerializer().writeValueAsString(new GetRequest[] {
          get1, get2
        });
        post.setEntity(new StringEntity(requestString, ContentType.APPLICATION_JSON));


        httpResponse = httpClient.execute(post);
        response = IOUtils.toString(httpResponse.getEntity().getContent());
        EntityUtils.consumeQuietly(httpResponse.getEntity());

        results = JsonExtensions.createDefaultJsonSerializer().readValue(response, GetResponse[].class);
        assertEquals(304, results[0].getStatus());
        assertEquals(304, results[1].getStatus());
      }
    }
  }
}
