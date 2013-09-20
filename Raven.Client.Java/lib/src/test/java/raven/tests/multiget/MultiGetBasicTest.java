package raven.tests.multiget;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;
import raven.abstractions.extensions.JsonExtensions;
import raven.client.IDocumentSession;
import raven.client.IDocumentStore;
import raven.client.RemoteClientTest;
import raven.client.document.DocumentQueryCustomizationFactory;
import raven.client.document.DocumentStore;
import raven.tests.bugs.QUser;
import raven.tests.bugs.User;

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

      HttpClient httpClient = new DefaultHttpClient();

      HttpPost post = new HttpPost(getDefaultUrl() + "/multi_get");

      String requestString = JsonExtensions.getDefaultObjectMapper().writeValueAsString(new GetRequest[] {
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

      HttpClient httpClient = new DefaultHttpClient();

      HttpPost post = new HttpPost(getDefaultUrl() + "/multi_get");

      String requestString = JsonExtensions.getDefaultObjectMapper().writeValueAsString(new GetRequest[] {
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

      HttpClient httpClient = new DefaultHttpClient();

      HttpPost post = new HttpPost(getDefaultUrl() + "/multi_get");

      String requestString = JsonExtensions.getDefaultObjectMapper().writeValueAsString(new GetRequest[] {
          new GetRequest("/docs/users/1"),
          new GetRequest("/docs/users/2")
      });
      post.setEntity(new StringEntity(requestString, ContentType.APPLICATION_JSON));

      HttpResponse httpResponse = httpClient.execute(post);
      String response = IOUtils.toString(httpResponse.getEntity().getContent());
      EntityUtils.consumeQuietly(httpResponse.getEntity());

      GetResponse[] results = JsonExtensions.getDefaultObjectMapper().readValue(response, GetResponse[].class);
      assertTrue(results[0].getHeaders().containsKey("ETag"));
      assertTrue(results[1].getHeaders().containsKey("ETag"));

      post = new HttpPost(getDefaultUrl() + "/multi_get");

      GetRequest get1 = new GetRequest("/docs/users/1");
      get1.getHeaders().put("If-None-Match", results[0].getHeaders().get("ETag"));
      GetRequest get2 = new GetRequest("/docs/users/2");
      get2.getHeaders().put("If-None-Match", results[1].getHeaders().get("ETag"));


      requestString = JsonExtensions.getDefaultObjectMapper().writeValueAsString(new GetRequest[] {
          get1, get2
      });
      post.setEntity(new StringEntity(requestString, ContentType.APPLICATION_JSON));


      httpResponse = httpClient.execute(post);
      response = IOUtils.toString(httpResponse.getEntity().getContent());
      EntityUtils.consumeQuietly(httpResponse.getEntity());

      results = JsonExtensions.getDefaultObjectMapper().readValue(response, GetResponse[].class);
      assertEquals(304, results[0].getStatus());
      assertEquals(304, results[1].getStatus());

    }
  }
}
