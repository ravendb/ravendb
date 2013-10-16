package net.ravendb.client.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import net.ravendb.abstractions.basic.EventHandler;
import net.ravendb.abstractions.closure.Functions;
import net.ravendb.abstractions.connection.WebRequestEventArgs;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.exceptions.HttpOperationException;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.RavenDBAwareTests;
import net.ravendb.client.connection.CreateHttpJsonRequestParams;
import net.ravendb.client.connection.ReplicationInformer;
import net.ravendb.client.connection.ServerClient;
import net.ravendb.client.connection.implementation.HttpJsonRequest;
import net.ravendb.client.connection.implementation.HttpJsonRequestFactory;
import net.ravendb.client.connection.profiling.RequestResultArgs;
import net.ravendb.client.connection.profiling.RequestStatus;
import net.ravendb.client.document.DocumentConvention;
import net.ravendb.client.document.FailoverBehavior;
import net.ravendb.client.document.FailoverBehaviorSet;
import net.ravendb.client.listeners.IDocumentConflictListener;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;


public class HttpJsonRequestTest extends RavenDBAwareTests {

  private List<HttpRequest> requestQueue = new ArrayList<>();
  private List<RequestResultArgs> requestResultArgs = new ArrayList<>();

  @Override
  @Before
  public void init() {
    System.setProperty("java.net.preferIPv4Stack" , "true");
    convention = new DocumentConvention();
    convention.setFailoverBehavior(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY));
    factory = new HttpJsonRequestFactory(10);

    replicationInformer = new ReplicationInformer(convention);

    serverClient = new ServerClient(DEFAULT_SERVER_URL_1, convention,
      new Functions.StaticFunction1<String, ReplicationInformer>(replicationInformer), null, factory,
      UUID.randomUUID(), new IDocumentConflictListener[0]);
    initFactory();
  }


  private void initFactory() {
    factory.addConfigureRequestEventHandler(new EventHandler<WebRequestEventArgs>() {
      @Override
      public void handle(Object sender, WebRequestEventArgs event) {
        requestQueue.add(event.getRequest());
      }
    });

    factory.addLogRequestEventHandler(new EventHandler<RequestResultArgs>() {
      @Override
      public void handle(Object sender, RequestResultArgs event) {
        requestResultArgs.add(event);
      }
    });

  }

  @Test
  public void testAttachment() throws Exception {

    Map<String, String> operationHeaders = new HashMap<>();

    try (InputStream ravenImage = this.getClass().getResourceAsStream("/raven.png")){
      assertNotNull(ravenImage);
      createDb();

      RavenJObject metadata = new RavenJObject();
      metadata.add("Content-Type", RavenJToken.fromObject("image/png"));

      HttpJsonRequest jsonRequest = factory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/static/images/ravendb.png", HttpMethods.PUT,
        metadata, convention).addOperationHeaders(operationHeaders));

      jsonRequest.write(ravenImage);
      jsonRequest.executeRequest();

      // and now verify attachment using plain client.

      HttpGet get = new HttpGet(DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/static/images/ravendb.png");
      HttpClient httpClient = factory.getHttpClient();
      HttpResponse httpResponse = httpClient.execute(get);
      byte[] imageBytes = IOUtils.toByteArray(httpResponse.getEntity().getContent());
      assertEquals(7642, imageBytes.length);

      EntityUtils.consumeQuietly(httpResponse.getEntity());

    } finally {
      deleteDb();
    }
  }


  @Test
  public void testTryGetNotExistingDocument() throws Exception {
    Map<String, String> operationHeaders = new HashMap<>();

    try {
      createDb();

      HttpJsonRequest jsonRequest = factory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/10", HttpMethods.GET,
        new RavenJObject(), convention).addOperationHeaders(operationHeaders));

      try {
        jsonRequest.readResponseJson();
        fail("readResponse should throw an exception");
      } catch (HttpOperationException e) {
        assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
      }

    } finally {
      deleteDb();
    }

  }

  @Test
  public void testReadResponseBytes() throws Exception {
    try {
      createDb();
      putSampleDocument(new HashMap<String, String>());

      // repeat test to check connection management
      for (int i = 0;i < 10; i++) {

        HttpJsonRequest jsonRequest = factory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/1", HttpMethods.GET,
          new RavenJObject() , convention).addOperationHeaders(new HashMap<String, String>()));

        byte[] responseBytes = jsonRequest.readResponseBytes();
        assertNotNull(responseBytes);
        RavenJToken jToken = RavenJObject.tryLoad(new ByteArrayInputStream(responseBytes));
        assertEquals(jToken.toString(), "John", jToken.value(String.class, "FirstName"));
      }

    } finally {
      deleteDb();
    }
  }

  @Test
  public void testDelete() throws Exception {
    try {
      createDb();
      RavenJToken putResult = putSampleDocument(new HashMap<String, String>());
      Etag etag = putResult.value(Etag.class, "ETag");
      assertNotNull(etag);

      RavenJObject ravenJObject = new RavenJObject();
      ravenJObject.add("ETag", RavenJToken.fromObject(UUID.randomUUID()));

      Map<String, String> operationHeaders = new HashMap<>();

      requestQueue.clear();
      requestResultArgs.clear();

      // try with invalid ETag
      HttpJsonRequest jsonRequest = factory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/1", HttpMethods.DELETE,
        ravenJObject , convention).addOperationHeaders(operationHeaders));
      try {
        jsonRequest.executeRequest();
        fail("request should throw");
      } catch (HttpOperationException e) {
        try {

          assertEquals(HttpStatus.SC_CONFLICT, requestResultArgs.get(0).getHttpResult());
        } finally {
          EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
        }
      }

      // now retry with valid ETag
      ravenJObject = new RavenJObject();
      ravenJObject.add("ETag", RavenJToken.fromObject(etag));

      requestQueue.clear();
      requestResultArgs.clear();

      // try with invalid ETag
      jsonRequest = factory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/1", HttpMethods.DELETE,
        ravenJObject , convention).addOperationHeaders(operationHeaders));
      jsonRequest.executeRequest();
      assertEquals(HttpStatus.SC_NO_CONTENT, requestResultArgs.get(0).getHttpResult());


    } finally {
      deleteDb();
    }
  }

  @Test
  public void testAggresiveCache() throws Exception {
    Map<String, String> operationHeaders = new HashMap<>();

    try {
      createDb();

      putSampleDocument(operationHeaders);

      getSampleDocument(operationHeaders);

      factory.setAggressiveCacheDuration(1000l);

      requestResultArgs.clear();
      requestQueue.clear();

      getSampleDocument(operationHeaders);

      assertEquals(1, requestQueue.size());
      assertEquals(1, requestResultArgs.size());
      RequestResultArgs resultArgs = requestResultArgs.get(0);
      requestResultArgs.clear();

      assertEquals(RequestStatus.AGGRESSIVELY_CACHED, resultArgs.getStatus());

    } finally {
      deleteDb();
    }

  }

  private void getSampleDocument(Map<String, String> operationHeaders) throws Exception {
    HttpJsonRequest jsonRequest = factory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/1", HttpMethods.GET,
      new RavenJObject(), convention).addOperationHeaders(operationHeaders));
    RavenJToken responseJson = jsonRequest.readResponseJson();
    assertNotNull(responseJson);
  }


  @Test(expected = IllegalStateException.class)
  public void testBadRequest() {
    HttpJsonRequest jsonRequest = factory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/admin/noSuchEndpoint", HttpMethods.GET,
      new RavenJObject() , convention).addOperationHeaders(new HashMap<String, String>()));
    jsonRequest.readResponseJson();
  }

  @Test
  public void testPutAndCache() throws Exception {

    Map<String, String> operationHeaders = new HashMap<>();

    try {
      createDb();

      /*
       * put sample document
       */
      RavenJToken putResult = putSampleDocument(operationHeaders);

      Map<String, String> expectedRequestHeaders = new HashMap<>();
      expectedRequestHeaders.put("Accept-Encoding", "gzip,deflate");
      expectedRequestHeaders.put("Content-Encoding", "gzip");
      verifyRequestHeaders(expectedRequestHeaders, true);

      assertEquals(1, requestResultArgs.size());
      RequestResultArgs resultArgs = requestResultArgs.get(0);
      requestResultArgs.clear();

      assertEquals(RequestStatus.SEND_TO_SERVER, resultArgs.getStatus());

      /*
       * Get inserted document
       */

      getSampleDocument(new HashMap<String, String>());

      expectedRequestHeaders = new HashMap<>();
      expectedRequestHeaders.put("Accept-Encoding", "gzip,deflate");
      verifyRequestHeaders(expectedRequestHeaders, true);

      String etag = putResult.value(String.class, "ETag");

      assertEquals(1, requestResultArgs.size());
      resultArgs = requestResultArgs.get(0);
      requestResultArgs.clear();

      assertEquals(RequestStatus.SEND_TO_SERVER, resultArgs.getStatus());

      getSampleDocument(new HashMap<String, String>());

      expectedRequestHeaders = new HashMap<>();
      expectedRequestHeaders.put("Accept-Encoding", "gzip,deflate");
      expectedRequestHeaders.put("If-None-Match", "\"" +  etag + "\"");
      verifyRequestHeaders(expectedRequestHeaders, true);


      assertEquals(0, requestResultArgs.get(0).getHttpResult());

      assertEquals(1, requestResultArgs.size());
      resultArgs = requestResultArgs.get(0);
      requestResultArgs.clear();

      assertEquals(RequestStatus.CACHED, resultArgs.getStatus());

      /*
       * Get object in non-cache scope
       */

      try (AutoCloseable closable = factory.disableAllCaching()) {
        getSampleDocument(new HashMap<String, String>());

        requestQueue.clear();

        assertEquals(1, requestResultArgs.size());
        resultArgs = requestResultArgs.get(0);
        requestResultArgs.clear();

        assertEquals(RequestStatus.SEND_TO_SERVER, resultArgs.getStatus());
      }

    } finally {
      deleteDb();
    }

  }

  @Test
  public void testPutWithOutGzip() throws Exception {

    Map<String, String> operationHeaders = new HashMap<>();
    factory.setDisableRequestCompression(true);

    try {
      createDb();

      /*
       * put sample document
       */
      RavenJToken putResult = putSampleDocument(operationHeaders);

      Map<String, String> expectedRequestHeaders = new HashMap<>();

      Map<String, String> requestHeaders = HttpJsonRequest.extractHeaders(requestQueue.get(0).getAllHeaders());
      assertTrue("Accept-Encoding must not be present! Headers: " + requestHeaders, !requestHeaders.containsKey("Accept-Encoding"));
      assertTrue("Content-Encoding must not be present! Headers: " + requestHeaders, !requestHeaders.containsKey("Content-Encoding"));

      verifyRequestHeaders(expectedRequestHeaders, true);

      assertEquals(1, requestResultArgs.size());
      RequestResultArgs resultArgs = requestResultArgs.get(0);
      requestResultArgs.clear();

      assertEquals(RequestStatus.SEND_TO_SERVER, resultArgs.getStatus());

      /*
       * Get inserted document
       */

      getSampleDocument(new HashMap<String, String>());

      expectedRequestHeaders = new HashMap<>();
      verifyRequestHeaders(expectedRequestHeaders, true);

      String etag = putResult.value(String.class, "ETag");

      assertEquals(1, requestResultArgs.size());
      resultArgs = requestResultArgs.get(0);
      requestResultArgs.clear();

      assertEquals(RequestStatus.SEND_TO_SERVER, resultArgs.getStatus());

      getSampleDocument(new HashMap<String, String>());

      expectedRequestHeaders = new HashMap<>();
      expectedRequestHeaders.put("If-None-Match", "\"" + etag + "\"");
      verifyRequestHeaders(expectedRequestHeaders, true);



    } finally {
      deleteDb();
      factory.setDisableRequestCompression(false);
    }

  }

  private RavenJToken putSampleDocument(Map<String, String> operationHeaders) throws Exception {
    HttpJsonRequest jsonRequest = factory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/1", HttpMethods.PUT,
      new RavenJObject(), convention).addOperationHeaders(operationHeaders));
    Person person = new Person("5", "John", "Smith");

    jsonRequest.write(RavenJObject.fromObject(person).toString());
    RavenJToken responseJson = jsonRequest.readResponseJson();
    assertNotNull(responseJson);
    return responseJson;
  }

  private void verifyRequestHeaders(Map<String, String> expectedRequestHeaders, boolean drop) {
    assertTrue(requestQueue.size() > 0);
    HttpRequest httpRequest = requestQueue.get(0);
    if (drop) {
      assertEquals(1, requestQueue.size());
      requestQueue.clear();
    }

    Map<String, String> extractedHeaders = HttpJsonRequest.extractHeaders(httpRequest.getAllHeaders());
    for (Entry<String, String> entry : expectedRequestHeaders.entrySet()) {
      assertTrue("Expected header: " + entry.getKey() + ". Available headers: " + extractedHeaders.keySet(), extractedHeaders.containsKey(entry.getKey()));
      assertEquals(entry.getValue(), extractedHeaders.get(entry.getKey()));
    }

  }
}
