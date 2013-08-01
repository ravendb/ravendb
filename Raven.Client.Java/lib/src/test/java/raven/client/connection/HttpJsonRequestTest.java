package raven.client.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import raven.abstractions.basic.EventHandler;
import raven.abstractions.connection.profiling.RequestResultArgs;
import raven.abstractions.data.Etag;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.exceptions.HttpOperationException;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.RavenDBAwareTests;
import raven.client.connection.implementation.HttpJsonRequest;
import raven.client.connection.implementation.HttpJsonRequestFactory;
import raven.client.connection.profiling.RequestStatus;
import raven.client.document.DocumentConvention;

public class HttpJsonRequestTest extends RavenDBAwareTests {

  private HttpJsonRequestFactory jsonRequestFactory;
  private List<HttpRequest> requestQueue = new ArrayList<>();
  private List<HttpResponse> responseQueue = new ArrayList<>();
  private List<RequestResultArgs> requestResultArgs = new ArrayList<>();
  private DocumentConvention convention = new DocumentConvention();

  public HttpJsonRequestTest() {
    initFactory();
  }

  private void initFactory() {
    jsonRequestFactory = new HttpJsonRequestFactory(10);
    DefaultHttpClient httpClient = (DefaultHttpClient) jsonRequestFactory.getHttpClient();
    httpClient.addRequestInterceptor(new HttpRequestInterceptor() {
      @Override
      public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
        requestQueue.add(request);
      }
    });

    httpClient.addResponseInterceptor(new HttpResponseInterceptor() {
      @Override
      public void process(HttpResponse response, HttpContext context) throws HttpException, IOException {
        responseQueue.add(response);
      }
    });

    jsonRequestFactory.addLogRequestEventHandler(new EventHandler<RequestResultArgs>() {
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

      HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/static/images/ravendb.png", HttpMethods.PUT,
          metadata, null, convention).addOperationHeaders(operationHeaders));

      jsonRequest.write(ravenImage);
      jsonRequest.executeRequest();

      // and now verify attachment using plain client.

      HttpGet get = new HttpGet(DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/static/images/ravendb.png");
      HttpClient httpClient = jsonRequestFactory.getHttpClient();
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

      HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/10", HttpMethods.GET,
          new RavenJObject(), null, convention).addOperationHeaders(operationHeaders));

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

        HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/1", HttpMethods.GET,
            new RavenJObject() , null, convention).addOperationHeaders(new HashMap<String, String>()));

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

      responseQueue.clear();
      requestQueue.clear();
      requestResultArgs.clear();

      // try with invalid ETag
      HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/1", HttpMethods.DELETE,
          ravenJObject , null, convention).addOperationHeaders(operationHeaders));
      try {
        jsonRequest.executeRequest();
        fail("request should throw");
      } catch (HttpOperationException e) {
        try {
          assertEquals(HttpStatus.SC_CONFLICT, responseQueue.get(0).getStatusLine().getStatusCode());

          RavenJToken conflictObject = RavenJObject.tryLoad(responseQueue.get(0).getEntity().getContent());
          assertNotNull(conflictObject);
        } finally {
          EntityUtils.consumeQuietly(e.getHttpResponse().getEntity());
        }
      }

      // now retry with valid ETag
      ravenJObject = new RavenJObject();
      ravenJObject.add("ETag", RavenJToken.fromObject(etag));

      responseQueue.clear();
      requestQueue.clear();
      requestResultArgs.clear();

      // try with invalid ETag
      jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/1", HttpMethods.DELETE,
          ravenJObject , null, convention).addOperationHeaders(operationHeaders));
      jsonRequest.executeRequest();
      assertEquals(HttpStatus.SC_NO_CONTENT, responseQueue.get(0).getStatusLine().getStatusCode());


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

      jsonRequestFactory.setAggressiveCacheDuration(1000l);

      requestResultArgs.clear();
      requestQueue.clear();

      getSampleDocument(operationHeaders);

      assertEquals(0, requestQueue.size());
      assertEquals(1, requestResultArgs.size());
      RequestResultArgs resultArgs = requestResultArgs.get(0);
      requestResultArgs.clear();

      assertEquals(RequestStatus.AGGRESSIVELY_CACHED, resultArgs.getStatus());

    } finally {
      deleteDb();
    }

  }

  private void getSampleDocument(Map<String, String> operationHeaders) throws IOException, Exception {
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/1", HttpMethods.GET,
        new RavenJObject(), null, convention).addOperationHeaders(operationHeaders));
    RavenJToken responseJson = jsonRequest.readResponseJson();
    assertNotNull(responseJson);
  }


  @Test(expected = IllegalStateException.class)
  public void testBadRequest() throws IOException {
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/admin/noSuchEndpoint", HttpMethods.GET,
        new RavenJObject() , null, convention).addOperationHeaders(new HashMap<String, String>()));
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
      putSampleDocument(operationHeaders);

      Map<String, String> expectedRequestHeaders = new HashMap<>();
      expectedRequestHeaders.put("Accept-Encoding", "gzip,deflate");
      expectedRequestHeaders.put("Content-Type", "application/json; charset=UTF-8");
      expectedRequestHeaders.put("Transfer-Encoding", "chunked");
      expectedRequestHeaders.put("Content-Encoding", "gzip");
      verifyRequestHeaders(expectedRequestHeaders, true);

      Map<String, String> expectedResponseHeaders = new HashMap<>();
      expectedResponseHeaders.put("Transfer-Encoding", "chunked");
      expectedResponseHeaders.put("Content-Type", "application/json; charset=utf-8");
      // content encoding is returned by removed by DecompressingHttpClient expectedResponseHeaders.put("Content-Encoding", "gzip");
      expectedResponseHeaders.put("Location", "/docs/persons/1");
      verifyResponseHeaders(expectedResponseHeaders, true);


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

      String etag = responseQueue.get(0).getFirstHeader("ETag").getValue();

      expectedResponseHeaders = new HashMap<>();
      expectedResponseHeaders.put("Transfer-Encoding", "chunked");
      expectedResponseHeaders.put("Content-Type", "application/json; charset=utf-8");
      // content encoding is returned by removed by DecompressingHttpClient expectedResponseHeaders.put("Content-Encoding", "gzip");
      expectedResponseHeaders.put("__document_id", "persons/1");
      verifyResponseHeaders(expectedResponseHeaders, true);

      assertEquals(1, requestResultArgs.size());
      resultArgs = requestResultArgs.get(0);
      requestResultArgs.clear();

      assertEquals(RequestStatus.SEND_TO_SERVER, resultArgs.getStatus());

      getSampleDocument(new HashMap<String, String>());

      expectedRequestHeaders = new HashMap<>();
      expectedRequestHeaders.put("Accept-Encoding", "gzip,deflate");
      expectedRequestHeaders.put("If-None-Match", etag);
      verifyRequestHeaders(expectedRequestHeaders, true);


      assertEquals(HttpStatus.SC_NOT_MODIFIED, responseQueue.get(0).getStatusLine().getStatusCode());
      expectedResponseHeaders = new HashMap<>();
      expectedResponseHeaders.put("Content-Type", "application/json; charset=utf-8");
      verifyResponseHeaders(expectedResponseHeaders, true);

      assertEquals(1, requestResultArgs.size());
      resultArgs = requestResultArgs.get(0);
      requestResultArgs.clear();

      assertEquals(RequestStatus.CACHED, resultArgs.getStatus());

      /*
       * Get object in non-cache scope
       */

      try (AutoCloseable closable = jsonRequestFactory.disableAllCaching()) {
        getSampleDocument(new HashMap<String, String>());

        requestQueue.clear();
        responseQueue.clear();

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
    jsonRequestFactory.setDisableRequestCompression(true);

    try {
      createDb();

      /*
       * put sample document
       */
      putSampleDocument(operationHeaders);

      Map<String, String> expectedRequestHeaders = new HashMap<>();
      expectedRequestHeaders.put("Content-Type", "application/json; charset=UTF-8");
      expectedRequestHeaders.put("Transfer-Encoding", "chunked");

      Map<String, String> requestHeaders = HttpJsonRequest.extractHeaders(requestQueue.get(0).getAllHeaders());
      assertTrue("Accept-Encoding must not be present! Headers: " + requestHeaders, !requestHeaders.containsKey("Accept-Encoding"));
      assertTrue("Content-Encoding must not be present! Headers: " + requestHeaders, !requestHeaders.containsKey("Content-Encoding"));

      verifyRequestHeaders(expectedRequestHeaders, true);

      Map<String, String> expectedResponseHeaders = new HashMap<>();
      expectedResponseHeaders.put("Transfer-Encoding", "chunked");
      expectedResponseHeaders.put("Content-Type", "application/json; charset=utf-8");
      // content encoding is returned by removed by DecompressingHttpClient expectedResponseHeaders.put("Content-Encoding", "gzip");
      expectedResponseHeaders.put("Location", "/docs/persons/1");
      verifyResponseHeaders(expectedResponseHeaders, true);


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

      String etag = responseQueue.get(0).getFirstHeader("ETag").getValue();

      expectedResponseHeaders = new HashMap<>();
      expectedResponseHeaders.put("Transfer-Encoding", "chunked");
      expectedResponseHeaders.put("Content-Type", "application/json; charset=utf-8");
      // content encoding is returned by removed by DecompressingHttpClient expectedResponseHeaders.put("Content-Encoding", "gzip");
      expectedResponseHeaders.put("__document_id", "persons/1");
      verifyResponseHeaders(expectedResponseHeaders, true);

      assertEquals(1, requestResultArgs.size());
      resultArgs = requestResultArgs.get(0);
      requestResultArgs.clear();

      assertEquals(RequestStatus.SEND_TO_SERVER, resultArgs.getStatus());

      getSampleDocument(new HashMap<String, String>());

      expectedRequestHeaders = new HashMap<>();
      expectedRequestHeaders.put("If-None-Match", etag);
      verifyRequestHeaders(expectedRequestHeaders, true);



    } finally {
      deleteDb();
      jsonRequestFactory.setDisableRequestCompression(false);
    }

  }

  private RavenJToken putSampleDocument(Map<String, String> operationHeaders) throws UnsupportedEncodingException, IOException, Exception {
    HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL_1 + "/databases/" + getDbName() + "/docs/persons/1", HttpMethods.PUT,
        new RavenJObject(), null, convention).addOperationHeaders(operationHeaders));
    Person person = new Person("5", "John", "Smith");

    jsonRequest.write(RavenJObject.fromObject(person).toString());
    RavenJToken responseJson = jsonRequest.readResponseJson();
    assertNotNull(responseJson);
    return responseJson;
  }

  private void verifyResponseHeaders(Map<String, String> expectedResponseHeaders, boolean drop) {
    assertTrue(responseQueue.size() > 0);
    HttpResponse httpResponse = responseQueue.get(0);
    if (drop) {
      assertEquals(1, responseQueue.size());
      responseQueue.clear();
    }

    Map<String, String> extractedHeaders = HttpJsonRequest.extractHeaders(httpResponse.getAllHeaders());
    for (Entry<String, String> entry : expectedResponseHeaders.entrySet()) {
      assertTrue("Expected header: " + entry.getKey() + ". Available headers: " + extractedHeaders.keySet(), extractedHeaders.containsKey(entry.getKey()));
      assertEquals(entry.getValue(), extractedHeaders.get(entry.getKey()));
    }

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
