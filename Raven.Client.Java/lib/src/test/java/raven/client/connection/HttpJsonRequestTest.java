package raven.client.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.endsWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.junit.Test;

import raven.abstractions.basic.EventHandler;
import raven.abstractions.connection.profiling.RequestResultArgs;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.exceptions.HttpOperationException;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.RavenDBAwareTests;
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
  public void testTryGetNotExistingDocument() throws Exception {
    Map<String, String> operationHeaders = new HashMap<>();

    try {
      createDb("db1");

      try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL + "/databases/db1/docs/persons/10", HttpMethods.GET,
          new RavenJObject(), null, convention).addOperationHeaders(operationHeaders))) {

        jsonRequest.readResponseJson();
        fail("readResponse should throw an exception");
      } catch (HttpOperationException e) {
        assertEquals(HttpStatus.SC_NOT_FOUND, e.getStatusCode());
      }

    } finally {
      deleteDb("db1");
    }

  }

  @Test
  public void testPutAndCache() throws Exception {

    Map<String, String> operationHeaders = new HashMap<>();

    try {
      createDb("db1");

      /*
       * put sample document
       */
      try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL + "/databases/db1/docs/persons/1", HttpMethods.PUT,
          new RavenJObject(), null, convention).addOperationHeaders(operationHeaders))) {

        Person person = new Person("5", "John", "Smith");

        jsonRequest.write(RavenJObject.fromObject(person).toString());
        RavenJToken responseJson = jsonRequest.readResponseJson();
        assertNotNull(responseJson);
      }

      Map<String, String> expectedRequestHeaders = new HashMap<>();
      expectedRequestHeaders.put("Accept-Encoding", "gzip,deflate");
      expectedRequestHeaders.put("Content-Type", "application/json; charset=UTF-8");
      expectedRequestHeaders.put("Transfer-Encoding", "chunked");
      expectedRequestHeaders.put("Content-Encoding", "gzip");
      expectedRequestHeaders.put("Expect", "100-continue");
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

      try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL + "/databases/db1/docs/persons/1", HttpMethods.GET,
          new RavenJObject(), null, convention).addOperationHeaders(operationHeaders))) {

        RavenJToken responseJson = jsonRequest.readResponseJson();
        assertNotNull(responseJson);
      }

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

      /*
       * Get inserted document (should go from cache)
       */
      try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL + "/databases/db1/docs/persons/1", HttpMethods.GET,
          new RavenJObject(), null, convention).addOperationHeaders(operationHeaders))) {

        RavenJToken responseJson = jsonRequest.readResponseJson();
        assertNotNull(responseJson);
      }

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
        try (HttpJsonRequest jsonRequest = jsonRequestFactory.createHttpJsonRequest(new CreateHttpJsonRequestParams(null, DEFAULT_SERVER_URL + "/databases/db1/docs/persons/1", HttpMethods.GET,
            new RavenJObject(), null, convention).addOperationHeaders(operationHeaders))) {

          RavenJToken responseJson = jsonRequest.readResponseJson();
          assertNotNull(responseJson);
        }

        requestQueue.clear();
        responseQueue.clear();

        assertEquals(1, requestResultArgs.size());
        resultArgs = requestResultArgs.get(0);
        requestResultArgs.clear();

        assertEquals(RequestStatus.SEND_TO_SERVER, resultArgs.getStatus());
      }

    } finally {
      deleteDb("db1");
    }

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
