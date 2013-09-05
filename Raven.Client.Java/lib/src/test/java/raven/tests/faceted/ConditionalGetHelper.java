package raven.tests.faceted;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import raven.abstractions.basic.Reference;
import raven.abstractions.data.Etag;
import raven.client.connection.HttpExtensions;

public class ConditionalGetHelper {
  private HttpClient httpClient;

  public ConditionalGetHelper() {
    httpClient = new DefaultHttpClient();
  }

  private HttpResponse getHttpResponseHandle304(HttpRequestBase request) throws ClientProtocolException, IOException {
    return httpClient.execute(request);
  }


  public int performGet(String url, Etag requestEtag, Reference<Etag> responseEtag) throws ClientProtocolException, IOException {
    HttpGet getRequest = new HttpGet(url);

    if (requestEtag != null) {
      getRequest.setHeader("If-None-Match", requestEtag.toString());
    }
    HttpResponse response = getHttpResponseHandle304(getRequest);

    try {
      responseEtag.value = HttpExtensions.getEtagHeader(response);
    } catch (Exception e) {
      responseEtag.value = null;
    }

    EntityUtils.consumeQuietly(response.getEntity());

    return response.getStatusLine().getStatusCode();
  }

  public int performPost(String url, String payload, Etag requestEtag, Reference<Etag> responseEtag) throws IOException {
    HttpPost request = new HttpPost(url);
    if (requestEtag != null) {
      request.setHeader("If-None-Match", requestEtag.toString());
    }

    request.setEntity(new StringEntity(payload));
    HttpResponse response = getHttpResponseHandle304(request);

    try {
      responseEtag.value = HttpExtensions.getEtagHeader(response);
    } catch (Exception e) {
      responseEtag.value = null;
    }

    EntityUtils.consumeQuietly(response.getEntity());

    return response.getStatusLine().getStatusCode();
  }
}
