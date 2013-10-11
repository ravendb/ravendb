package raven.client.delegates;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;

import raven.abstractions.closure.Action1;


public interface HttpResponseHandler {
  public Action1<HttpRequest> handle(HttpResponse httpResponse);
}
