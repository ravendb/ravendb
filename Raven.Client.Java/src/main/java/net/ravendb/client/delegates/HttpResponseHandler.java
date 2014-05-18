package net.ravendb.client.delegates;

import net.ravendb.abstractions.closure.Action1;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;



public interface HttpResponseHandler {
  public Action1<HttpRequest> handle(HttpResponse httpResponse);
}
