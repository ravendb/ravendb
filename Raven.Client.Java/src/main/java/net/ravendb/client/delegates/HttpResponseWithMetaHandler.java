package net.ravendb.client.delegates;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.connection.OperationCredentials;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;



public interface HttpResponseWithMetaHandler {
  public Action1<HttpRequest> handle(HttpResponse httpResponse, OperationCredentials credentials);
}
