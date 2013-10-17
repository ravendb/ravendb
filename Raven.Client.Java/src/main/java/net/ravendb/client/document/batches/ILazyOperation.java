package net.ravendb.client.document.batches;

import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;

public interface ILazyOperation {
  public GetRequest createRequest();
  public Object getResult();
  public boolean isRequiresRetry();
  public void handleResponse(GetResponse response);
  public AutoCloseable enterContext();

}
