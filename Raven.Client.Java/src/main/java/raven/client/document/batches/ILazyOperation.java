package raven.client.document.batches;

import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;

public interface ILazyOperation {
  public GetRequest createRequest();
  public Object getResult();
  public boolean isRequiresRetry();
  public void handleResponse(GetResponse response);
  public AutoCloseable enterContext();

}
