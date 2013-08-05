package raven.client.document.sessionoperations;

import raven.abstractions.closure.Function0;
import raven.abstractions.closure.Function1;
import raven.abstractions.data.JsonDocument;
import raven.client.document.InMemoryDocumentSessionOperations;

//TODO: finish me
public class LoadOperation {
  public LoadOperation(InMemoryDocumentSessionOperations sessionOperations, AutoCloseable disableAllCaching, String id) {
    //TODO:
  }
  public void logOperation() {
    //TODO:
  }

  public AutoCloseable enterLoadContext() {
    //TODO:
    return null;
  }

  public boolean setResult(JsonDocument document) {
    //TODO:
    return false;///TODO: delete me
  }

  public <T> T complete(Class<T> clazz) {
    //TODO:
    return null;
  }


}
