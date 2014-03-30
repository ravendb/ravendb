package net.ravendb.client.connection;

import java.io.IOException;
import java.util.Iterator;

import net.ravendb.abstractions.json.linq.RavenJObject;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;


public class RavenJObjectIterator implements Iterator<RavenJObject>, AutoCloseable {

  private HttpEntity httpEntity;
  private CloseableHttpResponse httpResponse;
  private JsonParser jsonParser;
  private boolean hasNext;
  private RavenJObject currentObject;

  public RavenJObjectIterator(CloseableHttpResponse httpResponse, JsonParser jsonParser) {
    try {
      this.httpResponse = httpResponse;
      this.httpEntity = httpResponse.getEntity();
      this.jsonParser = jsonParser;
      fetchNextObject();
    } catch (IOException e) {
      throw new RuntimeException("Unable to read stream!");
    }
  }

  private void fetchNextObject() throws JsonParseException, IOException {
    JsonToken token = jsonParser.nextToken();
    if (token == JsonToken.END_ARRAY) {
      hasNext = false;
      EntityUtils.consumeQuietly(httpEntity);
      this.currentObject = null;
    } else {
      this.currentObject = RavenJObject.load(jsonParser);
      this.hasNext = true;
    }

  }

  @Override
  public boolean hasNext() {
    return hasNext;
  }

  @Override
  public RavenJObject next() {
    RavenJObject current = currentObject;
    try {
      fetchNextObject();
    } catch (IOException e) {
      throw new RuntimeException("Unable to read object");
    }
    return current;
  }

  @Override
  public void remove() {
    throw new IllegalStateException("You can't remove entries");
  }

  @Override
  public void close() throws Exception {
    EntityUtils.consumeQuietly(httpEntity);
  }

}
