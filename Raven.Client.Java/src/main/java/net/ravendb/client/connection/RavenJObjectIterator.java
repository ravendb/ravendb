package net.ravendb.client.connection;

import java.io.IOException;
import java.util.Iterator;

import net.ravendb.abstractions.json.linq.RavenJObject;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;


public class RavenJObjectIterator implements Iterator<RavenJObject> {

  private HttpEntity httpEntity;
  private JsonParser jsonParser;
  private boolean hasNext;
  private RavenJObject currentObject;

  public RavenJObjectIterator(HttpEntity httpEntity, JsonParser jsonParser) {
    try {
      this.httpEntity = httpEntity;
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

}
