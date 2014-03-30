package net.ravendb.abstractions.json;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;

public class RavenJsonTextReader extends JsonFactory {
  public RavenJsonTextReader() {
    super();
    enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
  }
}
