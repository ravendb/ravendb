package net.ravendb.java.http.client;

import java.net.URI;

import org.apache.http.client.methods.HttpRequestBase;

/**
 * Http RESET method
 *
 */
public class HttpReset extends HttpRequestBase {
  public final static String METHOD_NAME = "RESET";

  public HttpReset() {
      super();
  }

  public HttpReset(final URI uri) {
      super();
      setURI(uri);
  }

  /**
   * @throws IllegalArgumentException if the uri is invalid.
   */
  public HttpReset(final String uri) {
      super();
      setURI(URI.create(uri));
  }

  @Override
  public String getMethod() {
      return METHOD_NAME;
  }

}
