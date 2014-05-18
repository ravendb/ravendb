package net.ravendb.java.http.client;

import java.net.URI;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

/**
 * Http EVAL method
 *
 */
public class HttpEval extends HttpEntityEnclosingRequestBase {
  public final static String METHOD_NAME = "EVAL";

  public HttpEval() {
      super();
  }

  public HttpEval(final URI uri) {
      super();
      setURI(uri);
  }

  /**
   * @throws IllegalArgumentException if the uri is invalid.
   */
  public HttpEval(final String uri) {
      super();
      setURI(URI.create(uri));
  }

  @Override
  public String getMethod() {
      return METHOD_NAME;
  }

}
