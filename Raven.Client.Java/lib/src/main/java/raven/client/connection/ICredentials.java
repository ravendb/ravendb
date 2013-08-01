package raven.client.connection;

import java.net.URI;

public interface ICredentials {
  public NetworkCredential getCredential(URI uri, String authType);
}
