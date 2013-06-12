package raven.client.extensions;

//TODO: finish me
public class MultiDatabase {
  public static String getRootDatabaseUrl(String url) {
    int indexOfDatabases = url.indexOf("/databases/");
    if (indexOfDatabases != -1) {
      url = url.substring(0, indexOfDatabases);
    }
    if (url.endsWith("/")) {
      return url.substring(0, url.length() - 1);
    }
    return url;
  }
}
