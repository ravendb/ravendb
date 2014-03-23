package net.ravendb.client.extensions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.DatabaseDocument;


/**
 * Methods to create multitenant databases
 */
public class MultiDatabase {

  private static final String VALID_DB_NAME_CHARS = "[A-Za-z0-9_\\-\\.]+";

  public static DatabaseDocument createDatabaseDocument(String name) {
    assertValidName(name);
    if (name.equalsIgnoreCase(Constants.SYSTEM_DATABASE)) {
      DatabaseDocument databaseDocument = new DatabaseDocument();
      databaseDocument.setId(Constants.SYSTEM_DATABASE);
      return databaseDocument;
    }
    DatabaseDocument document = new DatabaseDocument();
    document.setId("Raven/Databases/" + name);
    document.getSettings().put("Raven/DataDir", "~\\Databases\\" + name);
    return document;
  }

  public static void assertValidName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("name");
    }
    if (name.equalsIgnoreCase(Constants.SYSTEM_DATABASE)) {
      return;
    }
    if (!name.matches(VALID_DB_NAME_CHARS)) {
      throw new IllegalStateException("Database name can only contain A-Z, a-z, 0-9, \"_\", \".\" or \"-\", but was: " + name);
    }
  }

  public static String getDatabaseUrl(String url, String database) {
    if (Constants.SYSTEM_DATABASE.equals(database)) {
      return getRootDatabaseUrl(url);
    }
    return getRootDatabaseUrl(url) + "/databases/" + database + "/";
  }

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

  public static String getDatabaseName(String url) {
    if (url == null) {
      return null;
    }
    String databaseUrl = url;
    int indexOfDatabases = databaseUrl.indexOf("/databases/");
    if (indexOfDatabases != -1) {
      databaseUrl = databaseUrl.substring(indexOfDatabases + "/databases/".length());
      Matcher matcher = Pattern.compile(VALID_DB_NAME_CHARS).matcher(databaseUrl);
      if (matcher.find()) {
        return matcher.group();
      }
    }
    return Constants.SYSTEM_DATABASE;
  }
}
