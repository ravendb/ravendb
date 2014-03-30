package net.ravendb.abstractions.data;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.replication.ReplicationDestination;

import org.apache.commons.lang.StringUtils;

public class ConnectionStringParser<T extends RavenConnectionStringOptions> {


  public static <S extends RavenConnectionStringOptions> ConnectionStringParser<S> fromConnectionString(Class<S> class1, String connString) {
    return new ConnectionStringParser<>(class1, connString);
  }

  private static Pattern connectionStringRegex = Pattern.compile("(\\w+)\\s*=\\s*(.*)");


  private final String connectionString;

  private boolean setupPasswordInConnectionString;
  private boolean setupUsernameInConnectionString;

  private T connectionStringOptions;



  public void setConnectionStringOptions(T connectionStringOptions) {
    this.connectionStringOptions = connectionStringOptions;
  }

  public T getConnectionStringOptions() {
    return connectionStringOptions;
  }

  private ConnectionStringParser(Class<T> clazz, String connectionString) {
    try {
      setConnectionStringOptions(clazz.newInstance());
      this.connectionString = connectionString;
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to init ConnectionStringOptions", e);
    }
  }

  /**
   * Parse the connection string option
   * @param networkCredentials
   * @param key
   * @param value
   */
  protected void processConnectionStringOption(String key, String value) {
    switch (key)
    {
      case "apikey":
        connectionStringOptions.setApiKey(value);
        break;
      case "url":
        connectionStringOptions.setUrl(value);
        break;
      case "failover":
        if (connectionStringOptions.getFailoverServers() == null) {
          connectionStringOptions.setFailoverServers(new FailoverServers());
        }

        String[] databaseNameAndFailoverDestination = value.split("|");
        ReplicationDestination destination;
        try {
          if (databaseNameAndFailoverDestination.length == 1) {
            destination = JsonExtensions.createDefaultJsonSerializer().readValue(databaseNameAndFailoverDestination[0], ReplicationDestination.class);
            connectionStringOptions.getFailoverServers().addForDefaultDatabase(destination);
          } else {
            destination = JsonExtensions.createDefaultJsonSerializer().readValue(databaseNameAndFailoverDestination[1], ReplicationDestination.class);
            connectionStringOptions.getFailoverServers().addForDatabase(databaseNameAndFailoverDestination[0], destination);
          }
        } catch (IOException e) {
          throw new RuntimeException("Unable to process failover info", e);
        }
        break;
      case "database":
      case "defaultdatabase":
        connectionStringOptions.setDefaultDatabase(value);
        break;
      default:
        throw new IllegalArgumentException(String.format("Connection string : '%s' could not be parsed, unknown option: '%s'", connectionString, key));
    }
  }

  public void parse() {
    if (StringUtils.isEmpty(connectionString)) {
      throw new IllegalArgumentException("connection string is blank.");
    }
    String[] strings = connectionString.split(";");
    for (String str: strings) {
      String arg = str.trim();
      if (StringUtils.isEmpty(arg) || !arg.contains("=")) {
        continue;
      }
      Matcher matcher = connectionStringRegex.matcher(arg);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(String.format("Connection string name: '%s' could not be parsed", connectionString));
      }
      processConnectionStringOption(matcher.group(1).toLowerCase(), matcher.group(2).trim());
    }

    if (setupUsernameInConnectionString == false && setupPasswordInConnectionString == false)
      return;

    if (setupUsernameInConnectionString == false || setupPasswordInConnectionString == false) {
      throw new IllegalArgumentException(String.format("User and Password must both be specified in the connection string: '%s'", connectionString));
    }
  }

}
