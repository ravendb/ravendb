package raven.abstractions.data;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import raven.client.connection.NetworkCredential;

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
  protected void processConnectionStringOption(NetworkCredential networkCredentials, String key, String value) {
    switch (key)
    {
    case "apikey":
      connectionStringOptions.setApiKey(value);
      break;
    case "enlist":
      connectionStringOptions.setEnlistInDistributedTransactions(Boolean.valueOf(value));
      break;
    case "resourcemanagerid":
      connectionStringOptions.setResourceManagerId(UUID.fromString(value));
      break;
    case "url":
      connectionStringOptions.setUrl(value);
      break;
      /*TODO:
       * case "failoverurl":
                    if (ConnectionStringOptions.FailoverServers == null)
                        ConnectionStringOptions.FailoverServers = new FailoverServers();

                    var databaseNameAndFailoverUrl = value.Split('|');

                    if (databaseNameAndFailoverUrl.Length == 1)
                    {
                        ConnectionStringOptions.FailoverServers.AddForDefaultDatabase(urls: databaseNameAndFailoverUrl[0]);
                    }
                    else
                    {
                        ConnectionStringOptions.FailoverServers.AddForDatabase(databaseName: databaseNameAndFailoverUrl[0], urls: databaseNameAndFailoverUrl[1]);
                    }
                    break;
       */
    case "database":
    case "defaultdatabase":
      connectionStringOptions.setDefaultDatabase(value);
      break;
    case "user":
      networkCredentials.setUserName(value);
      setupUsernameInConnectionString = true;
      break;
    case "password":
      networkCredentials.setPassword(value);
      setupPasswordInConnectionString = true;
      break;
    case "domain":
      networkCredentials.setDomain(value);
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
    NetworkCredential networkCredential = new NetworkCredential();
    for (String str: strings) {
      String arg = str.trim();
      if (StringUtils.isEmpty(arg) || !arg.contains("=")) {
        continue;
      }
      Matcher matcher = connectionStringRegex.matcher(arg);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(String.format("Connection string name: '%s' could not be parsed", connectionString));
      }
      processConnectionStringOption(networkCredential, matcher.group(1).toLowerCase(), matcher.group(2).trim());
    }

    if (setupUsernameInConnectionString == false && setupPasswordInConnectionString == false)
      return;

    if (setupUsernameInConnectionString == false || setupPasswordInConnectionString == false) {
      throw new IllegalArgumentException(String.format("User and Password must both be specified in the connection string: '%s'", connectionString));
    }
    connectionStringOptions.setCredentials(networkCredential);
  }

}
