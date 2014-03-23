package net.ravendb.client;


import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import net.ravendb.abstractions.basic.EventHandler;
import net.ravendb.abstractions.closure.Functions;
import net.ravendb.abstractions.connection.OperationCredentials;
import net.ravendb.abstractions.connection.WebRequestEventArgs;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJValue;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.connection.IDocumentStoreReplicationInformer;
import net.ravendb.client.connection.ReplicationInformer;
import net.ravendb.client.connection.ServerClient;
import net.ravendb.client.connection.implementation.HttpJsonRequestFactory;
import net.ravendb.client.document.DocumentConvention;
import net.ravendb.client.document.FailoverBehavior;
import net.ravendb.client.document.FailoverBehaviorSet;
import net.ravendb.client.listeners.IDocumentConflictListener;
import net.ravendb.client.utils.UrlUtils;

import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;


public abstract class RavenDBAwareTests {

  @Rule
  public TestName testName = new TestName();

  protected DocumentConvention convention;
  protected HttpJsonRequestFactory factory;
  protected ReplicationInformer replicationInformer;
  protected ServerClient serverClient;

  public final static String DEFAULT_HOST = "localhost";
  public final static int DEFAULT_SERVER_PORT_1 = 8123;
  public final static int DEFAULT_SERVER_PORT_2 = 8124;
  public final static String DEFAULT_SERVER_URL_1 = "http://" + DEFAULT_HOST + ":" + DEFAULT_SERVER_PORT_1;
  public final static String DEFAULT_SERVER_URL_2 = "http://" + DEFAULT_HOST + ":" + DEFAULT_SERVER_PORT_2;

  public final static int DEFAULT_RUNNER_PORT = 8585;

  public final static boolean RUN_IN_MEMORY = false;

  public final static String DEFAULT_SERVER_RUNNER_URL = "http://" + DEFAULT_HOST + ":" + DEFAULT_RUNNER_PORT + "/servers";

  protected static HttpClient client = HttpClients.createDefault();


  public String getServerUrl() {
    return DEFAULT_SERVER_URL_1;
  }

  public String getServerUrl(int i) {
    if (i == 1) {
      return DEFAULT_SERVER_URL_1;
    }
    return DEFAULT_SERVER_URL_2;
  }

  @BeforeClass
  public static void startServerBefore() throws Exception {
    startServer(DEFAULT_SERVER_PORT_1, true);
  }

  @AfterClass
  public static void stopServerAfter() throws Exception {
    stopServer(DEFAULT_SERVER_PORT_1);
  }

  @Before
  public void init() {
    System.setProperty("java.net.preferIPv4Stack" , "true");
    convention = new DocumentConvention();
    convention.setFailoverBehavior(new FailoverBehaviorSet(FailoverBehavior.FAIL_IMMEDIATELY));
    factory = new HttpJsonRequestFactory(10);

    replicationInformer = new ReplicationInformer(convention);

    serverClient = new ServerClient(DEFAULT_SERVER_URL_1, convention, new OperationCredentials(),
      new Functions.StaticFunction1<String, IDocumentStoreReplicationInformer>((IDocumentStoreReplicationInformer)replicationInformer), null, factory,
      UUID.randomUUID(), new IDocumentConflictListener[0]);

  }

  @After
  public void cleanUp() throws Exception {
    factory.close();
  }


  protected void useFiddler(IDocumentStore store){
    store.getJsonRequestFactory().addConfigureRequestEventHandler(new FiddlerConfigureRequestHandler());
  }

  public static class FiddlerConfigureRequestHandler implements EventHandler<WebRequestEventArgs> {

    @Override
    public void handle(Object sender, WebRequestEventArgs event) {
      HttpRequestBase requestBase = (HttpRequestBase) event.getRequest();
      HttpHost proxy = new HttpHost("127.0.0.1", 8888, "http");
      RequestConfig requestConfig = requestBase.getConfig();
      if (requestConfig == null) {
        requestConfig = RequestConfig.DEFAULT;
      }
      requestConfig = RequestConfig.copy(requestConfig).setProxy(proxy).build();
      requestBase.setConfig(requestConfig);

    }
  }



  /**
   * Creates new db with name taken from test name
   */
  protected void createDb() throws Exception {
    createDb(1);
  }

  protected void createDb(int i) throws Exception {
    createDb(getDbName(), i);
  }

  protected void createDb(String dbName) throws Exception {
    createDb(dbName, 1);
  }

  protected void createDb(String dbName, int i) throws Exception {
    HttpPut put = null;
    try {
      put = new HttpPut(getServerUrl(i) + "/admin/databases/" + UrlUtils.escapeDataString(dbName));
      put.setEntity(new StringEntity(getCreateDbDocument(dbName, getDefaultServerPort(i)), ContentType.APPLICATION_JSON));
      HttpResponse httpResponse = client.execute(put);
      if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IllegalStateException("Invalid response on put:" + httpResponse.getStatusLine().getStatusCode());
      }
    } finally {
      if (put != null) {
        put.releaseConnection();
      }
    }
  }

  private int getDefaultServerPort(int i) {
    if (i == 1) {
      return DEFAULT_SERVER_PORT_1;
    }
    return DEFAULT_SERVER_PORT_2;
  }

  protected void createDbAtPort(String dbName, int port) throws Exception {
    HttpPut put = null;
    try {
      put = new HttpPut("http://" + DEFAULT_HOST + ":" + port + "/admin/databases/" + UrlUtils.escapeDataString(dbName));
      put.setEntity(new StringEntity(getCreateDbDocument(dbName, port), ContentType.APPLICATION_JSON));
      HttpResponse httpResponse = client.execute(put);
      if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IllegalStateException("Invalid response on put:" + httpResponse.getStatusLine().getStatusCode());
      }
    } finally {
      if (put != null) {
        put.releaseConnection();
      }
    }
  }

  protected void startServer() throws Exception{
    startServer(DEFAULT_SERVER_PORT_1, true);
  }

  protected void stopServer() throws Exception{
    stopServer(DEFAULT_SERVER_PORT_1);
  }

  protected static void startServerWithOAuth(int port) throws Exception {
    HttpPut put = null;
    try {
      put = new HttpPut(DEFAULT_SERVER_RUNNER_URL);
      put.setEntity(new StringEntity(getCreateServerDocumentWithApiKey(port), ContentType.APPLICATION_JSON));
      HttpResponse httpResponse = client.execute(put);
      if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IllegalStateException("Invalid response on put:" + httpResponse.getStatusLine().getStatusCode());
      }
    } finally {
      if (put != null) {
        put.releaseConnection();
      }
    }
  }

  protected static void startServer(int port, boolean deleteData) throws Exception {
    HttpPut put = null;
    try {
      String putUrl = DEFAULT_SERVER_RUNNER_URL;
      if (deleteData) {
        putUrl += "?deleteData=true";
      }
      put = new HttpPut(putUrl);
      put.setEntity(new StringEntity(getCreateServerDocument(port), ContentType.APPLICATION_JSON));
      HttpResponse httpResponse = client.execute(put);
      if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IllegalStateException("Invalid response on put:" + httpResponse.getStatusLine().getStatusCode());
      }
    } finally {
      if (put != null) {
        put.releaseConnection();
      }
    }
  }

  protected static void stopServer(int port) throws Exception {
    HttpDelete delete = null;
    try {
      delete = new HttpDelete(DEFAULT_SERVER_RUNNER_URL + "?port=" + port);
      HttpResponse httpResponse = client.execute(delete);
      if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IllegalStateException("Invalid response on put:" + httpResponse.getStatusLine().getStatusCode());
      }
    } finally {
      if (delete != null) {
        delete.releaseConnection();
      }
    }
  }


  protected String getCreateDbDocument(String dbName, int port) {
    RavenJObject doc = new RavenJObject();
    RavenJObject settings = new RavenJObject();
    doc.add("Settings", settings);
    settings.add("Raven/DataDir", RavenJValue.fromObject("~\\" +  port + "\\Databases\\" + dbName));
    settings.add("Raven/ActiveBundles", RavenJValue.fromObject("Replication"));
    doc.add("SecuredSettings", new RavenJObject());
    doc.add("Disabled", new RavenJValue(false));
    return doc.toString();
  }

  protected static String getCreateServerDocument(int port) {
    RavenJObject doc = new RavenJObject();
    doc.add("Port", new RavenJValue(port));
    doc.add("RunInMemory", new RavenJValue(RUN_IN_MEMORY));
    doc.add("ApiKeyName", new RavenJValue("java"));
    return doc.toString();
  }

  protected static String getCreateServerDocumentWithApiKey(int port) {
    RavenJObject doc = new RavenJObject();
    doc.add("Port", port);
    doc.add("RunInMemory", RUN_IN_MEMORY);
    doc.add("ApiKeyName", "java");
    doc.add("ApiKeySecret", "6B4G51NrO0P");
    doc.add("UseCommercialLicense", true);
    return doc.toString();
  }


  protected void waitForNonStaleIndexes(IDatabaseCommands dbCommands) {
    while (true) {
      if (dbCommands.getStatistics().getStaleIndexes().length == 0) {
        return;
      }
      try {
        Thread.sleep(40);
      } catch (InterruptedException e) {
        //ignore
      }

    }
  }

  protected <T> List<T> extractSinglePropertyFromList(List<RavenJObject> inputList, String propName, Class<T> resultClass) {
    List<T> result = new ArrayList<>();
    for (RavenJObject obj: inputList) {
      result.add(obj.value(resultClass, propName));
    }
    return result;
  }


  protected String getDbName() {
    String method = testName.getMethodName();
    StringBuilder dbName = new StringBuilder();
    if (method.length() > 15) {
      dbName.append(method.substring(0, 10));
      for (int i = 10; i < method.length() - 2; i++) {
        if (CharUtils.isAsciiAlphaUpper(method.charAt(i))) {
          dbName.append(method.charAt(i));
          if (CharUtils.isAsciiAlphaLower(method.charAt(i+1))) {
            dbName.append(method.charAt(i+1));
          }
        }
        if ('_' == method.charAt(i)) {
          dbName.append(CharUtils.toString(method.charAt(i+1)).toUpperCase());
          dbName.append(method.charAt(i+2));
          i++;
        }
      }
    } else {
      return method;
    }
    return dbName.toString();
  }


  protected void deleteDb() throws Exception {
    deleteDb(getDbName(), 1);
  }


  protected void deleteDb(int i) throws Exception {
    deleteDb(getDbName(), i);
  }

  protected void deleteDb(String dbName) throws Exception {
    deleteDb(dbName, 1);
  }

  protected Date mkDate(int year, int month, int day) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    calendar = DateUtils.truncate(calendar, Calendar.DAY_OF_MONTH);
    calendar.set(year, month - 1, day);
    return calendar.getTime();
  }

  protected void deleteDb(String dbName, int i) throws Exception {

    HttpDelete deleteMethod = null;
    try {
      deleteMethod = new HttpDelete(getServerUrl(i) + "/admin/databases/" + UrlUtils.escapeDataString(dbName) + "?hard-delete=true");
      HttpResponse httpResponse = client.execute(deleteMethod);
      if (httpResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new IllegalStateException("Invalid response on put:" + httpResponse.getStatusLine().getStatusCode());
      }
    } finally {
      if (deleteMethod != null) {
        deleteMethod.releaseConnection();
      }
    }

  }
}
