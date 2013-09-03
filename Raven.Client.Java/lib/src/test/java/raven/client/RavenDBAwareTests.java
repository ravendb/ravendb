package raven.client;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import raven.abstractions.closure.Functions;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.connection.IDatabaseCommands;
import raven.client.connection.ReplicationInformer;
import raven.client.connection.ServerClient;
import raven.client.connection.implementation.HttpJsonRequestFactory;
import raven.client.document.DocumentConvention;
import raven.client.document.DocumentStore;
import raven.client.listeners.IDocumentConflictListener;
import raven.client.utils.UrlUtils;

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

  public final static String DEFAULT_SERVER_RUNNER_URL = "http://" + DEFAULT_HOST + ":" + DEFAULT_RUNNER_PORT + "/servers";

  protected static HttpClient client = new DefaultHttpClient();

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
    try {
      startServer(DEFAULT_SERVER_PORT_1);
    } finally {

    }
  }

  @AfterClass
  public static void stopServerAfter() throws Exception {
    try {
      stopServer(DEFAULT_SERVER_PORT_1);
    } finally {

    }
  }

  @Before
  public void init() {
    System.setProperty("java.net.preferIPv4Stack" , "true");
    convention = new DocumentConvention();
    convention.setEnlistInDistributedTransactions(false);
    factory = new HttpJsonRequestFactory(10);

    replicationInformer = new ReplicationInformer(convention);

    serverClient = new ServerClient(DEFAULT_SERVER_URL_1, convention, null,
        new Functions.StaticFunction1<String, ReplicationInformer>(replicationInformer), null, factory,
        UUID.randomUUID(), new IDocumentConflictListener[0]);
  }

  protected void useFiddler(IDocumentStore store){
    /*System.setProperty("http.proxyHost", "127.0.0.1");
    System.setProperty("https.proxyHost", "127.0.0.1");
    System.setProperty("http.proxyPort", "8888");
    System.setProperty("https.proxyPort", "8888");*/
    HttpHost proxy = new HttpHost("127.0.0.1", 8888, "http");
    store.getJsonRequestFactory().getHttpClient().getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
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
      put.setEntity(new StringEntity(getCreateDbDocument(dbName), ContentType.APPLICATION_JSON));
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
    startServer(DEFAULT_SERVER_PORT_1);
  }

  protected void stopServer() throws Exception{
    stopServer(DEFAULT_SERVER_PORT_1);
  }

  protected static void startServer(int port) throws Exception {
    HttpPut put = null;
    try {
      put = new HttpPut(DEFAULT_SERVER_RUNNER_URL);
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


  protected String getCreateDbDocument(String dbName) {
    RavenJObject doc = new RavenJObject();
    RavenJObject settings = new RavenJObject();
    doc.add("Settings", settings);
    settings.add("Raven/DataDir", RavenJValue.fromObject("~\\Databases\\" + dbName));
    settings.add("Raven/ActiveBundles", RavenJValue.fromObject("Replication"));
    doc.add("SecuredSettings", new RavenJObject());
    doc.add("Disabled", new RavenJValue(false));
    return doc.toString();
  }

  protected static String getCreateServerDocument(int port) {
    RavenJObject doc = new RavenJObject();
    doc.add("Port", new RavenJValue(port));
    doc.add("RunInMemory", new RavenJValue(true));
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
    return testName.getMethodName();
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
