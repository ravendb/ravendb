package raven.client;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Before;
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
import raven.client.listeners.IDocumentConflictListener;
import raven.client.utils.UrlUtils;

public abstract class RavenDBAwareTests {

  @Rule
  public TestName testName = new TestName();

  protected DocumentConvention convention;
  protected HttpJsonRequestFactory factory;
  protected ReplicationInformer replicationInformer;
  protected ServerClient serverClient;

  public final static String DEFAULT_SERVER_URL = "http://localhost:8123";

  private HttpClient client = new DefaultHttpClient();

  public String getServerUrl() {
    return DEFAULT_SERVER_URL;
  }

  @Before
  public void init() {
    System.setProperty("java.net.preferIPv4Stack" , "true");
    convention = new DocumentConvention();
    factory = new HttpJsonRequestFactory(10);
    replicationInformer = new ReplicationInformer(convention);

    serverClient = new ServerClient(DEFAULT_SERVER_URL, convention, null,
        new Functions.StaticFunction1<String, ReplicationInformer>(replicationInformer), null, factory,
        UUID.randomUUID(), new IDocumentConflictListener[0]);
  }


  /**
   * Creates new db with name taken from test name
   */
  protected void createDb() throws Exception {
    createDb(getDbName());
  }

  protected void createDb(String dbName) throws Exception {
    HttpPut put = null;
    try {
      put = new HttpPut(getServerUrl() + "/admin/databases/" + UrlUtils.escapeDataString(dbName));
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

  protected String getCreateDbDocument(String dbName) {
    RavenJObject doc = new RavenJObject();
    RavenJObject settings = new RavenJObject();
    doc.add("Settings", settings);
    settings.add("Raven/DataDir", RavenJValue.fromObject("~\\Databases\\" + dbName));
    settings.add("Raven/ActiveBundles", RavenJValue.fromObject("Raven/ActiveBundles"));
    doc.add("SecuredSettings", new RavenJObject());
    doc.add("Disabled", new RavenJValue(false));
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

  protected void deleteDb() throws Exception {
    deleteDb(getDbName());
  }

  protected String getDbName() {
    return testName.getMethodName();
  }


  protected void deleteDb(String dbName) throws Exception {

    HttpDelete deleteMethod = null;
    try {
      deleteMethod = new HttpDelete(getServerUrl() + "/admin/databases/" + UrlUtils.escapeDataString(dbName) + "?hard-delete=true");
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
