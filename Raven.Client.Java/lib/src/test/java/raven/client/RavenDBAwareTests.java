package raven.client;


import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpParams;

import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.utils.UrlUtils;

public abstract class RavenDBAwareTests {

  public final static String DEFAULT_SERVER_URL = "http://localhost:8123";

  private HttpClient client = new DefaultHttpClient();

  public String getServerUrl() {
    return DEFAULT_SERVER_URL;
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

  private String getCreateDbDocument(String dbName) {
    RavenJObject doc = new RavenJObject();
    RavenJObject settings = new RavenJObject();
    doc.add("Settings", settings);
    settings.add("Raven/DataDir", RavenJValue.fromObject("~\\Databases\\" + dbName));
    settings.add("Raven/ActiveBundles", RavenJValue.fromObject("Raven/ActiveBundles"));
    doc.add("SecuredSettings", new RavenJObject());
    doc.add("Disabled", new RavenJValue(false));
    return doc.toString();
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
