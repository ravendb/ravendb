package net.ravendb.client;

import static org.junit.Assert.assertEquals;

import net.ravendb.abstractions.json.linq.RavenJToken;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;


public abstract class RemoteClientTest extends RavenDBAwareTests {

  protected String getDefaultUrl() {
    return RavenDBAwareTests.DEFAULT_SERVER_URL_1;
  }

  protected String getDefaultDb() {
    return getDbName();
  }

  protected RavenJToken getAdminStats(String serverUrl) {
    return serverClient.executeGetRequest("/admin/stats");
  }

  public int getNumberOfRequests() {
    RavenJToken adminStats = getAdminStats(getDefaultUrl());
    return adminStats.value(Integer.class, "TotalNumberOfRequests");
  }

  protected void assertNumberOfRequests(int i, int prevValue) {
    int currentReqCount = getNumberOfRequests();
    currentReqCount--; //  remove one as it is current request for adminStats
    assertEquals("Expected " + i + " requests. Got: " + (currentReqCount - prevValue), i, currentReqCount - prevValue);
  }

  protected void waitForAllRequestsToComplete() throws Exception {
    HttpGet get = new HttpGet(DEFAULT_SERVER_RUNNER_URL + "?port=" + DEFAULT_SERVER_PORT_1 + "&action=waitForAllRequestsToComplete");
    HttpResponse httpResponse = null;
    try {
      httpResponse = client.execute(get);

      assertEquals(HttpStatus.SC_OK, httpResponse.getStatusLine().getStatusCode());
    } finally {
      if (httpResponse != null) {
        EntityUtils.consumeQuietly(httpResponse.getEntity());
      }
    }

  }


}
