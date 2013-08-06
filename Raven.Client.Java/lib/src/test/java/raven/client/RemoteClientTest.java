package raven.client;

import org.junit.Rule;
import org.junit.rules.TestName;

public  abstract class RemoteClientTest {


  @Rule
  public TestName testName = new TestName();

  protected String getDefaultUrl() {
    return RavenDBAwareTests.DEFAULT_SERVER_URL_1;
  }

  protected String getDefaultDb() {
    return testName.getMethodName();
  }
}
