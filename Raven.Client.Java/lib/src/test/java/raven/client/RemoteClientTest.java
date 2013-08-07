package raven.client;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class RemoteClientTest {

  @Rule
  public TestName testName = new TestName();

  protected String getDefaultUrl() {
    return RavenDBAwareTests.DEFAULT_SERVER_URL_1;
  }

  @After
  public void after() {
    //TODO: delete db
  }


  protected String getDefaultDb() {
    return testName.getMethodName();
  }
}
