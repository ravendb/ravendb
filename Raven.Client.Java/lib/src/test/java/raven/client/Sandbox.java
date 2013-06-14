package raven.client;

import static org.junit.Assert.assertNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.URIException;
import org.junit.Test;

import raven.abstractions.data.Constants;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.PutResult;
import raven.abstractions.exceptions.ServerClientException;
import raven.abstractions.json.linq.RavenJObject;
import raven.client.RavenJObjectsTest.Person;
import raven.client.connection.ServerClient;

public class Sandbox {


  @Test
  public void testParseRavenLastModifiedDate() throws ParseException {
    String dateString = "2013-05-10T11:33:04.6708000Z";
    SimpleDateFormat sdf = new SimpleDateFormat(Constants.RAVEN_LAST_MODIFIED_DATE_FORAT);
    Date date = sdf.parse(dateString);
    System.out.println(date);

  }

 /* @Test
  public void testGet() throws ServerClientException {
    ServerClient client = new ServerClient("http://localhost:8123");
    JsonDocument jsonDocument = client.get("users/ayende");
    System.out.println(jsonDocument);

    Person p1 = new Person();
    p1.setName("John");
    p1.setSurname("Smith");

    RavenJObject person = RavenJObject.fromObject(p1);

    PutResult putResult = client.put("persons/20", null, person, null);
    System.out.println(putResult);

    JsonDocument document = client.get("no-such-key");
    assertNull(document);

    client.delete("no-such", null);
    client.delete("persons/20", null);

  }*/
}