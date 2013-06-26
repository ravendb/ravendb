package raven.client;

import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Test;

import raven.abstractions.data.Constants;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.PutResult;
import raven.abstractions.exceptions.ServerClientException;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.client.RavenJObjectsTest.Person;
import raven.client.connection.ServerClient;

public class Sandbox {

  public static void main(String[] args) throws ClientProtocolException, IOException, URISyntaxException {

    String text = "this is sample text";
    byte[] textBytes = text.getBytes();

    Set<String> s1 =new HashSet<>();
    s1.add("df");
    s1.add("aa");
    String string = RavenJArray.fromObject(s1).toString();
    System.out.println(string);

  }
  @Test
  public void testParseRavenLastModifiedDate() throws ParseException {
    String dateString = "2013-05-10T11:33:04.6708000Z";
    SimpleDateFormat sdf = new SimpleDateFormat(Constants.RAVEN_LAST_MODIFIED_DATE_FORMAT);
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