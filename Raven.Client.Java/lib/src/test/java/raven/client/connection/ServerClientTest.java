package raven.client.connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.List;
import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

import raven.abstractions.data.JsonDocument;
import raven.abstractions.data.PutResult;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJValue;
import raven.client.RavenDBAwareTests;
import raven.client.document.DocumentConvention;
import raven.client.listeners.IDocumentConflictListener;

public class ServerClientTest extends RavenDBAwareTests {

  private static class Person {
    private String firstname;
    private String lastname;
    public String getFirstname() {
      return firstname;
    }
    public void setFirstname(String firstname) {
      this.firstname = firstname;
    }
    public String getLastname() {
      return lastname;
    }
    public void setLastname(String lastname) {
      this.lastname = lastname;
    }

  }

  @Test
  //TODO: remove this test - as it breaks one test one functionality principal
  public void testDatabaseChanges() throws Exception {
    try {
      HttpJsonRequestFactory factory = new HttpJsonRequestFactory(10);

      ServerClient client = new ServerClient(DEFAULT_SERVER_URL, new DocumentConvention(), null, new ReplicationInformer(), factory, null, new IDocumentConflictListener[0]);
      IDatabaseCommands systemDatabaseClient = client.forSystemDatabase();
      assertSame(systemDatabaseClient, client);

      createDb("db1");

      List<String> databaseNames = systemDatabaseClient.getDatabaseNames(20, 0);
      System.out.println(databaseNames);

      IDatabaseCommands db1Commands = client.forDatabase("db1");
      Person person1 = new Person();
      person1.setFirstname("John");
      person1.setLastname("Smith");
      PutResult putResult = db1Commands.put("users/marcin", null, RavenJObject.fromObject(person1), null);
      assertNotNull(putResult.getEtag());

      assertNull("Object was created in different db!", client.get("users/marcin"));
      JsonDocument jsonDocument = db1Commands.get("users/marcin");
      assertNotNull(jsonDocument);

      assertEquals(new RavenJValue("John"), jsonDocument.getDataAsJson().get("firstname"));

      Person person2 = new Person();
      person2.setFirstname("Albert");
      person2.setLastname("Einstein");
      PutResult albertPutResult = db1Commands.put("users/albert", null, RavenJObject.fromObject(person2), null);

      List<JsonDocument> jsonDocuments = db1Commands.startsWith("users", "", 0, 10, false);
      assertEquals(2, jsonDocuments.size());


      List<JsonDocument> documents = db1Commands.getDocuments(0, 20, false);
      assertEquals(2, documents.size());
      //TODO: test for all fields!

      db1Commands.delete("users/albert", albertPutResult.getEtag());
      db1Commands.delete("users/albert", UUID.randomUUID());

      assertEquals(1,  db1Commands.getDocuments(0, 20, false).size());
    } finally {
      deleteDb("db1");
    }
  }
}
