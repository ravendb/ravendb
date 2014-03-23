package net.ravendb.tests.document;

import static org.junit.Assert.assertEquals;

import java.util.List;

import net.ravendb.abstractions.indexing.IndexDefinition;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.RemoteClientTest;
import net.ravendb.client.document.DocumentStore;

import org.junit.Test;


public class GameTest extends RemoteClientTest {

  /**
   * http://groups.google.com/group/ravendb/browse_thread/thread/e9f045e073d7a698
   * @throws Exception
   */
  @Test
  public void willNotGetDuplicatedResults() throws Exception {
    try (IDocumentStore store = new DocumentStore(getDefaultUrl(), getDefaultDb()).initialize()) {

      IndexDefinition indexDefinition = new IndexDefinition();
      indexDefinition.setMap("from doc in docs where doc.DataUploadId != null " +
          " && doc.RealmName != null " +
          " && doc.Region != null " +
          " && doc.CharacterName != null " +
          " && doc.Zone != null " +
          " && doc.SubZone != null " +
          " select new " +
          " { " +
          " DataUploadId = doc.DataUploadId, " +
          " RealmName = doc.RealmName, " +
          " Region = doc.Region, " +
          " CharacterName = doc.CharacterName, " +
          " Zone = doc.Zone, " +
          " Count = 1 " +
          " };");

      indexDefinition.setReduce("from result in results " +
          " group result by new " +
          " { " +
          " DataUploadId = result.DataUploadId, " +
          " RealmName = result.RealmName, " +
          " Region = result.Region, " +
          " CharacterName = result.CharacterName, " +
          " Zone = result.Zone " +
          " } into g " +
          " select new " +
          " { " +
          " DataUploadId = g.Key.DataUploadId, " +
          " RealmName = g.Key.RealmName, " +
          " Region = g.Key.Region, " +
          " CharacterName = g.Key.CharacterName, " +
          " Zone = g.Key.Zone, " +
          " Count = g.Sum(x => (int)x.Count) " +
          " };");
      store.getDatabaseCommands().putIndex("GameEventCountZoneBySpecificCharacter", indexDefinition);

      try (IDocumentSession session = store.openSession()) {
        GameEvent event1 = new GameEvent();
        event1.setId("1");
        event1.setUserId("UserId1");
        event1.setTime("232");
        event1.setActionName("Something");
        event1.setCharacterName("Darykal");
        event1.setDataUploadId("10");
        event1.setRealmName("Moonglade");
        event1.setRegion("SingleRegion");
        event1.setSubZone("SubzoneOne");
        event1.setZone("ZoneOne");
        session.store(event1);

        GameEvent event2 = new GameEvent();
        event2.setId("2");
        event2.setUserId("UserId1");
        event2.setTime("232");
        event2.setActionName("Something");
        event2.setCharacterName("Darykal");
        event2.setDataUploadId("10");
        event2.setRealmName("Moonglade");
        event2.setRegion("SingleRegion");
        event2.setSubZone("SubzoneOne");
        event2.setZone("ZoneOne");
        session.store(event2);

        GameEvent event3 = new GameEvent();
        event3.setId("3");
        event3.setUserId("UserId1");
        event3.setTime("232");
        event3.setActionName("Something");
        event3.setCharacterName("Darykal");
        event3.setDataUploadId("10");
        event3.setRealmName("Moonglade");
        event3.setRegion("SingleRegion");
        event3.setSubZone("SubzoneOne");
        event3.setZone("ZoneOne");
        session.store(event3);

        GameEvent event4 = new GameEvent();
        event4.setId("4");
        event4.setUserId("UserId1");
        event4.setTime("232");
        event4.setActionName("Something");
        event4.setCharacterName("Darykal");
        event4.setDataUploadId("10");
        event4.setRealmName("Moonglade");
        event4.setRegion("SingleRegion");
        event4.setSubZone("SubzoneOne");
        event4.setZone("ZoneOne");
        session.store(event4);

        GameEvent event5 = new GameEvent();
        event5.setId("5");
        event5.setUserId("UserId1");
        event5.setTime("232");
        event5.setActionName("Something");
        event5.setCharacterName("Darykal");
        event5.setDataUploadId("10");
        event5.setRealmName("Moonglade");
        event5.setRegion("SingleRegion");
        event5.setSubZone("SubzoneOne");
        event5.setZone("ZoneOne");
        session.store(event5);

        GameEvent event6 = new GameEvent();
        event6.setId("6");
        event6.setUserId("UserId1");
        event6.setTime("232");
        event6.setActionName("Something");
        event6.setCharacterName("Darykal");
        event6.setDataUploadId("10");
        event6.setRealmName("Moonglade");
        event6.setRegion("SingleRegion");
        event6.setSubZone("SubzoneOne");
        event6.setZone("ZoneTwo");
        session.store(event6);

        GameEvent event7 = new GameEvent();
        event7.setId("7");
        event7.setUserId("UserId1");
        event7.setTime("232");
        event7.setActionName("Something");
        event7.setCharacterName("Darykal");
        event7.setDataUploadId("10");
        event7.setRealmName("Moonglade");
        event7.setRegion("SingleRegion");
        event7.setSubZone("SubzoneOne");
        event7.setZone("ZoneTwo");
        session.store(event7);

        GameEvent event8 = new GameEvent();
        event8.setId("8");
        event8.setUserId("UserId1");
        event8.setTime("232");
        event8.setActionName("Something");
        event8.setCharacterName("Darykal");
        event8.setDataUploadId("10");
        event8.setRealmName("Moonglade");
        event8.setRegion("SingleRegion");
        event8.setSubZone("SubzoneOne");
        event8.setZone("ZoneThree");
        session.store(event8);

        GameEvent event9 = new GameEvent();
        event9.setId("9");
        event9.setUserId("UserId1");
        event9.setTime("232");
        event9.setActionName("Something");
        event9.setCharacterName("Darykal");
        event9.setDataUploadId("10");
        event9.setRealmName("Moonglade");
        event9.setRegion("SingleRegion");
        event9.setSubZone("SubzoneOne");
        event9.setZone("ZoneThree");
        session.store(event9);

        GameEvent event10 = new GameEvent();
        event10.setId("10");
        event10.setUserId("UserId1");
        event10.setTime("232");
        event10.setActionName("Something");
        event10.setCharacterName("Darykal");
        event10.setDataUploadId("10");
        event10.setRealmName("Moonglade");
        event10.setRegion("SingleRegion");
        event10.setSubZone("SubzoneOne");
        event10.setZone("ZoneOne");
        session.store(event10);

        session.saveChanges();

        List<ZoneCountResult> darykalSumResults = session.advanced()
        .documentQuery(GameEvent.class, "GameEventCountZoneBySpecificCharacter")
        .where("RealmName:Moonglade AND Region:SingleRegion AND DataUploadId:10")
        .selectFields(ZoneCountResult.class, "Zone", "Count")
        .waitForNonStaleResults(24 * 3600 * 1000)
        .toList();

        assertEquals(3, darykalSumResults.size());

      }

    }
  }

  public static class GameEvent {
    private String id;
    private String userId;
    private String region;
    private String characterName;
    private String realmName;
    private String dataUploadId;
    private String time;
    private String actionName;
    private String zone;
    private String subZone;
    public String getId() {
      return id;
    }
    public void setId(String id) {
      this.id = id;
    }
    public String getUserId() {
      return userId;
    }
    public void setUserId(String userId) {
      this.userId = userId;
    }
    public String getRegion() {
      return region;
    }
    public void setRegion(String region) {
      this.region = region;
    }
    public String getCharacterName() {
      return characterName;
    }
    public void setCharacterName(String characterName) {
      this.characterName = characterName;
    }
    public String getRealmName() {
      return realmName;
    }
    public void setRealmName(String realmName) {
      this.realmName = realmName;
    }
    public String getDataUploadId() {
      return dataUploadId;
    }
    public void setDataUploadId(String dataUploadId) {
      this.dataUploadId = dataUploadId;
    }
    public String getTime() {
      return time;
    }
    public void setTime(String time) {
      this.time = time;
    }
    public String getActionName() {
      return actionName;
    }
    public void setActionName(String actionName) {
      this.actionName = actionName;
    }
    public String getZone() {
      return zone;
    }
    public void setZone(String zone) {
      this.zone = zone;
    }
    public String getSubZone() {
      return subZone;
    }
    public void setSubZone(String subZone) {
      this.subZone = subZone;
    }

  }

  public static class GameEventCount {
    private String region;
    private String characterName;
    private String realmName;
    private String dataUploadId;
    private String zone;
    private String subZone;
    private int count;
    public String getRegion() {
      return region;
    }
    public void setRegion(String region) {
      this.region = region;
    }
    public String getCharacterName() {
      return characterName;
    }
    public void setCharacterName(String characterName) {
      this.characterName = characterName;
    }
    public String getRealmName() {
      return realmName;
    }
    public void setRealmName(String realmName) {
      this.realmName = realmName;
    }
    public String getDataUploadId() {
      return dataUploadId;
    }
    public void setDataUploadId(String dataUploadId) {
      this.dataUploadId = dataUploadId;
    }
    public String getZone() {
      return zone;
    }
    public void setZone(String zone) {
      this.zone = zone;
    }
    public String getSubZone() {
      return subZone;
    }
    public void setSubZone(String subZone) {
      this.subZone = subZone;
    }
    public int getCount() {
      return count;
    }
    public void setCount(int count) {
      this.count = count;
    }

  }

}
