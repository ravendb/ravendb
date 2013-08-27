package raven.client.connection;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import raven.abstractions.data.JsonDocument;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.logging.ILog;
import raven.abstractions.logging.LogManager;

public class ReplicationInformerLocalCache {

  private static ILog log = LogManager.getCurrentClassLogger();

  private static String tempDir = System.getProperty("java.io.tmpdir");

  public static JsonDocument tryLoadReplicationInformationFromLocalCache(String serverHash) {
    JsonDocument result = null;
    try {
      String path = "RavenDB Replication Information For - " + serverHash;
      File file = new File(tempDir, path);
      String fileContent = FileUtils.readFileToString(file);
      if (StringUtils.isBlank(fileContent)) {
        return null;
      }
      result = SerializationHelper.toJsonDocument(RavenJObject.parse(fileContent));
    } catch (Exception e) {
      log.error("Could not understand the persisted replication information", e);
      return null;
    }
    return result;
  }

  public static void trySavingReplicationInformationToLocalCache(String serverHash, JsonDocument document) {
    try {
      String path = "RavenDB Replication Information For - " + serverHash;
      File file = new File(tempDir, path);
      FileUtils.writeStringToFile(file, document.toJson().toString());
    } catch (Exception e) {
      log.error("Could not persist the replication information", e);
    }
  }

}
