package net.ravendb.client.connection;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Function2;
import net.ravendb.abstractions.closure.Function3;
import net.ravendb.abstractions.data.DatabaseDocument;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.connection.implementation.HttpJsonRequest;
import net.ravendb.client.extensions.MultiDatabase;
import net.ravendb.client.utils.UrlUtils;


public class AdminRequestCreator {
  //url, method
  private final Function2<String, HttpMethods, HttpJsonRequest> createRequestForSystemDatabase;
  private final Function2<String, HttpMethods, HttpJsonRequest> createRequest;

  //currentServerUrl, operationUrl, method
  private final Function3<String, String, HttpMethods, HttpJsonRequest> createReplicationAwareRequest;

  public AdminRequestCreator(Function2<String, HttpMethods, HttpJsonRequest> createRequestForSystemDatabase,
    Function2<String, HttpMethods, HttpJsonRequest> createRequest,
    Function3<String, String, HttpMethods, HttpJsonRequest> createReplicationAwareRequest) {
    super();
    this.createRequestForSystemDatabase = createRequestForSystemDatabase;
    this.createRequest = createRequest;
    this.createReplicationAwareRequest = createReplicationAwareRequest;
  }

  public HttpJsonRequest createDatabase(DatabaseDocument databaseDocument, Reference<RavenJObject> docRef) {
    if (!databaseDocument.getSettings().containsKey("Raven/DataDir")) {
      throw new IllegalStateException("The Raven/DataDir setting is mandatory");
    }
    String dbname = databaseDocument.getId().replace("Raven/Databases/", "");
    MultiDatabase.assertValidDatabaseName(dbname);
    RavenJObject doc = RavenJObject.fromObject(databaseDocument);
    doc.remove("id");
    docRef.value = doc;

    return createRequestForSystemDatabase.apply("/admin/databases/" + UrlUtils.escapeDataString(dbname), HttpMethods.PUT);
  }

  public HttpJsonRequest deleteDatabase(String databaseName, boolean hardDelete) {
    String deleteUrl = "/admin/databases/" + UrlUtils.escapeDataString(databaseName);

    if (hardDelete) {
      deleteUrl += "?hard-delete=true";
    }

    return createRequestForSystemDatabase.apply(deleteUrl, HttpMethods.DELETE);
  }

  public HttpJsonRequest stopIndexing(String serverUrl) {
    return createReplicationAwareRequest.apply(serverUrl, "/admin/StopIndexing", HttpMethods.POST);
  }

  public HttpJsonRequest startIndexing(String serverUrl) {
    return createReplicationAwareRequest.apply(serverUrl, "/admin/StartIndexing", HttpMethods.POST);
  }

  public HttpJsonRequest adminStats() {
    return createRequestForSystemDatabase.apply("/admin/stats", HttpMethods.GET);
  }

  public HttpJsonRequest startBackup(String backupLocation, DatabaseDocument databaseDocument, Reference<RavenJObject> backupSettingsRef) {
    RavenJObject backupSettings = new RavenJObject();
    backupSettingsRef.value = backupSettings;

    backupSettings.add("BackupLocation", backupLocation);
    backupSettings.add("DatabaseDocument", RavenJObject.fromObject(databaseDocument));

    return createRequest.apply("/admin/backup", HttpMethods.POST);
  }

  public HttpJsonRequest startRestore(String restoreLocation, String databaseLocation, String databaseName, boolean defrag, Reference<RavenJObject> restoreSettingsRef) {
    RavenJObject restoreSettings = new RavenJObject();
    restoreSettingsRef.value = restoreSettings;
    restoreSettings.add("RestoreLocation", restoreLocation);
    restoreSettings.add("DatabaseLocation", databaseLocation);
    restoreSettings.add("DatabaseName", databaseName);

    return createRequest.apply("/admin/restore?defrag=" + defrag, HttpMethods.POST);

  }

  public HttpJsonRequest indexingStatus(String serverUrl) {
    return createReplicationAwareRequest.apply(serverUrl, "/admin/IndexingStatus", HttpMethods.GET);
  }

  public HttpJsonRequest compactDatabase(String databaseName) {
    return createRequestForSystemDatabase.apply("/admin/compact?database=" + databaseName, HttpMethods.POST);
  }
}
