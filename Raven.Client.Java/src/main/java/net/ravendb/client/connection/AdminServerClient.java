package net.ravendb.client.connection;

import java.io.IOException;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Function1;
import net.ravendb.abstractions.closure.Function2;
import net.ravendb.abstractions.closure.Function3;
import net.ravendb.abstractions.data.AdminStatistics;
import net.ravendb.abstractions.data.DatabaseDocument;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.connection.implementation.HttpJsonRequest;
import net.ravendb.client.document.DocumentConvention;
import net.ravendb.client.extensions.MultiDatabase;
import net.ravendb.client.indexes.RavenDocumentsByEntityName;



public class AdminServerClient implements IAdminDatabaseCommands, IGlobalAdminDatabaseCommands {

  private final ServerClient innerServerClient;
  private final AdminRequestCreator adminRequest;

  public AdminServerClient(ServerClient serverClient) {
    innerServerClient = serverClient;
    adminRequest = new AdminRequestCreator(new Function2<String, HttpMethods, HttpJsonRequest>() {

      @Override
      public HttpJsonRequest apply(String url, HttpMethods method) {
        return ((ServerClient)innerServerClient.forSystemDatabase()).createRequest(method, url);
      }
    }, new Function2<String, HttpMethods, HttpJsonRequest>() {

      @Override
      public HttpJsonRequest apply(String url, HttpMethods method) {
        return innerServerClient.createRequest(method, url);
      }
    }, new Function3<String, String, HttpMethods, HttpJsonRequest>() {

      @Override
      public HttpJsonRequest apply(String currentServerUrl, String requestUrl, HttpMethods method) {
        return innerServerClient.createReplicationAwareRequest(currentServerUrl, requestUrl, method);
      }
    });
  }

  @Override
  public void createDatabase(DatabaseDocument databaseDocument) {
    Reference<RavenJObject> docRef = new Reference<>();
    HttpJsonRequest req = adminRequest.createDatabase(databaseDocument, docRef);

    req.write(docRef.value.toString());
    req.executeRequest();
  }

  @Override
  public void deleteDatabase(String databaseName)  {
    deleteDatabase(databaseName, false);

  }
  @Override
  public void deleteDatabase(String databaseName, boolean hardDelete) {
    adminRequest.deleteDatabase(databaseName, hardDelete).executeRequest();
  }

  @Override
  public IDatabaseCommands getCommands() {
    return innerServerClient;
  }

  @Override
  public void compactDatabase(String databaseName) {
    adminRequest.compactDatabase(databaseName).executeRequest();
  }

  @Override
  public void stopIndexing() {
    innerServerClient.executeWithReplication(HttpMethods.POST, new Function1<OperationMetadata, Void>() {

      @Override
      public Void apply(OperationMetadata operationMetadata) {
        adminRequest.stopIndexing(operationMetadata.getUrl()).executeRequest();
        return null;
      }
    });
  }

  @Override
  public void startIndexing() {
    innerServerClient.executeWithReplication(HttpMethods.POST, new Function1<OperationMetadata, Void>() {

      @Override
      public Void apply(OperationMetadata operationMetadata) {
        adminRequest.startIndexing(operationMetadata.getUrl()).executeRequest();
        return null;
      }
    });
  }


  @Override
  public void startBackup(String backupLocation, DatabaseDocument databaseDocument, String databaseName) {
    Reference<RavenJObject> backupSettingsRef = new Reference<>();
    HttpJsonRequest request = adminRequest.startBackup(backupLocation, databaseDocument, databaseName, backupSettingsRef);

    request.write(backupSettingsRef.value.toString());
    request.executeRequest();
  }

  @Override
  public void startRestore(String restoreLocation, String databaseLocation) {
    startRestore(restoreLocation, databaseLocation, null, false);
  }

  @Override
  public void startRestore(String restoreLocation, String databaseLocation, String databaseName) {
    startRestore(restoreLocation, databaseLocation, databaseName, false);
  }

  @Override
  public void startRestore(String restoreLocation, String databaseLocation, String databaseName, boolean defrag) {
    Reference<RavenJObject> restoreSettingsRef = new Reference<>();

    HttpJsonRequest request = adminRequest.startRestore(restoreLocation, databaseLocation, databaseName, defrag, restoreSettingsRef);

    request.write(restoreSettingsRef.value.toString());
    request.executeRequest();
  }


  @Override
  public String getIndexingStatus() {
    return innerServerClient.executeWithReplication(HttpMethods.GET, new Function1<OperationMetadata, String>() {

      @Override
      public String apply(OperationMetadata operationMetadata) {
        RavenJToken result = adminRequest.indexingStatus(operationMetadata.getUrl()).readResponseJson();
        return result.value(String.class, "IndexingStatus");
      }
    });
  }

  @Override
  public AdminStatistics getStatistics() {
    try {
      RavenJObject json = (RavenJObject)adminRequest.adminStats().readResponseJson();
      return innerServerClient.convention.createSerializer().readValue(json.toString(), AdminStatistics.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void ensureDatabaseExists(String name, boolean ignoreFailures) {

    ServerClient serverClient = (ServerClient) innerServerClient.forSystemDatabase();

    try (AutoCloseable readFromMaster = serverClient.forceReadFromMaster()) {
      DatabaseDocument doc = MultiDatabase.createDatabaseDocument(name);

      try {
        if (serverClient.get(doc.getId()) != null) {
          return;
        }
        serverClient.getGlobalAdmin().createDatabase(doc);
      } catch (Exception e) {
        if (!ignoreFailures) {
          throw new RuntimeException(e);
        }
      }

      try {
        new RavenDocumentsByEntityName().execute(serverClient.forDatabase(name), new DocumentConvention());
      } catch (Exception e) {
        // we really don't care if this fails, and it might, if the user doesn't have permissions on the new db
      }

    } catch (Exception e) {
        // we really don't care if this fails, and it might, if the user doesn't have permissions on the new db
    }
  }

  @Override
  public void ensureDatabaseExists(String name) {
    ensureDatabaseExists(name, false);
  }


}
