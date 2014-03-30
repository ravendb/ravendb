package net.ravendb.client.indexes;

import java.io.IOException;

import net.ravendb.abstractions.closure.Action2;
import net.ravendb.abstractions.connection.OperationCredentials;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.replication.ReplicationDestination;
import net.ravendb.abstractions.replication.ReplicationDocument;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.connection.OperationMetadata;
import net.ravendb.client.connection.ServerClient;
import net.ravendb.client.document.DocumentConvention;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class AbstractCommonApiForIndexesAndTransformers {

  private Log logger = LogFactory.getLog(getClass());

  protected OperationMetadata getReplicationOperation(ReplicationDestination replicationDestination) {
    String replicationUrl = replicationDestination.getUrl();
    if (replicationDestination.getClientVisibleUrl() != null) {
      replicationUrl = replicationDestination.getClientVisibleUrl();
    }
    String url = StringUtils.isBlank(replicationDestination.getDatabase()) ? replicationUrl : replicationUrl + "/databases/" + replicationDestination.getDatabase();
    return new OperationMetadata(url, new OperationCredentials(replicationDestination.getApiKey()));
  }

  protected void updateIndexInReplication(IDatabaseCommands databaseCommands, DocumentConvention documentConvention, Action2<ServerClient, OperationMetadata> action) {
    ServerClient serverClient = (ServerClient) databaseCommands;
    if (serverClient == null) {
      return ;
    }

    JsonDocument doc = serverClient.get("Raven/Replication/Destinations");
    if (doc == null) {
      return ;
    }
    ReplicationDocument replicationDocument = null;
    try {
      replicationDocument = JsonExtensions.createDefaultJsonSerializer().readValue(doc.getDataAsJson().toString(), ReplicationDocument.class);
    } catch(IOException e) {
      throw new RuntimeException("Unable to read replicationDocument", e);
    }

    if (replicationDocument == null) {
      return ;
    }

    for (ReplicationDestination replicationDestination: replicationDocument.getDestinations()) {
      try {
        if (replicationDestination.getDisabled() || replicationDestination.getIgnoredClient()) {
          continue;
        }
        action.apply(serverClient, getReplicationOperation(replicationDestination));
      } catch (Exception e) {
        logger.warn("Could not put index in replication server", e);
      }
    }
  }
}
