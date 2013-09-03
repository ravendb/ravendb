package raven.client.indexes;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import raven.abstractions.closure.Action2;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.extensions.JsonExtensions;
import raven.abstractions.replication.ReplicationDestination;
import raven.abstractions.replication.ReplicationDocument;
import raven.client.connection.IDatabaseCommands;
import raven.client.connection.ServerClient;
import raven.client.document.DocumentConvention;

public class AbstractCommonApiForIndexesAndTransformers {

  private Log logger = LogFactory.getLog(getClass());

  protected String getReplicationUrl(ReplicationDestination replicationDestination) {
    String replicationUrl = replicationDestination.getUrl();
    if (replicationDestination.getClientVisibleUrl() != null) {
      replicationUrl = replicationDestination.getClientVisibleUrl();
    }
    return StringUtils.isBlank(replicationDestination.getDatabase()) ? replicationUrl : replicationUrl + "/databases/" + replicationDestination.getDatabase();
  }

  protected void updateIndexInReplication(IDatabaseCommands databaseCommands, DocumentConvention documentConvention, Action2<ServerClient, String> action) {
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
      replicationDocument = JsonExtensions.getDefaultObjectMapper().readValue(doc.getDataAsJson().toString(), ReplicationDocument.class);
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
        action.apply(serverClient, getReplicationUrl(replicationDestination));
      } catch (Exception e) {
        logger.warn("Could not put index in replication server", e);
      }
    }
  }
}
