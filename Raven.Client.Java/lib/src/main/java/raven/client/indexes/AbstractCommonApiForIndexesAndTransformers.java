package raven.client.indexes;

import java.io.IOException;
import java.util.Stack;

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
import raven.linq.dsl.LinqExpressionMixin;
import raven.linq.dsl.LinqOps;

import com.mysema.query.support.Expressions;
import com.mysema.query.types.Expression;
import com.mysema.query.types.Path;
import com.mysema.query.types.path.EntityPathBase;
import com.mysema.query.types.path.ListPath;

public class AbstractCommonApiForIndexesAndTransformers {

  private Log logger = LogFactory.getLog(getClass());

  protected <T> Expression<?> recurse(ListPath<T, ? extends EntityPathBase<T>> path) {
    Path<?> originalRoot = path.getRoot();

    // 1. replace lambda in path to some custom value
    Stack<String> pathStack = new Stack<>();
    Stack<Class<?>> classStack = new Stack<>();
    Path<?> currentPath = path;
    while (currentPath.getMetadata().getParent() != null) {
      pathStack.add(currentPath.getMetadata().getName());
      classStack.add(currentPath.getType());
      currentPath = (Path< ? >) currentPath.getMetadata().getParent();
    }
    Path<?> innerRoot = Expressions.path(currentPath.getType(), "x");
    Path< ? > newPath = innerRoot;
    while (!pathStack.isEmpty()) {
      String prop = pathStack.pop();
      Class<?> elementClass = classStack.pop();
      newPath = Expressions.path(elementClass, newPath, prop);
    }

    Expression<?> innerLambda = Expressions.operation(LinqExpressionMixin.class, LinqOps.LAMBDA, innerRoot, newPath);

    Expression<?> recurseOp = Expressions.operation(LinqExpressionMixin.class, LinqOps.Markers.RECURSE, originalRoot, innerLambda);

    return recurseOp;
  }

  public <T extends EntityPathBase<?>> T loadDocument(Class<T> dslClass, String key) {
    return null; //TODO finish me
  }

  //TODO: public T[] LoadDocument<T>(IEnumerable<string> keys)


  //TODO: protected RavenJObject MetadataFor(object doc)
  //TODO: protected RavenJObject AsDocument(object doc)

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
