package net.ravendb.client.document;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.closure.Function1;
import net.ravendb.abstractions.data.BulkInsertOptions;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.changes.IDatabaseChanges;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.extensions.MultiDatabase;



public class BulkInsertOperation implements AutoCloseable {

  public static interface BeforeEntityInsert {
    public void apply(String id, RavenJObject data, RavenJObject metadata);
  }

  private final IDocumentStore documentStore;
  private final GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
  private final ILowLevelBulkInsertOperation operation;
  private IDatabaseCommands databaseCommands;

  private final EntityToJson entityToJson;

  private Set<BeforeEntityInsert> onBeforeEntityInsert = new LinkedHashSet<>();

  public Action1<String> getReport() {
    return operation.getReport();
  }


  public void setReport(Action1<String> report) {
    operation.setReport(report);
  }

  public void addOnBeforeEntityInsert(BeforeEntityInsert action) {
    onBeforeEntityInsert.add(action);
  }

  public void removeOnBeforeEntityInsert(BeforeEntityInsert action) {
    onBeforeEntityInsert.remove(action);
  }

  public IDatabaseCommands getDatabaseCommands() {
    return databaseCommands;
  }

  public UUID getOperationId() {
    return operation.getOperationId();
  }

  public BulkInsertOperation(final String database, final IDocumentStore documentStore, DocumentSessionListeners listeners, BulkInsertOptions options, IDatabaseChanges changes) {
    this.documentStore = documentStore;
    final String finalDatabase = (database != null) ? database : MultiDatabase.getDatabaseName(documentStore.getUrl());
    // Fitzchak: Should not be ever null because of the above code, please refactor this.
    databaseCommands = finalDatabase == null ? documentStore.getDatabaseCommands().forSystemDatabase()
      :documentStore.getDatabaseCommands().forDatabase(finalDatabase);
    generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(documentStore, new Function1<Object, String>() {
      @Override
      public String apply(Object entity) {
        return documentStore.getConventions().generateDocumentKey(finalDatabase, getDatabaseCommands(), entity);
      }
    });
    operation = databaseCommands.getBulkInsertOperation(options, changes);
    entityToJson = new EntityToJson(documentStore, listeners);
  }

  @Override
  public void close() throws Exception {
    operation.close();
  }

  public String store(Object entity) {
    String id = getId(entity);
    store(entity, id);
    return id;
  }

  public void store(Object entity, String id) {
    RavenJObject metadata = new RavenJObject();

    String tag = documentStore.getConventions().getTypeTagName(entity.getClass());
    if (tag != null) {
      metadata.add(Constants.RAVEN_ENTITY_NAME, tag);
    }
    RavenJObject data = entityToJson.convertEntityToJson(id, entity, metadata);

    onBeforeEntityInsert(id, data, metadata);

    operation.write(id, metadata, data);
  }

  private void onBeforeEntityInsert(String id, RavenJObject data, RavenJObject metadata) {
    for (BeforeEntityInsert event: onBeforeEntityInsert) {
      event.apply(id, data, metadata);
    }
  }

  public void store(RavenJObject document, RavenJObject metadata, String id) {
    onBeforeEntityInsert(id, document, metadata);

    operation.write(id, metadata, document);
  }

  public String getId(Object entity) {
    Reference<String> idRef = new Reference<>();

    if (generateEntityIdOnTheClient.tryGetIdFromInstance(entity, idRef)) {
      idRef.value = generateEntityIdOnTheClient.generateDocumentKeyForStorage(entity);
    } else {
      idRef.value = generateEntityIdOnTheClient.generateDocumentKeyForStorage(entity);
      generateEntityIdOnTheClient.trySetIdentity(entity, idRef.value);
    }
    return idRef.value;
  }
}
