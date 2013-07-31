package raven.client.document;

import java.util.UUID;

import com.mysema.commons.lang.Pair;
import com.mysema.query.types.Path;

import raven.abstractions.data.Etag;
import raven.client.IDocumentSessionImpl;
import raven.client.ISyncAdvancedSessionOperation;
import raven.client.ITransactionalDocumentSession;
import raven.client.connection.IDatabaseCommands;
import raven.client.indexes.AbstractIndexCreationTask;
import raven.client.linq.IDocumentQueryGenerator;
import raven.client.linq.IRavenQueryable;

/**
 * Implements Unit of Work for accessing the RavenDB server
 *
 */
public class DocumentSession extends InMemoryDocumentSessionOperations implements IDocumentSessionImpl, ITransactionalDocumentSession, ISyncAdvancedSessionOperation, IDocumentQueryGenerator {

  public DocumentSession(String database, DocumentStore documentStore, DocumentSessionListeners listeners, UUID sessionId, IDatabaseCommands setupCommands) {
    // TODO Auto-generated constructor stub
  }

  public void setDatabaseName(String dbName) {
    //TODO:
  }

  @Override
  public ISyncAdvancedSessionOperation getAdvanced() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> void delete(T entity) {
    // TODO Auto-generated method stub

  }

  @Override
  public <T> T load(Class<T> clazz, String id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T[] load(Class<T> clazz, String... ids) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T[] load(Class<T> clazz, Iterable<String> ids) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T load(Class<T> clazz, Number id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T load(Class<T> clazz, UUID id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T[] load(Class<T> clazz, Number... ids) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T[] load(Class<T> clazz, UUID... ids) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> IRavenQueryable<T> query(Class<T> clazz, String indexName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> IRavenQueryable<T> query(Class<T> clazz, String indexName, boolean isMapReduce) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> IRavenQueryable<T> query(Class<T> clazz) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> IRavenQueryable<T> query(Class<T> clazz, Class<AbstractIndexCreationTask> indexCreator) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ILoaderWithInclude<Object> include(String path) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> ILoaderWithInclude<T> include(Path< ? > path) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void saveChanges() {
    // TODO Auto-generated method stub

  }

  @Override
  public void store(Object entity, Etag etag) {
    // TODO Auto-generated method stub

  }

  @Override
  public void store(Object entity, Etag etag, String id) {
    // TODO Auto-generated method stub

  }

  @Override
  public void executeAllPendingLazyOperations() {
    // TODO Auto-generated method stub

  }

  @Override
  public DocumentConvention getConventions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T[] loadInternal(Class<T> clazz, String[] ids) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T[] loadInternal(Class<T> clazz, String[] ids, Pair<String, Class< ? >>[] includes) {
    // TODO Auto-generated method stub
    return null;
  }

}
